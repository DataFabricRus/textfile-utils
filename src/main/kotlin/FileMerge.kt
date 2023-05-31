package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.deleteExisting
import kotlin.io.path.fileSize
import kotlin.math.min

/**
 * Merges two file into single one with fixed allocation.
 * Source files must be sorted.
 * Files are read from end to beginning, so [comparator] should have reverse order: `(a, b) -> b.compareTo(a)`.
 * The target file will be inverse: e.g. if `a < d < b < e < c < f` then `a, b, c` + `d, e, f` = `f, c, e, b, d, a`
 * The [invert] method can be used to rewrite content in direct order.
 *
 * The chosen ratio is [allocatedMemorySize] = `chunkSizeSize + writeBufferSize + 2 * readBufferSize` and
 * has no any mathematical or experimental basis and is most likely not optimal.
 * Note that total memory consumption is greater than [allocatedMemorySize]
 * sine both reader and writer use additional arrays to store results.
 * Source files will be deleted if.
 *
 * @param [leftSource][Path]
 * @param [rightSource][Path]
 * @param [target][Path]
 * @param [allocatedMemorySize] = `chunkSizeSize + writeBufferSize + 2 * readBufferSize`, approximate memory consumption; number of bytes
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter]
 * @param [charset][Charset]
 */
fun mergeFilesInverse(
    leftSource: Path,
    rightSource: Path,
    target: Path,
    allocatedMemorySize: Int,
    deleteSourceFiles: Boolean = false,
    comparator: Comparator<String> = defaultComparator<String>().reversed(),
    delimiter: String = "\n",
    charset: Charset = Charsets.UTF_8,
) = mergeFilesInverse(
    leftSource = leftSource,
    rightSource = rightSource,
    target = target,
    chunkSize = allocatedMemorySize / 3,
    readBufferSize = allocatedMemorySize / 6,
    writeBufferSize = allocatedMemorySize / 3,
    deleteSourceFiles = deleteSourceFiles,
    comparator = comparator,
    delimiter = delimiter,
    charset = charset,
)

/**
 * Merges two file into single one with fixed allocation.
 * Source files must be sorted.
 * Files are read from end to beginning, so [comparator] should have reverse order: `(a, b) -> b.compareTo(a)`.
 * The target file content will be in inverse order:
 * e.g. if `a < d < b < e < c < f ` then `a, b, c` + `d, e, f` = `f, c, e, b, d, a`.
 * The [invert] method can be used to rewrite content in direct order.
 *
 * @param [leftSource][Path]
 * @param [rightSource][Path]
 * @param [target][Path]
 * @param [chunkSize] the number of bytes to be written to the [target] per one [insert]; must be greater than 2
 * @param [readBufferSize] the number of bytes for reading, must be greater than 2
 * @param [writeBufferSize] the number of bytes for writing; must be greater than 2
 * @param [deleteSourceFiles] if `true` source files will be truncated while process and completely deleted at the end of it;
 * this allows to save diskspace
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter]
 * @param [charset][Charset]
 */
fun mergeFilesInverse(
    leftSource: Path,
    rightSource: Path,
    target: Path,
    chunkSize: Int,
    readBufferSize: Int,
    writeBufferSize: Int,
    deleteSourceFiles: Boolean = false,
    comparator: Comparator<String> = defaultComparator<String>().reversed(),
    delimiter: String = "\n",
    charset: Charset = Charsets.UTF_8,
) {
    require(chunkSize > 2) { "specified chunk size is too small: $chunkSize" }
    require(readBufferSize > 2) { "specified read buffer size is too small: $readBufferSize" }
    require(writeBufferSize > 2) { "specified write buffer size is too small: $writeBufferSize" }

    val delimiterBytes = delimiter.toByteArray(charset)

    val leftBuffer = ByteBuffer.allocate(readBufferSize)
    val rightBuffer = ByteBuffer.allocate(readBufferSize)
    val targetBuffer = ByteBuffer.allocate(writeBufferSize)

    val leftSegmentSize = AtomicLong(leftSource.fileSize())
    val rightSegmentSize = AtomicLong(rightSource.fileSize())

    leftSource.use { left ->
        val leftSequence = left.readLines(
            direct = false,
            startPositionInclusive = 0,
            endPositionExclusive = leftSegmentSize.get(),
            listener = { leftSegmentSize.set(it) },
            buffer = leftBuffer,
            delimiter = delimiter,
            charset = charset,
            coroutineName = "LeftLinesReader"
        )
        rightSource.use { right ->
            val rightSequence = right.readLines(
                direct = false,
                startPositionInclusive = 0,
                endPositionExclusive = rightSegmentSize.get(),
                listener = { rightSegmentSize.set(it) },
                buffer = rightBuffer,
                delimiter = delimiter,
                charset = charset,
                coroutineName = "RightLinesReader"
            )
            target.use { res ->
                var firstLine = true
                leftSequence.mergeWith(rightSequence, comparator).forEach { line ->
                    if (!firstLine) {
                        res.writeData(delimiterBytes, targetBuffer)
                    }
                    firstLine = false
                    res.writeData(line.toByteArray(charset), targetBuffer)
                    if (deleteSourceFiles) {
                        left.truncate(leftSegmentSize.get())
                        right.truncate(rightSegmentSize.get())
                    }
                }
                if (targetBuffer.position() > 0) {
                    targetBuffer.limit(targetBuffer.position())
                    targetBuffer.position(0)
                    res.write(targetBuffer)
                    if (deleteSourceFiles) {
                        left.truncate(leftSegmentSize.get())
                        right.truncate(rightSegmentSize.get())
                    }
                }
            }
        }
    }
    if (deleteSourceFiles) {
        check(leftSource.fileSize() == 0L) {
            "Left source file $leftSource must be empty, real-size = ${leftSource.fileSize()}, segment-size = $leftSegmentSize"
        }
        check(rightSource.fileSize() == 0L) {
            "Right source file $rightSource must be empty, real-size = ${rightSource.fileSize()}, segment-size = $rightSegmentSize"
        }
        leftSource.deleteExisting()
        rightSource.deleteExisting()
    }
}

private fun SeekableByteChannel.writeData(data: ByteArray, buffer: ByteBuffer) {
    var index = 0
    while (index != -1) {
        index = put(data, index, buffer)
    }
}

private fun SeekableByteChannel.put(
    data: ByteArray,
    fromIndex: Int,
    buffer: ByteBuffer,
): Int {
    require(data.isNotEmpty())
    val length = min(data.size - fromIndex, buffer.capacity() - buffer.position())
    System.arraycopy(data, fromIndex, buffer.array(), buffer.position(), length)
    val nextPosition = buffer.position() + length
    if (nextPosition == buffer.capacity()) {
        buffer.rewind()
        write(buffer)
        buffer.rewind()
    } else {
        buffer.position(nextPosition)
    }
    val nextIndex = fromIndex + length
    return if (nextIndex == data.size) {
        -1 // stop
    } else {
        check(nextIndex < data.size)
        nextIndex
    }
}