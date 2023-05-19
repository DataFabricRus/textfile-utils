package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.deleteExisting
import kotlin.io.path.fileSize

/**
 * Merges two file into single one with fixed allocation.
 * Files must be sorted.
 * Files are read from end to beginning, so [comparator] should have reverse order: `(a, b) -> b.compareTo(a)`.
 * The chosen ratio is [allocatedMemorySize] = `chunkSizeSize + writeBufferSize + 2 * readBufferSize` and does not have
 * any mathematical or experimental basis and is most likely not optimal.
 * Note that total memory consumption is greater than [allocatedMemorySize] sine both reader and writer use [String]s arrays in additional to the read/write buffers.
 * Source files will be deleted.
 *
 * @param [leftSource][Path]
 * @param [rightSource][Path]
 * @param [target][Path]
 * @param [allocatedMemorySize] = `chunkSizeSize + writeBufferSize + 2 * readBufferSize`, approximate memory consumption; number of bytes
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter]
 * @param [charset][Charset]
 */
fun mergeFiles(
    leftSource: Path,
    rightSource: Path,
    target: Path,
    allocatedMemorySize: Int,
    comparator: Comparator<String> = defaultComparator<String>().reversed(),
    delimiter: String = "\n",
    charset: Charset = Charsets.UTF_8,
) = mergeFiles(
    leftSource = leftSource,
    rightSource = rightSource,
    target = target,
    chunkSize = allocatedMemorySize / 3,
    readBufferSize = allocatedMemorySize / 6,
    writeBufferSize = allocatedMemorySize / 3,
    deleteSourceFiles = true,
    comparator = comparator,
    delimiter = delimiter,
    charset = charset,
)

/**
 * Merges two file into single one.
 * Files must be sorted.
 * Files are read from the end to the beginning, so [comparator] should have reverse order: `(a, b) -> b.compareTo(a)`.
 *
 * @param [leftSource][Path]
 * @param [rightSource][Path]
 * @param [target][Path]
 * @param [chunkSize] the number of bytes to be written to the [target] per one [insertBefore]; must be greater than 2
 * @param [readBufferSize] the number of bytes for reading, must be greater than 2
 * @param [writeBufferSize] the number of bytes for writing; must be greater than 2
 * @param [deleteSourceFiles] if `true` source files will be truncated while process and completely deleted at the end of it;
 * this allows to save diskspace
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter]
 * @param [charset][Charset]
 */
fun mergeFiles(
    leftSource: Path,
    rightSource: Path,
    target: Path,
    chunkSize: Int,
    readBufferSize: Int,
    writeBufferSize: Int,
    deleteSourceFiles: Boolean = true,
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
            segmentSize = leftSegmentSize.get(),
            listener = { leftSegmentSize.set(it) },
            buffer = leftBuffer,
            delimiter = delimiter,
            charset = charset,
            coroutineName = "LeftLinesReader"
        )
        rightSource.use { right ->
            val rightSequence = right.readLines(
                segmentSize = rightSegmentSize.get(),
                listener = { rightSegmentSize.set(it) },
                buffer = rightBuffer,
                delimiter = delimiter,
                charset = charset,
                coroutineName = "RightLinesReader"
            )
            target.use { res ->
                var firstLine = true
                val chunk = LinesChunk()
                leftSequence.mergeWith(rightSequence, comparator).forEach {
                    if (!firstLine) {
                        chunk.put(delimiterBytes)
                    }
                    firstLine = false
                    chunk.put(it.toByteArray(charset))
                    if (chunk.size() > chunkSize) {
                        res.insertBefore(data = chunk.toByteArray(), buffer = targetBuffer)
                        chunk.clear()
                        if (deleteSourceFiles) {
                            left.truncate(leftSegmentSize.get())
                            right.truncate(rightSegmentSize.get())
                        }
                    }
                }
                if (chunk.size() > 0) {
                    res.insertBefore(data = chunk.toByteArray(), buffer = targetBuffer)
                    chunk.clear()
                    if (deleteSourceFiles) {
                        right.truncate(rightSegmentSize.get())
                        left.truncate(leftSegmentSize.get())
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

private class LinesChunk {
    private val lines: MutableList<ByteArray> = mutableListOf()
    private var size: Int = 0

    fun size() = size

    fun toByteArray(): ByteArray {
        val res = ByteArray(size)
        var position = 0
        lines.indices.reversed().forEach {
            val bytes = lines[it]
            System.arraycopy(bytes, 0, res, position, bytes.size)
            position += bytes.size
        }
        return res
    }

    fun put(line: ByteArray) {
        lines.add(line)
        size += line.size
    }

    fun clear() {
        lines.clear()
        size = 0
    }
}