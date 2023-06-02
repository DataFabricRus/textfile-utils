package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.deleteExisting
import kotlin.io.path.fileSize
import kotlin.math.max
import kotlin.math.min


/**
 * Merges files into single one with fixed allocation.
 * Source files must be sorted.
 * Files are read from end to beginning, so [comparator] should have reverse order: `(a, b) -> b.compareTo(a)`.
 * The target file will be inverse: e.g. if `a < d < b < e < c < f` then `a, b, c` + `d, e, f` = `f, c, e, b, d, a`
 * The [invert] method can be used to rewrite content in direct order.
 * Since this is IO operation, the [DirectByteBuffer][ByteBuffer.allocateDirect] is preferred.
 *
 * The chosen ratio is [allocatedMemorySizeInBytes] = `2 * x * (sourceFileSize{1} + ... sourceFileSize{N}) = 2 * x * targetFileSize`
 * and has no any mathematical or experimental basis and is most likely not optimal, but seems to be reasonable.
 * Note that total memory consumption is greater than [allocatedMemorySizeInBytes]
 * sine both reader and writer use additional arrays to store results.
 * Source files will be deleted if [deleteSourceFiles] = `true`.
 *
 * @param [sources][Set]<[Path]>
 * @param [target][Path]
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter]
 * @param [allocatedMemorySizeInBytes] = `chunkSizeSize + writeBufferSize + 2 * readBufferSize`, approximate memory consumption; number of bytes
 * @param [deleteSourceFiles] if `true` source files will be truncated while process and completely deleted at the end of it;
 * this allows to save diskspace
 * @param [charset][Charset]
 */
fun mergeFilesInverse(
    sources: Set<Path>,
    target: Path,
    comparator: Comparator<String> = defaultComparator<String>().reversed(),
    delimiter: String = "\n",
    allocatedMemorySizeInBytes: Int = 2 * MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES,
    deleteSourceFiles: Boolean = false,
    charset: Charset = Charsets.UTF_8,
) {
    require(sources.size > 1) { "Number of given sources (${sources.size}) must greater than 1" }
    val writeBufferSize = max((allocatedMemorySizeInBytes / 2.0).toInt(), MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES)
    val writeBuffer = ByteBuffer.allocateDirect(writeBufferSize)
    val readFilesSizeRatio = allocatedMemorySizeInBytes / (sources.sumOf { it.fileSize() } * 2.0)
    val sourceBuffers = sources.associateWith { file ->
        val size = max((readFilesSizeRatio * file.fileSize()).toInt(), MERGE_FILES_MIN_READ_BUFFER_SIZE_IN_BYTES)
        ByteBuffer.allocateDirect(size)
    }

    mergeFilesInverse(
        sources = sources,
        target = target,
        comparator = comparator,
        delimiter = delimiter,
        sourceBuffer = { checkNotNull(sourceBuffers[it]) },
        targetBuffer = { writeBuffer },
        deleteSourceFiles = deleteSourceFiles,
        charset = charset,
    )
}

/**
 * Merges two file into single one with fixed allocation.
 * Source files must be sorted.
 * Files are read from end to beginning, so [comparator] should have reverse order: `(a, b) -> b.compareTo(a)`.
 * The target file content will be in inverse order:
 * e.g. if `a < d < b < e < c < f ` then `a, b, c` + `d, e, f` = `f, c, e, b, d, a`.
 * The [invert] method can be used to rewrite content in direct order.
 * Since this is IO operation, the [DirectByteBuffer][ByteBuffer.allocateDirect] is preferred.
 *
 * @param [sources][Set]<[Path]>
 * @param [target][Path]
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter]
 * @param [sourceBuffer] get [ByteBuffer] for read operations, the number of bytes must be greater than 2
 * @param [targetBuffer] get [ByteBuffer] for writ operations, the number of bytes must be greater than 2
 * @param [deleteSourceFiles] if `true` source files will be truncated while process and completely deleted at the end of it;
 * this allows to save diskspace
 * @param [charset][Charset]
 */
fun mergeFilesInverse(
    sources: Set<Path>,
    target: Path,
    comparator: Comparator<String> = defaultComparator<String>().reversed(),
    delimiter: String = "\n",
    sourceBuffer: (Path) -> ByteBuffer = { ByteBuffer.allocateDirect(MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES) },
    targetBuffer: (Path) -> ByteBuffer = { ByteBuffer.allocateDirect(MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES) },
    deleteSourceFiles: Boolean = false,
    charset: Charset = Charsets.UTF_8,
) {
    require(sources.size > 1) { "Number of given sources (${sources.size}) must be greater than 1" }
    val readBuffers = sources.associateWith { file ->
        val buffer = sourceBuffer(file)
        require(buffer.capacity() >= MERGE_FILES_MIN_READ_BUFFER_SIZE_IN_BYTES) {
            "Specified read buffer size is too small: ${buffer.capacity()}"
        }
        buffer
    }
    val writeBuffer = targetBuffer(target)
    require(writeBuffer.capacity() >= MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES) {
        "Specified write buffer size is too small: ${writeBuffer.capacity()}"
    }

    val bomSymbols = charset.bomSymbols()
    val delimiterBytes = delimiter.bytes(charset)
    val segmentSizes = sources.associateWith { file -> AtomicLong(file.fileSize()) }

    target.use { res ->
        sources.use { source ->
            var firstLine = true
            val inputs = source.map { (file, channel) ->
                val segmentSize = checkNotNull(segmentSizes[file])
                val readBuffer = checkNotNull(readBuffers[file])

                file to channel.readLines(
                    direct = false,
                    startAreaPositionInclusive = 0,
                    endAreaPositionExclusive = segmentSize.get(),
                    listener = { size -> segmentSize.set(size) },
                    buffer = readBuffer,
                    delimiter = delimiter,
                    charset = charset,
                    coroutineName = "LeftLinesReader[$file]"
                )
            }
            if (bomSymbols.isNotEmpty()) {
                res.write(ByteBuffer.wrap(bomSymbols))
            }
            mergeSequences(inputs.map { it.second }, comparator).forEach { line ->
                if (!firstLine) {
                    res.writeData(delimiterBytes, writeBuffer)
                }
                firstLine = false
                res.writeData(line.bytes(charset), writeBuffer)
                if (deleteSourceFiles) {
                    source.forEach { (file, channel) -> channel.truncate(checkNotNull(segmentSizes[file]).get()) }
                }
            }
            if (writeBuffer.position() > 0) {
                writeBuffer.limit(writeBuffer.position())
                writeBuffer.position(0)
                res.write(writeBuffer)
                if (deleteSourceFiles) {
                    source.forEach { (file, channel) -> channel.truncate(checkNotNull(segmentSizes[file]).get()) }
                }
            }
        }
    }
    if (deleteSourceFiles) {
        segmentSizes.forEach { (file, segment) ->
            check(file.fileSize() == bomSymbols.size.toLong()) {
                "Source file <${file.fileName}> must be empty; real-size = ${file.fileSize()}, segment-size = $segment"
            }
            file.deleteExisting()
        }
    }
}

private fun SeekableByteChannel.writeData(data: ByteArray, buffer: ByteBuffer) {
    var index = 0
    while (index != -1) {
        index = writeData(data, index, buffer)
    }
}

private fun SeekableByteChannel.writeData(
    data: ByteArray,
    fromIndex: Int,
    buffer: ByteBuffer,
): Int {
    require(data.isNotEmpty())
    val length = min(data.size - fromIndex, buffer.capacity() - buffer.position())
    buffer.put(data, fromIndex, length)
    val nextPosition = buffer.position()
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