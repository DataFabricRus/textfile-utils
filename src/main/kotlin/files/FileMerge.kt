package cc.datafabric.textfileutils.files

import cc.datafabric.iterators.ResourceIterator
import cc.datafabric.iterators.closeAll
import cc.datafabric.textfileutils.iterators.byteArrayStringComparator
import cc.datafabric.textfileutils.iterators.mergeIterators
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.plus
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.deleteExisting
import kotlin.io.path.fileSize
import kotlin.math.max
import kotlin.math.min

/**
 * Merges files into single one with fixed memory allocation.
 * Source files must be sorted.
 * Files are read from the end to the beginning, so [comparator] should have reverse order: `(a, b) -> b.compareTo(a)`.
 * The target file will be inverse: e.g., if `a < d < b < e < c < f` then `a, b, c` + `d, e, f` = `f, c, e, b, d, a`
 * The [invert] method can be used to rewrite content in direct order.
 *
 * The method allocates `[allocatedMemorySizeInBytes] * [writeToTotalMemRatio]` bytes for write operation,
 * and `sourceFileSize{i} * [allocatedMemorySizeInBytes] * (1 - [writeToTotalMemRatio]) / sum (sourceFileSize{1} + ... sourceFileSize{N})`
 * bytes for each read operation.
 *
 * Note that total memory consumption is greater than [allocatedMemorySizeInBytes],
 * since each operation requires some temporal data.
 *
 * If [controlDiskspace] = `true` then source files will be truncated while process and completely deleted at the end of process.
 * When control diskspace is enabled, the method execution can take a long time.
 *
 * @param [sources][Set]<[Path]>
 * @param [target][Path]
 * @param [comparator][Comparator]<[ByteArray]>
 * @param [delimiter][ByteArray] e.g. for UTF-16 `" " = [0, 32]`
 * @param [bomSymbols][ByteArray] e.g. for UTF-16 `[-2, -1]`
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes] = `chunkSizeSize + writeBufferSize + 2 * readBufferSize`, approximate memory consumption; number of bytes
 * @param [controlDiskspace] if `true` source files will be truncated while processing and completely deleted at the end of it;
 * this allows saving diskspace, but it takes more time
 * @param [writeToTotalMemRatio] ratio of memory allocated for write operations to [total allocated memory][allocatedMemorySizeInBytes]
 * @param [coroutineScope][CoroutineScope]
 */
@Suppress("DuplicatedCode")
fun mergeFilesInverse(
    sources: Set<Path>,
    target: Path,
    comparator: Comparator<ByteArray> = byteArrayStringComparator().reversed(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    bomSymbols: ByteArray = byteArrayOf(),
    allocatedMemorySizeInBytes: Int = 2 * MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES,
    writeToTotalMemRatio: Double = MERGE_FILES_WRITE_BUFFER_TO_TOTAL_MEMORY_ALLOCATION_RATIO,
    controlDiskspace: Boolean = false,
    coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.IO) + CoroutineName("mergeFilesInverse")
) {
    require(sources.size > 1) { "Number of given sources (${sources.size}) must greater than 1" }
    require(writeToTotalMemRatio > 0.0 && writeToTotalMemRatio < 1.0) { "writeToTotalMemRatio must fall within the range of 0 to 1" }

    val writeBufferSize =
        max((allocatedMemorySizeInBytes * writeToTotalMemRatio).toInt(), MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES)
    val readBuffersSize = max(allocatedMemorySizeInBytes - writeBufferSize, MERGE_FILES_MIN_READ_BUFFER_SIZE_IN_BYTES)
    val filesSize = sources.sumOf { it.fileSize() }
    val readFilesSizeRatio = readBuffersSize.toDouble() / filesSize

    val writeBuffer = ByteBuffer.allocateDirect(writeBufferSize)
    val sourceBuffers = sources.associateWith { file ->
        val size = max((readFilesSizeRatio * file.fileSize()).toInt(), MERGE_FILES_MIN_READ_BUFFER_SIZE_IN_BYTES)
        ByteBuffer.allocateDirect(size)
    }

    mergeFilesInverse(
        sources = sources,
        target = target,
        comparator = comparator,
        delimiter = delimiter,
        bomSymbols = bomSymbols,
        sourceBuffer = { checkNotNull(sourceBuffers[it]) },
        targetBuffer = { writeBuffer },
        controlDiskspace = controlDiskspace,
        coroutineScope = coroutineScope,
    )
}

/**
 * Merges files into single one with fixed memory allocation.
 * Source files must be sorted.
 * In opposite [mergeFilesInverse], files are read from the beginning to the end.
 *
 * The method allocates `[allocatedMemorySizeInBytes] * [writeToTotalMemRatio]` bytes for write operation,
 * and `sourceFileSize{i} * [allocatedMemorySizeInBytes] * (1 - [writeToTotalMemRatio]) / sum (sourceFileSize{1} + ... sourceFileSize{N})`
 * bytes for each read operation.
 *
 * Note that total memory consumption is greater than [allocatedMemorySizeInBytes],
 * since each operation requires some temporal data.
 *
 * @param [sources][Set]<[Path]>
 * @param [target][Path]
 * @param [comparator][Comparator]<[ByteArray]>
 * @param [delimiter][ByteArray] e.g. for UTF-16 `" " = [0, 32]`
 * @param [bomSymbols][ByteArray] e.g. for UTF-16 `[-2, -1]`
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes] = `chunkSizeSize + writeBufferSize + 2 * readBufferSize`, approximate memory consumption; number of bytes
 * @param [writeToTotalMemRatio] ratio of memory allocated for write operations to [total allocated memory][allocatedMemorySizeInBytes]
 * @param [coroutineScope][CoroutineScope]
 */
@Suppress("DuplicatedCode")
fun mergeFilesDirect(
    sources: Set<Path>,
    target: Path,
    comparator: Comparator<ByteArray> = byteArrayStringComparator().reversed(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    bomSymbols: ByteArray = byteArrayOf(),
    allocatedMemorySizeInBytes: Int = 2 * MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES,
    writeToTotalMemRatio: Double = MERGE_FILES_WRITE_BUFFER_TO_TOTAL_MEMORY_ALLOCATION_RATIO,
    coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.IO) + CoroutineName("mergeFilesInverse")
) {
    require(sources.size > 1) { "Number of given sources (${sources.size}) must greater than 1" }
    require(writeToTotalMemRatio > 0.0 && writeToTotalMemRatio < 1.0) { "writeToTotalMemRatio must fall within the range of 0 to 1" }

    val writeBufferSize =
        max((allocatedMemorySizeInBytes * writeToTotalMemRatio).toInt(), MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES)
    val readBuffersSize = max(allocatedMemorySizeInBytes - writeBufferSize, MERGE_FILES_MIN_READ_BUFFER_SIZE_IN_BYTES)
    val filesSize = sources.sumOf { it.fileSize() }
    val readFilesSizeRatio = readBuffersSize.toDouble() / filesSize

    val writeBuffer = ByteBuffer.allocateDirect(writeBufferSize)
    val sourceBuffers = sources.associateWith { file ->
        val size = max((readFilesSizeRatio * file.fileSize()).toInt(), MERGE_FILES_MIN_READ_BUFFER_SIZE_IN_BYTES)
        ByteBuffer.allocateDirect(size)
    }

    mergeFiles(
        sources = sources,
        target = target,
        comparator = comparator,
        delimiter = delimiter,
        bomSymbols = bomSymbols,
        sourceBuffer = { checkNotNull(sourceBuffers[it]) },
        targetBuffer = { writeBuffer },
        controlDiskspace = false,
        direct = true,
        coroutineScope = coroutineScope
    )
}

/**
 * Merges two files into a single one with fixed memory allocation.
 * Source files must be sorted.
 * Files are read from the end to the beginning, so [comparator] should have reverse order: `(a, b) -> b.compareTo(a)`.
 * The target file content will be in inverse order:
 * e.g., if `a < d < b < e < c < f ` then `a, b, c` + `d, e, f` = `f, c, e, b, d, a`.
 * The [invert] method can be used to rewrite content in direct order.
 * Since this is IO operation, the [DirectByteBuffer][ByteBuffer.allocateDirect] is preferred.
 *
 * @param [sources][Set]<[Path]>
 * @param [target][Path]
 * @param [comparator][Comparator]<[ByteArray]>
 * @param [delimiter][ByteArray] e.g. for UTF-16 `" " = [0, 32]`
 * @param [bomSymbols][ByteArray] e.g. for UTF-16 `[-2, -1]`
 * @param [sourceBuffer] get [ByteBuffer] for read operations, the number of bytes must be greater than 2
 * @param [targetBuffer] get [ByteBuffer] for writ operations, the number of bytes must be greater than 2
 * @param [controlDiskspace] if `true` source files will be truncated while processing and completely deleted at the end of it;
 * this allows saving diskspace
 * @param coroutineScope[CoroutineScope]
 */
fun mergeFilesInverse(
    sources: Set<Path>,
    target: Path,
    comparator: Comparator<ByteArray> = byteArrayStringComparator().reversed(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    bomSymbols: ByteArray = byteArrayOf(),
    sourceBuffer: (Path) -> ByteBuffer = { ByteBuffer.allocateDirect(MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES) },
    targetBuffer: (Path) -> ByteBuffer = { ByteBuffer.allocateDirect(MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES) },
    controlDiskspace: Boolean = false,
    coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.IO) + CoroutineName("mergeFilesInverse"),
) = mergeFiles(
    sources = sources,
    target = target,
    comparator = comparator,
    delimiter = delimiter,
    bomSymbols = bomSymbols,
    sourceBuffer = sourceBuffer,
    targetBuffer = targetBuffer,
    controlDiskspace = controlDiskspace,
    direct = false,
    coroutineScope = coroutineScope
)

/**
 * Merges two files into a single one with fixed memory allocation.
 * Source files must be sorted.
 * If [direct] = `false`, files are read from the end to the beginning;
 * this allows truncating them during operation, to do so set [controlDiskspace] = `true`.
 *
 * @param [sources][Set]<[Path]>
 * @param [target][Path]
 * @param [comparator][Comparator]<[ByteArray]>
 * @param [delimiter][ByteArray] e.g. for UTF-16 `" " = [0, 32]`
 * @param [bomSymbols][ByteArray] e.g. for UTF-16 `[-2, -1]`
 * @param [sourceBuffer] get [ByteBuffer] for read operations, the number of bytes must be greater than 2
 * @param [targetBuffer] get [ByteBuffer] for writ operations, the number of bytes must be greater than 2
 * @param [controlDiskspace] if `true` source files will be truncated while processing and completely deleted at the end of it;
 * this allows saving diskspace; note that if it is set to `true`, the [direct] must be set to `false`
 * @param [direct] `true` for standard merging which preserves the original order:
 * files are read from the beginning to the end;
 * `false` for inverse merging when result file contains data in the reverse order;
 * note that for direct mode [controlDiskspace] is not possible
 * @param coroutineScope[CoroutineScope]
 */
fun mergeFiles(
    sources: Set<Path>,
    target: Path,
    comparator: Comparator<ByteArray>,
    delimiter: ByteArray,
    bomSymbols: ByteArray,
    sourceBuffer: (Path) -> ByteBuffer,
    targetBuffer: (Path) -> ByteBuffer,
    controlDiskspace: Boolean,
    direct: Boolean,
    coroutineScope: CoroutineScope,
) {
    require(!direct || !controlDiskspace) {
        "Control Diskspace option is not allowed for direct merging"
    }
    require(sources.size > 1) { "Number of given sources (${sources.size}) must be greater than 1" }
    val lineReaderInternalQueueSize = LINE_READER_INTERNAL_QUEUE_SIZE / sources.size
    require(lineReaderInternalQueueSize >= 1) {
        "Number of given sources (${sources.size}) must be less than $LINE_READER_INTERNAL_QUEUE_SIZE"
    }
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

    val segmentSizes = sources.associateWith { file -> AtomicLong(file.fileSize()) }

    target.use { res ->
        sources.use { source ->
            var firstLine = true
            val iterators = mutableListOf<ResourceIterator<ByteArray>>()
            source.forEach { (file, channel) ->
                val segmentSize = checkNotNull(segmentSizes[file])
                val readBuffer = checkNotNull(readBuffers[file])

                val iterator = channel.readLines(
                    direct = direct,
                    startAreaPositionInclusive = 0,
                    endAreaPositionExclusive = segmentSize.get(),
                    listener = { size -> segmentSize.set(size) },
                    buffer = readBuffer,
                    delimiter = delimiter,
                    bomSymbolsLength = bomSymbols.size,
                    coroutineScope = coroutineScope,
                    internalQueueSize = lineReaderInternalQueueSize,
                    onError = {
                        iterators.closeAll(it)
                        throw it
                    }
                )
                iterators.add(iterator)
            }
            if (bomSymbols.isNotEmpty()) {
                res.write(ByteBuffer.wrap(bomSymbols))
            }
            iterators.use {
                mergeIterators(this, comparator).forEach { line ->
                    if (!firstLine) {
                        res.writeData(delimiter, writeBuffer)
                    }
                    firstLine = false
                    res.writeData(line, writeBuffer)
                    if (controlDiskspace) {
                        source.forEach { (file, channel) -> channel.truncate(checkNotNull(segmentSizes[file]).get()) }
                    }
                }
                if (writeBuffer.position() > 0) {
                    writeBuffer.limit(writeBuffer.position())
                    writeBuffer.position(0)
                    res.write(writeBuffer)
                    if (controlDiskspace) {
                        source.forEach { (file, channel) -> channel.truncate(checkNotNull(segmentSizes[file]).get()) }
                    }
                }
            }
        }
    }
    if (controlDiskspace) {
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
    if (data.isEmpty()) {
        return -1 // empty line
    }
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

private inline fun <X, R> Iterable<ResourceIterator<X>>.use(block: Iterable<ResourceIterator<X>>.() -> R): R = try {
    this.block()
} finally {
    closeAll()
}