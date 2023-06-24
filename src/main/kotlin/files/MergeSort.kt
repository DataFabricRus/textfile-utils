package com.gitlab.sszuev.textfiles.files

import com.gitlab.sszuev.textfiles.iterators.ResourceIterator
import com.gitlab.sszuev.textfiles.iterators.defaultComparator
import com.gitlab.sszuev.textfiles.iterators.toByteArrayComparator
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlinx.coroutines.runBlocking
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.createFile
import kotlin.io.path.deleteExisting
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.isRegularFile
import kotlin.io.path.moveTo
import kotlin.math.max

/**
 * Sorts the content of the given file and writes result to the specified target file.
 * This method **blocks** the current thread _interruptibly_ until its completion.
 * This function **should not** be used from a coroutine.
 *
 * Performs sorting in memory if [source] file size is small enough (less than [allocatedMemorySizeInBytes]).
 * For large files the method splits the source on pieces,
 * then sort each in memory with reversed [comparator] and writes to part files,
 * after this it merges parts into single one use direct [comparator] and method [mergeFilesInverse].
 * The memory consumption of all operations is controlled by [allocatedMemorySizeInBytes] parameter.
 *
 * If [controlDiskspace] is `true` no additional diskspace is required:
 * source file and its parts files will be truncated during the process and [source] file will be deleted,
 * but the whole process in this case can take a long time.
 *
 * @param [source][Path] existing regular file
 * @param [target][Path] result file, must not exist
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES]
 * @param [controlDiskspace] if `true` source file will be truncated while process and completely deleted at the end of it;
 * this allows to save diskspace, but the whole process will take will require more time
 * @param [charset][Charset]
 * @param [coroutineContext][CoroutineContext]
 */
fun sort(
    source: Path,
    target: Path,
    comparator: Comparator<String> = defaultComparator<String>(),
    delimiter: String = "\n",
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    controlDiskspace: Boolean = false,
    charset: Charset = Charsets.UTF_8,
    coroutineContext: CoroutineContext = Dispatchers.IO,
) = runBlocking(coroutineContext) {
    suspendSort(
        source,
        target,
        comparator,
        delimiter,
        allocatedMemorySizeInBytes,
        controlDiskspace,
        charset,
        coroutineContext
    )
}

/**
 * Sorts the content of the given file and writes result to the specified target file.
 * This is a suspended method, can be used from a coroutine.
 *
 * Performs sorting in memory if [source] file size is small enough (less than [allocatedMemorySizeInBytes]).
 * For large files the method splits the source on pieces,
 * then sort each in memory with reversed [comparator] and writes to part files,
 * after this it merges parts into single one use direct [comparator] and method [mergeFilesInverse].
 * The memory consumption of all operations is controlled by [allocatedMemorySizeInBytes] parameter.
 *
 * If [controlDiskspace] is `true` no additional diskspace is required:
 * source file and its parts files will be truncated during the process and [source] file will be deleted,
 * but the whole process in this case can take a long time.
 *
 * @param [source][Path] existing regular file
 * @param [target][Path] result file, must not exist
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES]
 * @param [controlDiskspace] if `true` source file will be truncated while process and completely deleted at the end of it;
 * this allows to save diskspace, but the whole process will take will require more time
 * @param [charset][Charset]
 * @param [coroutineContext][CoroutineContext]
 */
suspend fun suspendSort(
    source: Path,
    target: Path,
    comparator: Comparator<String> = defaultComparator<String>(),
    delimiter: String = "\n",
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    controlDiskspace: Boolean = false,
    charset: Charset = Charsets.UTF_8,
    coroutineContext: CoroutineContext = Dispatchers.IO,
) {
    require(source.exists()) { "The source file <$source> does not exist" }
    require(source.isRegularFile()) { "The source <$source> is not a file" }
    require(!target.exists()) { "The target <$target> already exists." }
    require(allocatedMemorySizeInBytes >= SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES) {
        "Too small allocated-memory-size specified: $allocatedMemorySizeInBytes < min ($SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES)"
    }

    if (controlDiskspace) {
        require(!sameFilePaths(source, target)) { "Control diskspace = true, but source = target (<$source>)" }
    }

    val bomSymbols = charset.bomSymbols()
    val delimiterBytes = delimiter.bytes(charset)

    if (source.fileSize() <= allocatedMemorySizeInBytes) { // small file, memory
        val writeBuffer = ByteBuffer.allocateDirect(allocatedMemorySizeInBytes / 2)
        val readBuffer = ByteBuffer.allocateDirect(allocatedMemorySizeInBytes / 2)
        val content = source.use { input ->
            input.readLinesBytes(
                controlDiskSpace = controlDiskspace,
                coroutineContext = coroutineContext,
                delimiterBytes = delimiterBytes,
                bomSymbols = bomSymbols,
                buffer = readBuffer,
            ).toList().toTypedArray()
        }
        writeLines(
            content = content.sort(comparator.toByteArrayComparator(charset, hashMapOf())),
            target = target,
            delimiterBytes = delimiterBytes,
            bomSymbols = bomSymbols,
            buffer = writeBuffer
        )
        if (controlDiskspace) {
            source.deleteExisting()
        }
        return
    }
    // large file
    val tmpTarget = target + ".sorted"
    tmpTarget.createFile()
    val parts = mutableSetOf<Path>()
    try {
        parts.addAll(
            suspendSplitAndSort(
                source = source,
                comparator = comparator.reversed(),
                delimiter = delimiter,
                allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
                controlDiskspace = controlDiskspace,
                charset = charset,
                coroutineContext = coroutineContext,
            )
        )
        if (parts.isNotEmpty()) {
            mergeFilesInverse(
                sources = parts,
                target = tmpTarget,
                comparator = comparator,
                delimiter = delimiter,
                charset = charset,
                allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
                controlDiskspace = controlDiskspace,
            )
        }
    } finally {
        parts.deleteAll()
    }
    tmpTarget.moveTo(target = target, overwrite = true)
}

/**
 * Splits the given file on chunks and sorts content of each chunk.
 *
 * @param [source][Path]
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES]
 * @param [controlDiskspace] if `true` source file will be truncated while processing and completely deleted at the end of it;
 * this allows to save diskspace, but the whole process will take will require more time
 * @param [charset][Charset]
 * @param [coroutineContext][CoroutineContext]
 * @param [numOfWriteWorkers] number of coroutines that will handle write operations
 * @param [writeToTotalMemRatio] ratio of memory allocated for write operations to [total allocated memory][allocatedMemorySizeInBytes]
 * @return [List]<[Path]> parts of source file with sorted content
 */
internal suspend fun suspendSplitAndSort(
    source: Path,
    comparator: Comparator<String> = defaultComparator<String>(),
    delimiter: String = "\n",
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    controlDiskspace: Boolean = false,
    charset: Charset = Charsets.UTF_8,
    coroutineContext: CoroutineContext = Dispatchers.IO,
    numOfWriteWorkers: Int = SORT_FILE_NUMBER_OF_WRITE_WORKERS,
    writeToTotalMemRatio: Double = SORT_FILE_WRITE_BUFFER_TO_TOTAL_MEMORY_ALLOCATION_RATIO,
): List<Path> {
    require(allocatedMemorySizeInBytes >= SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES) {
        "Too small allocated-memory-size specified: $allocatedMemorySizeInBytes < min ($SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES)"
    }
    require(numOfWriteWorkers > 0)
    require(writeToTotalMemRatio > 0.0 && writeToTotalMemRatio < 1.0)

    val chunkSize = calcChunkSize(
        totalSize = source.fileSize(),
        maxChunkSize = (allocatedMemorySizeInBytes * writeToTotalMemRatio / numOfWriteWorkers).toInt()
    )
    val readBufferSize =
        max(MAX_LINE_LENGTH_IN_BYTES, allocatedMemorySizeInBytes - chunkSize * numOfWriteWorkers)
    val readBuffer = ByteBuffer.allocateDirect(readBufferSize)
    val writeBuffers = ArrayBlockingQueue<ByteBuffer>(numOfWriteWorkers)
    writeBuffers.add(ByteBuffer.allocateDirect(chunkSize))

    val coroutineScope = CoroutineScope(coroutineContext) + CoroutineName("Writers[${source.fileName}]")

    val lines = arrayListOf<ByteArray>()
    var chunkPosition = source.fileSize() - chunkSize
    var linePosition = source.fileSize()
    var prevLinePosition = linePosition

    var fileCounter = 1
    val writers = mutableListOf<Job>()
    val res = mutableListOf<Path>()
    val bomSymbols = charset.bomSymbols()
    val delimiterBytes = delimiter.bytes(charset)

    try {
        source.use { input ->
            input.readLinesBytes(
                controlDiskSpace = controlDiskspace,
                coroutineContext = coroutineContext,
                delimiterBytes = delimiterBytes,
                bomSymbols = bomSymbols,
                buffer = readBuffer,
            ).forEach { line ->
                linePosition -= line.size
                if (linePosition <= chunkPosition) {
                    chunkPosition = prevLinePosition - chunkSize
                    val linesSnapshot = lines.toTypedArray()
                    lines.clear()
                    writers.add(
                        coroutineScope.writeJob(
                            content = linesSnapshot.sort(comparator.toByteArrayComparator(charset, hashMapOf()) ),
                            target = res.put(source + ".${fileCounter++}.part"),
                            delimiterBytes = delimiterBytes,
                            bomSymbols = bomSymbols,
                            buffers = writeBuffers,
                        )
                    )
                }
                lines.add(line)
                prevLinePosition = linePosition
                linePosition -= delimiterBytes.size
            }
        }
        if (lines.isNotEmpty()) {
            val linesSnapshot = lines.toTypedArray()
            lines.clear()
            writers.add(
                coroutineScope.writeJob(
                    content = linesSnapshot.sort(comparator.toByteArrayComparator(charset, hashMapOf())),
                    target = res.put(source + ".${fileCounter++}.part"),
                    delimiterBytes = delimiterBytes,
                    bomSymbols = bomSymbols,
                    buffers = writeBuffers,
                )
            )
        }
        writers.joinAll()
    } catch (ex: Exception) {
        res.deleteAll()
        res.clear()
        throw ex
    }
    coroutineScope.ensureActive()
    if (controlDiskspace) {
        check(source.fileSize() == bomSymbols.size.toLong())
        source.deleteExisting()
    }
    return res
}

internal fun calcChunkSize(totalSize: Long, maxChunkSize: Int): Int {
    require(maxChunkSize in 1..totalSize)
    if (totalSize == maxChunkSize.toLong() || totalSize % maxChunkSize == 0L) {
        return maxChunkSize
    }
    var res = maxChunkSize
    while (totalSize - (totalSize / res) * res > res || totalSize % res < res * SORT_FILE_CHUNK_GAP) {
        res--
    }
    check(res > 0) { "total=$totalSize, max=$maxChunkSize, chunk=$res" }
    return res
}

private fun SeekableByteChannel.readLinesBytes(
    controlDiskSpace: Boolean = false,
    coroutineContext: CoroutineContext = Dispatchers.IO,
    delimiterBytes: ByteArray,
    bomSymbols: ByteArray,
    buffer: ByteBuffer,
): ResourceIterator<ByteArray> {
    return asyncReadByteLines(
        startAreaPositionInclusive = bomSymbols.size.toLong(),
        endAreaPositionExclusive = size(),
        delimiter = delimiterBytes,
        listener = {
            if (controlDiskSpace) {
                truncate(it)
            }
        },
        direct = false,
        buffer = buffer,
        maxLineLengthInBytes = buffer.capacity(),
        coroutineName = "MergeSortReader",
        coroutineContext = coroutineContext
    )
}

private fun CoroutineScope.writeJob(
    content: Array<ByteArray>,
    target: Path,
    delimiterBytes: ByteArray,
    bomSymbols: ByteArray,
    buffers: BlockingQueue<ByteBuffer>
): Job = (this + CoroutineName("Writer[${target.fileName}]")).launch {
    writeLines(content, target, delimiterBytes, bomSymbols, buffers)
}

private fun writeLines(
    content: Array<ByteArray>,
    target: Path,
    delimiterBytes: ByteArray,
    bomSymbols: ByteArray,
    buffers: BlockingQueue<ByteBuffer>,
) {
    target.deleteIfExists()
    val buffer = checkNotNull(buffers.poll(SORT_FILE_WRITE_OPERATION_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)) {
        "Unable to obtain write-buffer within $SORT_FILE_WRITE_OPERATION_TIMEOUT_IN_MS ms"
    }
    try {
        writeLines(content, target, delimiterBytes, bomSymbols, buffer)
    } finally {
        buffers.add(buffer)
    }
}

private fun writeLines(
    content: Array<ByteArray>,
    target: Path,
    delimiterBytes: ByteArray,
    bomSymbols: ByteArray,
    buffer: ByteBuffer,
) {
    var size = bomSymbols.size
    buffer.clear()
    buffer.put(bomSymbols)
    content.forEachIndexed { index, line ->
        buffer.put(line)
        size += line.size
        if (index != content.size - 1) {
            buffer.put(delimiterBytes)
            size += delimiterBytes.size
        }
    }
    buffer.rewind()
    buffer.limit(size)
    target.use(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW) {
        it.write(buffer)
    }
}

private fun <X> MutableCollection<X>.put(item: X): X {
    add(item)
    return item
}

private fun Array<ByteArray>.sort(comparator: Comparator<ByteArray>): Array<ByteArray> {
    sortWith(comparator)
    return this
}
