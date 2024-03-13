package cc.datafabric.textfileutils.files

import cc.datafabric.iterators.ResourceIterator
import cc.datafabric.textfileutils.iterators.byteArraySimpleComparator
import cc.datafabric.textfileutils.iterators.defaultComparator
import cc.datafabric.textfileutils.iterators.toByteArrayComparator
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
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
import kotlin.coroutines.coroutineContext
import kotlin.io.path.createFile
import kotlin.io.path.deleteExisting
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.isRegularFile
import kotlin.io.path.moveTo
import kotlin.math.max

/**
 * Sorts the content of the given file and writes the result to the specified target file.
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
 * source file and its part files will be truncated during the process and [source] file will be deleted,
 * but the whole process in this case can take a long time.
 *
 * @param [source][Path] existing regular file
 * @param [target][Path] result file; must not exist
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES]
 * @param [controlDiskspace] if `true` source file will be truncated while process and completely deleted at the end of it;
 * this allows saving diskspace, but the whole process will take will require more time
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
) = blockingSort(
    source = source,
    target = target,
    comparator = comparator,
    delimiter = delimiter,
    allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
    controlDiskspace = controlDiskspace,
    charset = charset,
    coroutineContext = coroutineContext
)

/**
 * Sorts the content of the given file and writes the result to the specified target file.
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
 * source file and its part files will be truncated during the process and [source] file will be deleted,
 * but the whole process in this case can take a long time.
 *
 * @param [source][Path] existing regular file
 * @param [target][Path] result file; must not exist
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES]
 * @param [controlDiskspace] if `true` source file will be truncated while process and completely deleted at the end of it;
 * this allows saving diskspace, but the whole process will take will require more time
 * @param [charset][Charset]
 * @param [coroutineContext][CoroutineContext]
 */
fun blockingSort(
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
        source = source,
        target = target,
        comparator = comparator,
        delimiter = delimiter,
        allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
        controlDiskspace = controlDiskspace,
        charset = charset,
    )
}

/**
 * Sorts the content of the given file and writes the result to the specified target file.
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
 * source file and its part files will be truncated during the process and [source] file will be deleted,
 * but the whole process in this case can take a long time.
 *
 * @param [source][Path] existing regular file
 * @param [target][Path] result file; must not exist
 * @param [comparator][Comparator]<[ByteArray]>
 * @param [delimiter][ByteArray] e.g. for UTF-16 `" " = [0, 32]`
 * @param [bomSymbols][ByteArray] e.g. for UTF-16 `[-2, -1]`
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES];
 * the higher memory allocation, the faster the processing
 * @param [controlDiskspace] if `true` source file will be truncated while process and completely deleted at the end of it;
 * this allows saving diskspace, but the whole process will take will require more time
 * @param [coroutineContext][CoroutineContext]
 */
fun blockingSort(
    source: Path,
    target: Path,
    comparator: Comparator<ByteArray> = byteArraySimpleComparator(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    bomSymbols: ByteArray = byteArrayOf(),
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    controlDiskspace: Boolean = false,
    coroutineContext: CoroutineContext = Dispatchers.IO,
) = runBlocking(coroutineContext) {
    suspendSort(
        source = source,
        target = target,
        comparator = comparator,
        delimiter = delimiter,
        bomSymbols = bomSymbols,
        allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
        controlDiskspace = controlDiskspace,
    )
}

/**
 * Sorts the content of the given file and writes a result to the specified target file.
 * This is a suspended method, can be used from a coroutine.
 *
 * Performs sorting in memory if [source] file size is small enough (less than [allocatedMemorySizeInBytes]).
 * For large files the method splits the source on pieces,
 * then sort each in memory with reversed [comparator] and writes to part files,
 * after this it merges parts into single one use direct [comparator] and method [mergeFilesInverse].
 * The memory consumption of all operations is controlled by [allocatedMemorySizeInBytes] parameter.
 *
 * If [controlDiskspace] is `true` no additional diskspace is required:
 * source file and its part files will be truncated during the process and [source] file will be deleted,
 * but the whole process in this case can take a long time.
 *
 * @param [source][Path] existing regular file
 * @param [target][Path] result file; must not exist
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES]
 * @param [controlDiskspace] if `true` source file will be truncated while process and completely deleted at the end of it;
 * this allows saving diskspace, but the whole process will take will require more time
 * @param [charset][Charset]
 */
suspend fun suspendSort(
    source: Path,
    target: Path,
    comparator: Comparator<String> = defaultComparator<String>(),
    delimiter: String = "\n",
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    controlDiskspace: Boolean = false,
    charset: Charset = Charsets.UTF_8,
) = suspendSort(
    source = source,
    target = target,
    comparator = comparator.toByteArrayComparator(),
    delimiter = delimiter.bytes(charset), bomSymbols = charset.bomSymbols(),
    allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
    controlDiskspace = controlDiskspace,
)

/**
 * Sorts the content of the given file and writes a result to the specified target file.
 * This is a suspended method, can be used from a coroutine.
 *
 * Performs sorting in memory if [source] file size is small enough (less than [allocatedMemorySizeInBytes]).
 * For large files the method splits the source on pieces,
 * then sort each in memory with reversed [comparator] and writes to part files,
 * after this it merges parts into single one use direct [comparator] and method [mergeFilesInverse].
 * The memory consumption of all operations is controlled by [allocatedMemorySizeInBytes] parameter.
 *
 * If [controlDiskspace] is `true` no additional diskspace is required:
 * source file and its part files will be truncated during the process and [source] file will be deleted,
 * but the whole process in this case can take a long time.
 *
 * @param [source][Path] existing regular file
 * @param [target][Path] result file; must not exist
 * @param [comparator][Comparator]<[ByteArray]>
 * @param [delimiter][ByteArray] e.g. for UTF-16 `" " = [0, 32]`
 * @param [bomSymbols][ByteArray] e.g. for UTF-16 `[-2, -1]`
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES];
 * the higher memory allocation, the faster the processing:
 * if this number is small but the file is large,
 * then many temporary files are expected and merging them will take a long time.
 * @param [controlDiskspace] if `true` source file will be truncated while process and completely deleted at the end of it;
 * this allows saving diskspace, but the whole process will take will require more time
 */
suspend fun suspendSort(
    source: Path,
    target: Path,
    comparator: Comparator<ByteArray> = byteArraySimpleComparator(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    bomSymbols: ByteArray = byteArrayOf(),
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    controlDiskspace: Boolean = false,
    numberOfOpenDescriptors: Int = SORT_FILE_NUMBER_OF_OPEN_FILE_DESCRIPTORS,
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

    val coroutineScope = CoroutineScope(coroutineContext) + CoroutineName("suspendSort")

    if (source.fileSize() <= 5 * allocatedMemorySizeInBytes / 2) { // small file, memory
        val writeBuffer = ByteBuffer.allocateDirect(source.fileSize().toInt())
        val readBuffer = ByteBuffer.allocateDirect(allocatedMemorySizeInBytes / 2)
        val content = source.use { input ->
            input.readLinesBytes(
                controlDiskSpace = controlDiskspace,
                delimiterBytes = delimiter,
                bomSymbols = bomSymbols,
                buffer = readBuffer,
                coroutineScope = coroutineScope,
            ).toList().toTypedArray()
        }
        writeLines(
            content = content.sort(comparator),
            target = target,
            delimiterBytes = delimiter,
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
                bomSymbols = bomSymbols,
                allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
                controlDiskspace = controlDiskspace,
                coroutineScope = coroutineScope,
            )
        )
        mergeInverseParts(
            parts = parts,
            source = source,
            tmpTarget = tmpTarget,
            comparator = comparator,
            delimiter = delimiter,
            bomSymbols = bomSymbols,
            controlDiskspace = controlDiskspace,
            numberOfOpenDescriptors = numberOfOpenDescriptors,
            allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
            coroutineScope = coroutineScope,
        )
    } finally {
        parts.deleteAll()
    }
    tmpTarget.moveTo(target = target, overwrite = true)
}

private fun mergeInverseParts(
    parts: MutableSet<Path>,
    source: Path,
    tmpTarget: Path,
    comparator: Comparator<ByteArray>,
    delimiter: ByteArray,
    bomSymbols: ByteArray,
    controlDiskspace: Boolean,
    numberOfOpenDescriptors: Int,
    allocatedMemorySizeInBytes: Int,
    coroutineScope: CoroutineScope,
) {
    // while (true) {
    //  if (reversed.size == 1 && direct.isEmpty) {
    //      return reversed(invert[0])
    //  }
    //  if (reversed.isEmpty && direct.size == 1) {
    //      return direct[0]
    //  }
    //  if (reversed.size == 1 && direct.size == 1) {
    //      reversed.add(invert(direct[0]))
    //  }
    //  if (reversed.size > 1) {
    //      val chunk = reversed.removeChunk()
    //      val res = mergeFilesInverse(chunk)
    //      direct.add(res)
    //  }
    //  if (direct.size > 1) {
    //      val chunk = direct.removeChunk()
    //      val res = mergeFilesInverse(chunk)
    //      reverse.add(res)
    //  }
    // }

    val reversed = mutableSetOf<Path>()
    val direct = mutableSetOf<Path>()
    reversed.addAll(parts)
    var fileCounter = parts.size
    while (true) {
        if (reversed.isEmpty() && direct.isEmpty()) {
            throw IllegalStateException("Should not happen")
        }
        if (reversed.isEmpty() && direct.size == 1) {
            direct.single().moveTo(target = tmpTarget, overwrite = true)
            parts.removeAll(direct)
            break
        }
        if (reversed.size == 1 && direct.isEmpty()) {
            invert(
                source = reversed.single(),
                target = tmpTarget,
                controlDiskspace = controlDiskspace,
                delimiter = delimiter,
                bomSymbols = bomSymbols,
            )
            reversed.deleteAll()
            parts.removeAll(reversed)
            break
        }
        if (reversed.size == 1 && direct.size == 1) {
            val targetDirectFile = source + ("." + ++fileCounter + ".part")
            targetDirectFile.createFile()
            parts.add(targetDirectFile)
            invert(
                source = direct.single(),
                target = targetDirectFile,
                controlDiskspace = controlDiskspace,
                delimiter = delimiter,
                bomSymbols = bomSymbols,
            )
            reversed.add(targetDirectFile)
            parts.removeAll(direct)
            direct.clear()
        }
        if (reversed.size > 1) {
            val sourceReversedFiles = reversed.removeChunk(numberOfOpenDescriptors)
            val targetDirectFile = source + ("." + ++fileCounter + ".part")
            targetDirectFile.createFile()
            parts.add(targetDirectFile)
            mergeFilesInverse(
                sources = sourceReversedFiles,
                target = targetDirectFile,
                comparator = comparator,
                delimiter = delimiter,
                bomSymbols = bomSymbols,
                allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
                controlDiskspace = controlDiskspace,
                coroutineScope = coroutineScope,
            )
            sourceReversedFiles.deleteAll()
            direct.add(targetDirectFile)
            parts.removeAll(sourceReversedFiles)
        }
        if (direct.size > 1) {
            val sourceDirectFiles = direct.removeChunk(numberOfOpenDescriptors)
            val targetReversedFile = source + ("." + ++fileCounter + ".part")
            targetReversedFile.createFile()
            parts.add(targetReversedFile)
            mergeFilesInverse(
                sources = sourceDirectFiles,
                target = targetReversedFile,
                comparator = comparator.reversed(),
                delimiter = delimiter,
                bomSymbols = bomSymbols,
                allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
                controlDiskspace = controlDiskspace,
                coroutineScope = coroutineScope,
            )
            sourceDirectFiles.deleteAll()
            reversed.add(targetReversedFile)
            parts.removeAll(sourceDirectFiles)
        }
    }
}

/**
 * Splits the given file on chunks and sorts content of each chunk.
 *
 * @param [source][Path]
 * @param [comparator][Comparator]<[ByteArray]>
 * @param [delimiter][ByteArray] e.g. for UTF-16 `" " = [0, 32]`
 * @param [bomSymbols][ByteArray] e.g. for UTF-16 `[-2, -1]`
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES]
 * @param [controlDiskspace] if `true` source file will be truncated while processing and completely deleted at the end of it;
 * this allows saving diskspace, but the whole process will take will require more time
 * @param [coroutineScope][CoroutineScope]
 * @param [numOfWriteWorkers] number of coroutines that will handle write operations
 * @param [writeToTotalMemRatio] ratio of memory allocated for write operations to [total allocated memory][allocatedMemorySizeInBytes]
 * @return [List]<[Path]> parts of source file with sorted content
 */
@Suppress("DuplicatedCode")
internal suspend fun suspendSplitAndSort(
    source: Path,
    coroutineScope: CoroutineScope,
    comparator: Comparator<ByteArray> = byteArraySimpleComparator(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    bomSymbols: ByteArray = byteArrayOf(),
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    controlDiskspace: Boolean = false,
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

    val lines = arrayListOf<ByteArray>()
    var chunkPosition = source.fileSize() - chunkSize
    var linePosition = source.fileSize()
    var prevLinePosition = linePosition

    var fileCounter = 1
    val writers = mutableListOf<Job>()
    val res = mutableListOf<Path>()

    try {
        source.use { input ->
            input.readLinesBytes(
                controlDiskSpace = controlDiskspace,
                coroutineScope = coroutineScope,
                delimiterBytes = delimiter,
                bomSymbols = bomSymbols,
                buffer = readBuffer,
            ).forEach { line ->
                linePosition -= line.size
                if (linePosition <= chunkPosition) {
                    chunkPosition = prevLinePosition - chunkSize
                    val linesSnapshot = lines.toTypedArray()
                    lines.clear()
                    val file = source + ".${fileCounter++}.part"
                    writers.add(
                        coroutineScope.writeJob(
                            content = linesSnapshot.sort(comparator),
                            target = res.add(file).let { file },
                            delimiterBytes = delimiter,
                            bomSymbols = bomSymbols,
                            buffers = writeBuffers,
                        )
                    )
                }
                lines.add(line)
                prevLinePosition = linePosition
                linePosition -= delimiter.size
            }
        }
        if (lines.isNotEmpty()) {
            val linesSnapshot = lines.toTypedArray()
            lines.clear()
            val file = source + ".${fileCounter++}.part"
            writers.add(
                coroutineScope.writeJob(
                    content = linesSnapshot.sort(comparator),
                    target = res.add(file).let { file },
                    delimiterBytes = delimiter,
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
    delimiterBytes: ByteArray,
    bomSymbols: ByteArray,
    buffer: ByteBuffer,
    coroutineScope: CoroutineScope,
    onError: (Throwable) -> Unit = { throw it },
): ResourceIterator<ByteArray> = asyncReadByteLines(
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
    coroutineScope = coroutineScope,
    onError = onError,
)

private fun CoroutineScope.writeJob(
    content: Array<ByteArray>,
    target: Path,
    delimiterBytes: ByteArray,
    bomSymbols: ByteArray,
    buffers: BlockingQueue<ByteBuffer>
): Job = this.launch {
    try {
        writeLines(content, target, delimiterBytes, bomSymbols, buffers)
    } catch (ex: Exception) {
        this.cancel("Exception while write lines", ex)
    }
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

private fun Array<ByteArray>.sort(comparator: Comparator<ByteArray>): Array<ByteArray> {
    sortWith(comparator)
    return this
}

private fun MutableSet<Path>.removeChunk(n: Int): Set<Path> {
    val res = sortedBy { it.fileSize() }.take(n).toMutableSet()
    removeAll(res)
    if (size == 1 && res.size > 2) {
        val p = res.single()
        res.remove(p)
        add(p)
    }
    return res
}