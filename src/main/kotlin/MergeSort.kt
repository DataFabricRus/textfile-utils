package com.gitlab.sszuev.textfiles

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
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
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.isRegularFile
import kotlin.io.path.moveTo
import kotlin.math.ceil

/**
 * Sorts the content of the given file and writes result to the specified target file.
 * Performs sorting in memory if [source] file size is less than [allocatedMemorySizeInBytes].
 * For large files the method splits the source on pieces,
 * then sort each in memory with reversed [comparator] and writes to part files,
 * after this it merges parts into single one use direct [comparator] and method [mergeFilesInverse].
 * The memory consumption of all operations is controlled by [allocatedMemorySizeInBytes] parameter.
 * If [deleteSourceFile] is `true` no additional memory is required and [source] file will be deleted.
 *
 * @param [source][Path]
 * @param [target][Path]
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES]
 * @param [deleteSourceFile] if `true` source file will be truncated while process and completely deleted at the end of it;
 * this allows to save diskspace
 * @param [charset][Charset]
 * @param [coroutineContext][CoroutineContext]
 */
suspend fun suspendSort(
    source: Path,
    target: Path,
    comparator: Comparator<String> = defaultComparator<String>(),
    delimiter: String = "\n",
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    deleteSourceFile: Boolean = false,
    charset: Charset = Charsets.UTF_8,
    coroutineContext: CoroutineContext = Dispatchers.IO,
) {
    require(source.exists()) { "Source file <$source> does not exist" }
    require(source.isRegularFile()) { "Source <$source> is not a file" }
    require(allocatedMemorySizeInBytes >= SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES) {
        "Too small allocated-memory-size specified: $allocatedMemorySizeInBytes < min ($SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES)"
    }
    if (deleteSourceFile) {
        require(!sameFilePaths(source, target)) { "Delete source = true, but source = target (<$source>)" }
    }

    val bomSymbols = charset.bomSymbols()
    val delimiterBytes = delimiter.bytes(charset)

    if (source.fileSize() <= allocatedMemorySizeInBytes) { // small file, memory
        val writeBuffer = ByteBuffer.allocateDirect(allocatedMemorySizeInBytes / 2)
        val readBuffer = ByteBuffer.allocateDirect(allocatedMemorySizeInBytes / 2)
        val content = source.use { input ->
            input.readLinesBytes(
                deleteSourceFile = deleteSourceFile,
                coroutineContext = coroutineContext,
                delimiterBytes = delimiterBytes,
                bomSymbols = bomSymbols,
                buffer = readBuffer,
            ).toList().toTypedArray()
        }
        target.deleteExisting()
        writeLines(content.sort(comparator.toByteComparator(charset)), target, delimiterBytes, bomSymbols, writeBuffer)
        if (deleteSourceFile) {
            source.deleteExisting()
        }
        return
    }
    // large file
    val tmpTarget = target + ".sorted"
    tmpTarget.createFile()

    val parts = suspendSplitAndSort(
        source = source,
        comparator = comparator.reversed(),
        delimiter = delimiter,
        allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
        deleteSourceFile = deleteSourceFile,
        charset = charset,
        coroutineContext = coroutineContext,
    )
    mergeFilesInverse(
        sources = parts.toSet(),
        target = tmpTarget,
        comparator = comparator,
        delimiter = delimiter,
        charset = charset,
        allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
        deleteSourceFiles = deleteSourceFile,
    )
    tmpTarget.moveTo(target = target, overwrite = true)
}

/**
 * Splits the given file on chunks and sorts each part.
 * @param [source][Path]
 * @param [comparator][Comparator]<[String]>
 * @param [delimiter][String]
 * @param [allocatedMemorySizeInBytes][Int] the approximate allowed memory consumption;
 * must not be less than [SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES]
 * @param [deleteSourceFile] if `true` source file will be truncated while process and completely deleted at the end of it;
 * this allows to save diskspace
 * @param [charset][Charset]
 * @param [coroutineContext][CoroutineContext]
 */
internal suspend fun suspendSplitAndSort(
    source: Path,
    comparator: Comparator<String> = defaultComparator<String>(),
    delimiter: String = "\n",
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    deleteSourceFile: Boolean = false,
    charset: Charset = Charsets.UTF_8,
    coroutineContext: CoroutineContext = Dispatchers.IO,
): List<Path> {
    require(allocatedMemorySizeInBytes >= SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES) {
        "Too small allocated-memory-size specified: $allocatedMemorySizeInBytes < min ($SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES)"
    }
    val chunkSize = calcChunkSize(source.fileSize(), allocatedMemorySizeInBytes / 2)
    val readBuffer = ByteBuffer.allocateDirect(chunkSize)
    val writeBuffers = ArrayBlockingQueue<ByteBuffer>(1)
    writeBuffers.add(ByteBuffer.allocateDirect(chunkSize))

    val scope = CoroutineScope(coroutineContext) + CoroutineName("Writers[${source.fileName}]")

    val bytesComparator = comparator.toByteComparator(charset)

    val lines = arrayListOf<ByteArray>()
    var chunkPosition = source.fileSize() - chunkSize
    var linePosition = source.fileSize()
    var prevLinePosition = linePosition

    var fileCounter = 1
    val writers = mutableListOf<Job>()
    val res = mutableListOf<Path>()
    val bomSymbols = charset.bomSymbols()
    val delimiterBytes = delimiter.bytes(charset)

    source.use { input ->
        input.readLinesBytes(
            deleteSourceFile = deleteSourceFile,
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
                    scope.writeJob(
                        content = linesSnapshot.sort(bytesComparator),
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
            scope.writeJob(
                content = linesSnapshot.sort(bytesComparator),
                target = res.put(source + ".${fileCounter++}.part"),
                delimiterBytes = delimiterBytes,
                bomSymbols = bomSymbols,
                buffers = writeBuffers,
            )
        )
    }
    writers.joinAll()
    scope.ensureActive()
    if (deleteSourceFile) {
        check(source.fileSize() == bomSymbols.size.toLong())
        source.deleteExisting()
    }
    return res
}

internal fun <X> MutableCollection<X>.put(item: X): X {
    add(item)
    return item
}

internal fun Array<ByteArray>.sort(comparator: Comparator<ByteArray>): Array<ByteArray> {
    sortWith(comparator)
    return this
}

internal fun Comparator<String>.toByteComparator(charset: Charset) =
    Comparator<ByteArray> { a, b -> this.compare(a.toString(charset), b.toString(charset)) }

private fun SeekableByteChannel.readLinesBytes(
    deleteSourceFile: Boolean = false,
    coroutineContext: CoroutineContext = Dispatchers.IO,
    delimiterBytes: ByteArray,
    bomSymbols: ByteArray,
    buffer: ByteBuffer,
): Sequence<ByteArray> {
    return readLinesAsByteArrays(
        startAreaPositionInclusive = bomSymbols.size.toLong(),
        endAreaPositionExclusive = size(),
        delimiter = delimiterBytes,
        listener = {
            if (deleteSourceFile) {
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

private fun calcChunkSize(totalSize: Long, maxChunkSize: Int): Int {
    var res = maxChunkSize
    while (ceil(totalSize.toDouble() / res) * res > totalSize + res * SORT_FILE_CHUNK_GAP) {
        res--
    }
    check(res > 0)
    return res
}