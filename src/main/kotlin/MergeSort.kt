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
import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.deleteExisting
import kotlin.io.path.fileSize
import kotlin.math.ceil


internal suspend fun suspendSplitAndSort(
    source: Path,
    delimiter: String = "\n",
    comparator: Comparator<String> = defaultComparator<String>(),
    allocatedMemorySizeInBytes: Int = SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES,
    deleteSourceFile: Boolean = false,
    charset: Charset = Charsets.UTF_8,
    coroutineContext: CoroutineContext = Dispatchers.IO,
): List<Path> {
    require(allocatedMemorySizeInBytes >= SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES) {
        "too small allocated-memory-size specified: $allocatedMemorySizeInBytes < min ($SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES)"
    }
    val chunkSize = calcChunkSize(source.fileSize(), allocatedMemorySizeInBytes / 2)
    val readBuffer = ByteBuffer.allocateDirect(chunkSize)
    val writeBuffers = ArrayBlockingQueue<ByteBuffer>(1)
    writeBuffers.add(ByteBuffer.allocateDirect(chunkSize))
    val delimiterBytes = delimiter.toSymbolBytes(charset)

    val scope = CoroutineScope(coroutineContext) + CoroutineName("Writers[${source.fileName}]")

    val bytesComparator = Comparator<ByteArray> { a, b -> comparator.compare(a.toString(charset), b.toString(charset)) }

    val lines = arrayListOf<ByteArray>()
    var chunkPosition = source.fileSize() - chunkSize
    var linePosition = source.fileSize()
    var prevLinePosition = linePosition

    var fileCounter = 1
    val writers = mutableListOf<Job>()
    val res = mutableListOf<Path>()
    source.use { input ->
        input.readLinesAsByteArrays(
            startAreaPositionInclusive = 0,
            endAreaPositionExclusive = source.fileSize(),
            delimiter = delimiterBytes,
            direct = false,
            buffer = readBuffer,
            maxLineLengthInBytes = readBuffer.capacity(),
            coroutineContext = coroutineContext,
            coroutineName = "MergeSortReader",
            listener = {
                if (deleteSourceFile) {
                    input.truncate(it)
                }
            }
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
                        buffers = writeBuffers,
                    )
                )
            }
            lines.add(line)
            prevLinePosition = linePosition
            linePosition -= delimiterBytes.size
            if (linePosition < 0) {
                val linesSnapshot = lines.toTypedArray()
                lines.clear()
                writers.add(
                    scope.writeJob(
                        content = linesSnapshot.sort(bytesComparator),
                        target = res.put(source + ".${fileCounter++}.part"),
                        delimiterBytes = delimiterBytes,
                        buffers = writeBuffers,
                    )
                )
            }
        }
    }
    writers.joinAll()
    scope.ensureActive()
    if (deleteSourceFile) {
        check(source.fileSize() == 0L)
        source.deleteExisting()
    }
    return res
}

private fun CoroutineScope.writeJob(
    content: Array<ByteArray>,
    target: Path,
    delimiterBytes: ByteArray,
    buffers: BlockingQueue<ByteBuffer>
): Job = (this + CoroutineName("Writer[${target.fileName}]")).launch {
    writeLines(content, target, delimiterBytes, buffers)
}

private fun writeLines(
    content: Array<ByteArray>,
    target: Path,
    delimiterBytes: ByteArray,
    buffers: BlockingQueue<ByteBuffer>,
) {
    val buffer = checkNotNull(buffers.poll(SORT_FILE_WRITE_OPERATION_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)) {
        "Unable to obtain write-buffer within $SORT_FILE_WRITE_OPERATION_TIMEOUT_IN_MS ms"
    }
    try {
        var size = 0
        buffer.clear()
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
    } finally {
        buffers.add(buffer)
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

private fun <X> MutableCollection<X>.put(item: X): X {
    add(item)
    return item
}

private fun Array<ByteArray>.sort(comparator: Comparator<ByteArray>): Array<ByteArray> {
    sortWith(comparator)
    return this
}
