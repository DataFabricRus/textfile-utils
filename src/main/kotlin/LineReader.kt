package com.gitlab.sszuev.textfiles

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.math.max
import kotlin.math.min

/**
 * Reads text [String]-lines from the given area of file.
 * The area is set by the specified [startAreaPositionInclusive] and [endAreaPositionExclusive].
 * Since this is IO operation, the [DirectByteBuffer][ByteBuffer.allocateDirect] is preferred.
 *
 * @param [startAreaPositionInclusive][Long] the index to read from, non-negative number of bytes from the beginning of area
 * @param [endAreaPositionExclusive][Long] the bytes index to read to, non-negative number of bytes from the beginning of area
 * @param [direct][Boolean] if `true` the reading is performed from the beginning to the end of area,
 * otherwise the reading is reversed (from the end to start)
 * @param [buffer][ByteBuffer] to use while reading data from file; default `8192`; for IO `DirectByteBuffer` is most appropriate
 * @param [delimiter] lines-separator; default `\n`
 * @param [listener] callback to monitor the process that accepts current position (index); no listener by default
 * @param [coroutineName] the name of coroutine which processes physical (NIO) reading, to be used for async reader
 * @param [coroutineContext][CoroutineContext] to run async reader, default = [Dispatchers.IO]
 * @param [maxLineLengthInBytes][Int] line restriction, to avoid memory lack when there is no delimiter for example, default = `8192`
 * @param [singleOperationTimeoutInMs][Long] to prevent hangs
 * @param [internalQueueSize][Int] to hold lines before emitting
 * @return [Sequence]<[String]> of lines starting from the end of segment to the beginning
 * @throws [IllegalStateException] if line exceeds [maxLineLengthInBytes]
 */
fun SeekableByteChannel.readLines(
    startAreaPositionInclusive: Long = 0,
    endAreaPositionExclusive: Long = size(),
    delimiter: String = "\n",
    listener: (Long) -> Unit = {},
    direct: Boolean = true,
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
    maxLineLengthInBytes: Int = LINE_READER_MAX_LINE_LENGTH_IN_BYTES,
    charset: Charset = Charsets.UTF_8,
    singleOperationTimeoutInMs: Long = LINE_READER_SINGLE_OPERATION_TIMEOUT_IN_MS,
    internalQueueSize: Int = LINE_READER_INTERNAL_QUEUE_SIZE,
    coroutineName: String = "AsyncLineReader",
    coroutineContext: CoroutineContext = Dispatchers.IO,
): Sequence<String> {
    require(buffer.capacity() > 0)
    require(delimiter.isNotEmpty())
    require(startAreaPositionInclusive in 0..endAreaPositionExclusive)
    require(endAreaPositionExclusive <= size())
    val bomSymbols = charset.bomSymbols()
    val startPosition = startAreaPositionInclusive + bomSymbols.size
    val areaSize = endAreaPositionExclusive - startPosition
    if (buffer.capacity() >= areaSize) {
        listener(if (direct) startPosition else endAreaPositionExclusive)
        // read everything into memory
        this.position(startPosition)
        buffer.rewind()
        buffer.limit(areaSize.toInt())
        this.read(buffer)
        buffer.rewind()
        listener(if (direct) endAreaPositionExclusive - 1 else startPosition)
        val bytes = ByteArray(areaSize.toInt())
        buffer.get(bytes)
        if (bytes.isEmpty()) {
            return emptySequence()
        }
        val lines = bytes.toString(charset).split(delimiter).onEach {
            check(
                bytes.size <= maxLineLengthInBytes &&
                        it.length <= maxLineLengthInBytes &&
                        it.toByteArray(charset).size <= maxLineLengthInBytes
            ) {
                "The line is too long (max length = $maxLineLengthInBytes, actual length = ${it.toByteArray(charset).size}"
            }
        }
        return if (direct) {
            lines.asSequence()
        } else {
            sequence {
                lines.indices.reversed().forEach {
                    yield(lines[it])
                }
            }
        }
    }
    return readLinesAsByteArrays(
        startAreaPositionInclusive = startAreaPositionInclusive + bomSymbols.size,
        endAreaPositionExclusive = endAreaPositionExclusive,
        delimiter = delimiter.bytes(charset),
        listener = listener,
        direct = direct,
        buffer = buffer,
        maxLineLengthInBytes = maxLineLengthInBytes,
        singleOperationTimeoutInMs = singleOperationTimeoutInMs,
        internalQueueSize = internalQueueSize,
        coroutineName = coroutineName,
        coroutineContext = coroutineContext,
    ).map { it.toString(charset) }
}

/**
 * Reads text [ByteArray]-lines from the given area of file.
 * The area is set by the specified [startAreaPositionInclusive] and [endAreaPositionExclusive].
 * Since this is IO operation, the [DirectByteBuffer][ByteBuffer.allocateDirect] is preferred.
 *
 * @param [startAreaPositionInclusive][Long] the index to read from, non-negative number of bytes from the beginning of area
 * @param [endAreaPositionExclusive][Long] the bytes index to read to, non-negative number of bytes from the beginning of area
 * @param [direct][Boolean] if `true` the reading is performed from the beginning to the end of area,
 * otherwise the reading is reversed (from the end to start)
 * @param [buffer][ByteBuffer] to use while reading data from file; default `8192`; for IO `DirectByteBuffer` is most appropriate
 * @param [delimiter] lines-separator; default `\n`
 * @param [listener] callback to monitor the process that accepts current position (index); no listener by default
 * @param [coroutineName] the name of coroutine which processes physical (NIO) reading, to be used for async reader
 * @param [coroutineContext][CoroutineContext] to run async reader, default = [Dispatchers.IO]
 * @param [maxLineLengthInBytes][Int] line restriction, to avoid memory lack when there is no delimiter for example, default = `8192`
 * @param [singleOperationTimeoutInMs][Long] to prevent hangs
 * @param [internalQueueSize][Int] to hold lines before emitting
 * @return [Sequence]<[ByteArray]> of lines starting from the end of segment to the beginning
 * @throws [IllegalStateException] if line exceeds [maxLineLengthInBytes]
 */
fun SeekableByteChannel.readLinesAsByteArrays(
    startAreaPositionInclusive: Long = 0,
    endAreaPositionExclusive: Long = size(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    listener: (Long) -> Unit = {},
    direct: Boolean = true,
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
    maxLineLengthInBytes: Int = LINE_READER_MAX_LINE_LENGTH_IN_BYTES,
    singleOperationTimeoutInMs: Long = LINE_READER_SINGLE_OPERATION_TIMEOUT_IN_MS,
    internalQueueSize: Int = LINE_READER_INTERNAL_QUEUE_SIZE,
    coroutineName: String = "AsyncLineReader",
    coroutineContext: CoroutineContext = Dispatchers.IO,
): Sequence<ByteArray> {
    val reader = LineReader(
        source = this,
        direct = direct,
        startAreaPositionInclusive = startAreaPositionInclusive,
        endAreaPositionExclusive = endAreaPositionExclusive,
        listener = listener,
        buffer = buffer,
        itemTimeoutInMs = singleOperationTimeoutInMs,
        delimiter = delimiter,
        maxLineLength = maxLineLengthInBytes,
        queueSize = internalQueueSize,
    )
    (CoroutineScope(coroutineContext) + CoroutineName(coroutineName)).launch {
        reader.run()
    }
    return reader.lines()
}

/**
 * Reads file from the [endAreaPositionExclusive] position to the [startAreaPositionInclusive].
 */
internal class LineReader(
    private val source: SeekableByteChannel,
    private val direct: Boolean,
    private val startAreaPositionInclusive: Long,
    private val endAreaPositionExclusive: Long,
    private val listener: (Long) -> Unit,
    private val buffer: ByteBuffer,
    private val itemTimeoutInMs: Long,
    private val delimiter: ByteArray,
    private val maxLineLength: Int,
    queueSize: Int,
) {

    private val end: AtomicBoolean = AtomicBoolean(false)
    private val error: AtomicReference<Throwable> = AtomicReference()
    private val queue: BlockingQueue<ByteArray> = ArrayBlockingQueue(queueSize)

    fun lines(): Sequence<ByteArray> = sequence {
        while (!end.get() || queue.isNotEmpty()) {
            error.get()?.let { throw it }
            queue.poll()?.let {
                yield(it)
            }
        }
        error.get()?.let { throw it }
    }

    fun run() {
        try {
            if (direct) directRead() else reverseRead()
        } catch (ex: Throwable) {
            error.set(ex)
            throw ex
        } finally {
            end.set(true)
        }
    }

    private fun directRead() {
        var startIndex = startAreaPositionInclusive
        var remainder = ByteArray(0)
        while (startIndex < endAreaPositionExclusive) {
            val endIndex = min(endAreaPositionExclusive - 1, startIndex + buffer.capacity() - 1)
            val readBytes = (endIndex - startIndex + 1).toInt()
            buffer.rewind()
            buffer.limit(readBytes)
            source.position(startIndex)
            source.read(buffer)
            listener(endIndex)
            val linesToRemainder = buffer.directLines(length = readBytes, remainder = ByteBuffer.wrap(remainder))
            remainder = linesToRemainder.second
            val readLines = linesToRemainder.first
            readLines.forEach {
                check(queue.offer(it, itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            if (endIndex == endAreaPositionExclusive - 1) {
                check(queue.offer(remainder, itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            startIndex = endIndex + 1
        }
    }

    private fun reverseRead() {
        listener(endAreaPositionExclusive)
        var endIndex = endAreaPositionExclusive - 1
        var remainder = ByteArray(0)
        while (endIndex >= startAreaPositionInclusive) {
            val startIndex = max(startAreaPositionInclusive, endIndex + 1 - buffer.capacity())
            val readBytes = (endIndex - startIndex + 1).toInt()
            buffer.rewind()
            buffer.limit(readBytes)
            source.position(startIndex)
            source.read(buffer)
            listener(startIndex)
            buffer.rewind()
            val linesToRemainder = buffer.reverseLines(length = readBytes, remainder = ByteBuffer.wrap(remainder))
            remainder = linesToRemainder.second
            val readLines = linesToRemainder.first
            readLines.indices.reversed().forEach {
                check(queue.offer(readLines[it], itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            if (startIndex == startAreaPositionInclusive) {
                check(queue.offer(remainder, itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            endIndex = startIndex - 1
        }
    }

    private fun ByteBuffer.directLines(
        length: Int,
        remainder: ByteBuffer
    ): Pair<List<ByteArray>, ByteArray> {
        val res = split(remainder, remainder.capacity(), this, length, delimiter)
        check(res.isNotEmpty())
        val lines = res.asSequence().take(res.size - 1).map { it.toByteArray() }.onEach { checkLine(it) }
        val nextRemainder = res[res.size - 1].toByteArray()
        checkLine(nextRemainder)
        return lines.toList() to nextRemainder
    }

    private fun ByteBuffer.reverseLines(
        length: Int,
        remainder: ByteBuffer
    ): Pair<List<ByteArray>, ByteArray> {
        val res = split(this, length, remainder, remainder.capacity(), delimiter)
        check(res.isNotEmpty())
        val lines = res.asSequence().drop(1).map { it.toByteArray() }.onEach { checkLine(it) }
        val nextRemainder = res[0].toByteArray()
        checkLine(nextRemainder)
        return lines.toList() to nextRemainder
    }

    private fun checkLine(line: ByteArray) {
        check(line.size <= maxLineLength) {
            "The line is too long (max = $maxLineLength). Position = ${source.position()}, line-length = ${line.size}, direct = $direct"
        }
    }

    companion object {

        private fun split(
            left: ByteBuffer,
            leftSize: Int,
            right: ByteBuffer,
            rightSize: Int,
            delimiter: ByteArray
        ): List<List<Byte>> {
            val size = leftSize + rightSize
            if (size == 0) {
                return emptyList()
            }
            val res = mutableListOf<MutableList<Byte>>()
            res.add(mutableListOf())
            var i = 0
            while (i < size) {
                if (isDelimiter(left, leftSize, right, rightSize, i, delimiter)) {
                    res.add(mutableListOf())
                    i += delimiter.size
                    continue
                }
                res[res.size - 1].add(get(left, leftSize, right, i))
                i++
            }
            return res
        }

        private fun isDelimiter(
            left: ByteBuffer,
            leftSize: Int,
            right: ByteBuffer,
            rightSize: Int,
            index: Int,
            delimiter: ByteArray
        ): Boolean {
            if (index > leftSize + rightSize - delimiter.size) {
                return false
            }
            for (i in delimiter.indices) {
                if (get(left, leftSize, right, i + index) != delimiter[i]) {
                    return false
                }
            }
            return true
        }

        private fun get(left: ByteBuffer, leftSize: Int, right: ByteBuffer, index: Int): Byte {
            return if (index < leftSize) {
                left[index]
            } else {
                right[index - leftSize]
            }
        }
    }
}