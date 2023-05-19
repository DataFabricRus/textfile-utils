package com.gitlab.sszuev.textfiles

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import java.util.Arrays
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max
import kotlin.math.min

/**
 * Reads text lines from the given area of file.
 * The area is set by the specified [startPositionInclusive] and [endPositionExclusive]
 *
 * @param [startPositionInclusive][Long] the index to read from, non-negative number of bytes from the beginning of area
 * @param [endPositionExclusive][Long] the bytes index to read to, non-negative number of bytes from the beginning of area
 * @param [direct][Boolean] if `true` the reading is performed from the beginning to the end of area,
 * otherwise the reading is reversed (from the end to start)
 * @param [buffer][ByteBuffer] to use while reading data from file;
 * the memory consumption is approximately three times the buffer size,
 * since bytes from the buffer transforms to [String] array before emit (assuming each [String] size is about `~ 2 * getBytes() + X`)
 * @param [delimiter] lines-separator
 * @param [charset][Charset]
 * @param [listener] function that accepts current position; to monitor the process
 * @param [coroutineName] the name of coroutine which processes physical (NIO) reading
 * @param [singleOperationTimeoutInMs][Long] to prevent hangs
 * @param [internalQueueSize][Int] to hold lines before emitting
 * @return [Sequence] of lines starting from the end of segment to the beginning
 */
fun SeekableByteChannel.readLines(
    startPositionInclusive: Long = 0,
    endPositionExclusive: Long = size(),
    delimiter: String = "\n",
    listener: (Long) -> Unit = {},
    direct: Boolean = true,
    buffer: ByteBuffer = ByteBuffer.allocate(8192),
    charset: Charset = Charsets.UTF_8,
    coroutineName: String = "AsyncLineReader",
    singleOperationTimeoutInMs: Long = 60 * 1000L,
    internalQueueSize: Int = 1024,
): Sequence<String> {
    require(buffer.capacity() > 0)
    require(delimiter.isNotEmpty())
    require(startPositionInclusive in 0..endPositionExclusive)
    require(endPositionExclusive <= size())
    val areaSize = endPositionExclusive - startPositionInclusive
    if (buffer.capacity() >= areaSize) {
        listener(if (direct) startPositionInclusive else endPositionExclusive)
        // read everything into memory
        this.position(startPositionInclusive)
        buffer.rewind()
        buffer.limit(areaSize.toInt())
        this.read(buffer)
        listener(if (direct) endPositionExclusive else startPositionInclusive)
        val bytes = Arrays.copyOf(buffer.array(), areaSize.toInt())
        if (bytes.isEmpty()) {
            return emptySequence()
        }
        val lines = bytes.toString(charset).split(delimiter)
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
    val reader = LineReader(
        source = this,
        direct = direct,
        startPositionInclusive = startPositionInclusive,
        endPositionExclusive = endPositionExclusive,
        listener = listener,
        buffer = buffer,
        delimiter = delimiter,
        charset = charset,
        itemTimeoutInMs = singleOperationTimeoutInMs,
        queueSize = internalQueueSize,
    )
    (CoroutineScope(Dispatchers.IO) + CoroutineName(coroutineName)).launch { reader.run() }
    return reader.lines()
}

/**
 * Reads file from the [endPositionExclusive] position to the [startPositionInclusive].
 */
internal class LineReader(
    private val source: SeekableByteChannel,
    private val direct: Boolean,
    private val startPositionInclusive: Long,
    private val endPositionExclusive: Long,
    private val listener: (Long) -> Unit,
    private val buffer: ByteBuffer,
    private val charset: Charset,
    private val itemTimeoutInMs: Long,
    delimiter: String,
    queueSize: Int,
) {

    private val end: AtomicBoolean = AtomicBoolean(false)
    private val error: AtomicReference<Throwable> = AtomicReference()
    private val queue: BlockingQueue<String> = ArrayBlockingQueue(queueSize)
    private val delimiter: ByteArray = delimiter.toByteArray(charset)

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
        var startIndex = startPositionInclusive
        var remainder = ByteArray(0)
        while (startIndex < endPositionExclusive) {
            val endIndex = min(endPositionExclusive - 1, startIndex + buffer.capacity() - 1)
            val readBytes = (endIndex - startIndex + 1).toInt()
            buffer.rewind()
            buffer.limit(readBytes)
            source.position(startIndex)
            source.read(buffer)
            listener(endIndex)
            val linesToRemainder = buffer.array().readDirectLines(length = readBytes, remainder = remainder)
            remainder = linesToRemainder.second
            val readLines = linesToRemainder.first
            readLines.forEach {
                check(queue.offer(it, itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            if (endIndex == endPositionExclusive - 1) {
                check(queue.offer(remainder.toString(charset), itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            startIndex = endIndex + 1
        }
        listener(endPositionExclusive)
    }

    private fun reverseRead() {
        listener(endPositionExclusive)
        var endIndex = endPositionExclusive - 1
        var remainder = ByteArray(0)
        while (endIndex >= startPositionInclusive) {
            val startIndex = max(startPositionInclusive, endIndex + 1 - buffer.capacity())
            val readBytes = (endIndex - startIndex + 1).toInt()
            buffer.rewind()
            buffer.limit(readBytes)
            source.position(startIndex)
            source.read(buffer)
            listener(startIndex)
            val linesToRemainder = buffer.array().readReverseLines(length = readBytes, remainder = remainder)
            remainder = linesToRemainder.second
            val readLines = linesToRemainder.first
            readLines.indices.reversed().forEach {
                check(queue.offer(readLines[it], itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            if (startIndex == startPositionInclusive) {
                check(queue.offer(remainder.toString(charset), itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            endIndex = startIndex - 1
        }
    }

    fun lines(): Sequence<String> = sequence {
        while (!end.get() || queue.isNotEmpty()) {
            error.get()?.let { throw it }
            queue.poll()?.let {
                yield(it)
            }
        }
        error.get()?.let { throw it }
    }

    private fun ByteArray.readDirectLines(
        length: Int,
        remainder: ByteArray
    ): Pair<List<String>, ByteArray> {
        val res = split(remainder, remainder.size, this, length, delimiter)
        check(res.isNotEmpty())
        val lines =
            res.asSequence().take(res.size - 1).map { it.toByteArray().toString(charset) }
        val nextRemainder = res[res.size - 1].toByteArray()
        return lines.toList() to nextRemainder
    }

    private fun ByteArray.readReverseLines(
        length: Int,
        remainder: ByteArray
    ): Pair<List<String>, ByteArray> {
        val res = split(this, length, remainder, remainder.size, delimiter)
        check(res.isNotEmpty())
        val lines = res.asSequence().drop(1).map { it.toByteArray().toString(charset) }
        val nextRemainder = res[0].toByteArray()
        return lines.toList() to nextRemainder
    }

    companion object {

        private fun split(
            left: ByteArray,
            leftSize: Int,
            right: ByteArray,
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
            left: ByteArray,
            leftSize: Int,
            right: ByteArray,
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

        private fun get(left: ByteArray, leftSize: Int, right: ByteArray, index: Int): Byte {
            return if (index < leftSize) {
                left[index]
            } else {
                right[index - leftSize]
            }
        }
    }
}