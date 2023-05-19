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

/**
 * Reads [segmentSize] bytes from the channel starting from the position `[segmentSize] - 1` to the beginning (`position = 0`).
 *
 * @param [segmentSize][Long] initial size of processing area
 * @param [buffer][ByteBuffer] to use while reading data from file;
 * the memory consumption is approximately three times the buffer size,
 * since bytes from the buffer transforms to Strings (assuming each [String] size is about `~ 2 * getBytes() + X`)
 * @param [delimiter] lines-separator
 * @param [charset][Charset]
 * @param [listener] function that accepts current segment size, to monitor the process
 * @param [coroutineName] the name of coroutine which processes physical (NIO) reading
 * @param [singleOperationTimeoutInMs][Long] to prevent hangs
 * @param [internalQueueSize][Int] to hold lines before emitting
 * @return [Sequence] of lines starting from the end of segment to the beginning
 */
fun SeekableByteChannel.readLines(
    segmentSize: Long,
    buffer: ByteBuffer,
    delimiter: String = "\n",
    charset: Charset = Charsets.UTF_8,
    listener: (Long) -> Unit = {},
    coroutineName: String = "AsyncInverseReader",
    singleOperationTimeoutInMs: Long = 60 * 1000L,
    internalQueueSize: Int = 1024,
): Sequence<String> {
    require(buffer.capacity() > 0)
    require(delimiter.isNotEmpty())
    if (buffer.capacity() >= segmentSize) {
        val size = segmentSize.toInt()
        // read everything into memory
        this.position(0)
        this.read(buffer)
        listener(0)
        val bytes = Arrays.copyOf(buffer.array(), size)
        if (bytes.isEmpty()) {
            return emptySequence()
        }
        val lines = bytes.toString(charset).split(delimiter)
        return sequence {
            lines.indices.reversed().forEach {
                yield(lines[it])
            }
        }
    }
    val reader = InverseLineReader(
        source = this,
        segmentSize = segmentSize,
        listener = listener,
        buffer = buffer,
        delimiter = delimiter,
        charset = charset,
        itemTimeoutInMs = singleOperationTimeoutInMs,
        queueSize = internalQueueSize
    )
    (CoroutineScope(Dispatchers.IO) + CoroutineName(coroutineName)).launch { reader.run() }
    return reader.lines()
}

/**
 * Reads file from the [segmentSize] position to the beginning of the file.
 */
internal class InverseLineReader(
    private val source: SeekableByteChannel,
    private val segmentSize: Long,
    private val listener: (Long) -> Unit,
    private val buffer: ByteBuffer,
    private val charset: Charset,
    private val itemTimeoutInMs: Long,
    delimiter: String,
    queueSize: Int,
) {
    init {
        require(segmentSize > 0)
        require(delimiter.isNotEmpty())
    }

    private val end: AtomicBoolean = AtomicBoolean(false)
    private val error: AtomicReference<Throwable> = AtomicReference()
    private val queue: BlockingQueue<String> = ArrayBlockingQueue(queueSize)
    private val delimiter: ByteArray = delimiter.toByteArray(charset)

    fun run() {
        try {
            read()
        } catch (ex: Throwable) {
            error.set(ex)
            throw ex
        } finally {
            end.set(true)
        }
    }

    private fun read() {
        listener(segmentSize)
        var lastIndex = segmentSize - 1
        var remainder = ByteArray(0)
        while (lastIndex >= 0) {
            val startIndex = max(0, lastIndex + 1 - buffer.capacity())
            val readBytes = (lastIndex + 1 - startIndex).toInt()
            readBlock(startIndex)
            listener(startIndex)
            val linesToRemainder = buffer.array().readLines(length = readBytes, remainder = remainder)
            remainder = linesToRemainder.second
            val readLines = linesToRemainder.first
            readLines.indices.reversed().forEach {
                check(queue.offer(readLines[it], itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            if (startIndex == 0L) {
                check(queue.offer(remainder.toString(charset), itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            lastIndex = startIndex - 1
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

    private fun ByteArray.readLines(
        length: Int,
        remainder: ByteArray
    ): Pair<List<String>, ByteArray> {
        val res = split(this, length, remainder, delimiter)
        check(res.isNotEmpty())
        val lines = res.drop(1).map { it.toByteArray().toString(charset) }
        val nextRemainder = res[0].toByteArray()
        return lines to nextRemainder
    }

    private fun readBlock(startIndex: Long): Int {
        buffer.rewind()
        source.position(startIndex)
        return source.read(buffer)
    }

    companion object {

        private fun split(left: ByteArray, leftSize: Int, right: ByteArray, delimiter: ByteArray): List<List<Byte>> {
            val size = leftSize + right.size
            if (size == 0) {
                return emptyList()
            }
            val res = mutableListOf<MutableList<Byte>>()
            res.add(mutableListOf())
            var i = 0
            while (i < size) {
                if (isDelimiter(left, leftSize, right, i, delimiter)) {
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
            index: Int,
            delimiter: ByteArray
        ): Boolean {
            if (index > leftSize + right.size - delimiter.size) {
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