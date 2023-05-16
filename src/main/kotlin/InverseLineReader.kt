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
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max

/**
 * Reads [segmentSize] bytes from the channel starting from the position `[segmentSize] - 1` to the beginning (`position = 0`).
 * @param [segmentSize][AtomicLong] modifiable to monitor process
 * @param [buffer][ByteBuffer]
 * @param [delimiter] lines-separator
 * @param [charset]
 * @param [coroutineName]
 * @param [singleOperationTimeoutInMs][Long]
 * @param [internalQueueSize]
 * @return [Sequence] of lines starting from the end of segment to the beginning
 */
fun SeekableByteChannel.readLines(
    segmentSize: AtomicLong,
    buffer: ByteBuffer,
    delimiter: String = "\n",
    charset: Charset = Charsets.UTF_8,
    coroutineName: String = "AsyncInverseReader",
    singleOperationTimeoutInMs: Long = 60 * 1000L,
    internalQueueSize: Int = 1024,
): Sequence<String> {
    if (buffer.capacity() >= segmentSize.get()) {
        // read everything into memory
        val size = segmentSize.get().toInt()
        this.position(0)
        this.read(buffer)
        segmentSize.set(0)
        val bytes = Arrays.copyOf(buffer.array(), size)
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
    private val segmentSize: AtomicLong,
    private val buffer: ByteBuffer,
    delimiter: String,
    private val charset: Charset,
    private val itemTimeoutInMs: Long,
    queueSize: Int,
) {
    init {
        require(segmentSize.get() > 0)
        require(delimiter.isNotEmpty())
    }

    private val end: AtomicBoolean = AtomicBoolean(false)
    private val error: AtomicReference<Throwable> = AtomicReference()
    private val queue: BlockingQueue<String> = ArrayBlockingQueue(queueSize)
    private val delimiter: ByteArray = delimiter.toByteArray(charset)

    private fun stop() {
        end.set(true)
    }

    fun run() {
        try {
            read()
        } catch (ex: Throwable) {
            error.set(ex)
            throw ex
        } finally {
            stop()
        }
    }

    private fun read() {
        var index = segmentSize.get() - 1
        var remainder = ByteArray(0)
        while (index > 0) {
            val startIndex = max(0, index + 1 - buffer.capacity())
            val readBytes = (index + 1 - startIndex).toInt()
            readBlock(startIndex)
            val linesToRemainder = buffer.array().readLines(length = readBytes, remainder = remainder)
            remainder = linesToRemainder.second
            val readLines = linesToRemainder.first
            readLines.indices.reversed().forEach {
                check(queue.offer(readLines[it], itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            if (startIndex == 0L) {
                check(queue.offer(remainder.toString(charset), itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
            index = startIndex - 1
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
        val lines = res.drop(1).map { it.toByteArray().toString(charset) }
        val nextRemainder = res[0].toByteArray()
        return lines to nextRemainder
    }

    private fun readBlock(startIndex: Long): Int {
        val size = segmentSize.get()
        check(startIndex < size)
        buffer.rewind()
        source.position(startIndex)
        val res = source.read(buffer)
        segmentSize.set(startIndex)
        return res
    }

    companion object {

        private fun split(left: ByteArray, leftSize: Int, right: ByteArray, delimiter: ByteArray): List<List<Byte>> {
            left + right
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