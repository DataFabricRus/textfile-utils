package cc.datafabric.textfileutils.files

import cc.datafabric.textfileutils.iterators.ResourceIterator
import cc.datafabric.textfileutils.iterators.asResourceIterator
import cc.datafabric.textfileutils.iterators.emptyResourceIterator
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
 * Reads text as [ResourceIterator]<[String]> lines from the given area of file.
 * The area is set by the specified [startAreaPositionInclusive] and [endAreaPositionExclusive].
 * Since this is IO operation, the [DirectByteBuffer][ByteBuffer.allocateDirect] is preferred.
 *
 * @param [startAreaPositionInclusive][Long] the index to read from, non-negative number of bytes from the beginning of area
 * @param [endAreaPositionExclusive][Long] the bytes' index to read to,
 * non-negative number of bytes from the beginning of area
 * @param [direct][Boolean] if `true` the reading is performed from the beginning to the end of area,
 * otherwise the reading is reversed (from the end to start)
 * @param [buffer][ByteBuffer] to use while reading data from file; default `8192`; for IO `DirectByteBuffer` is most appropriate
 * @param [delimiter] lines-separator; default `\n`
 * @param [listener] callback to monitor the process that accepts current position (index); no listener by default
 * @param [coroutineName] the name of coroutine which processes physical (NIO) reading, to be used for async reader
 * @param [coroutineContext][CoroutineContext] to run async reader, default = [Dispatchers.IO]
 * @param [maxLineLengthInBytes][Int] line restriction,
 * to avoid memory lack when there is no delimiter, default = `8192`
 * @param [singleOperationTimeoutInMs][Long] to prevent hangs
 * @param [internalQueueSize][Int] to hold lines before emitting
 * @return [ResourceIterator]<[String]> of lines starting from the end of segment to the beginning
 * @throws [IllegalStateException] if line exceeds [maxLineLengthInBytes]
 */
fun SeekableByteChannel.readLines(
    startAreaPositionInclusive: Long = 0,
    endAreaPositionExclusive: Long = size(),
    delimiter: String = "\n",
    listener: (Long) -> Unit = {},
    direct: Boolean = true,
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
    maxLineLengthInBytes: Int = MAX_LINE_LENGTH_IN_BYTES,
    charset: Charset = Charsets.UTF_8,
    singleOperationTimeoutInMs: Long = LINE_READER_SINGLE_OPERATION_TIMEOUT_IN_MS,
    internalQueueSize: Int = LINE_READER_INTERNAL_QUEUE_SIZE,
    coroutineName: String = "AsyncLineReader",
    coroutineContext: CoroutineContext = Dispatchers.IO,
): ResourceIterator<String> = readLines(
    startAreaPositionInclusive = startAreaPositionInclusive,
    endAreaPositionExclusive = endAreaPositionExclusive,
    delimiter = delimiter.bytes(charset),
    bomSymbolsLength = charset.bomSymbols().size,
    listener = listener,
    direct = direct,
    buffer = buffer,
    maxLineLengthInBytes = maxLineLengthInBytes,
    singleOperationTimeoutInMs = singleOperationTimeoutInMs,
    internalQueueSize = internalQueueSize,
    coroutineName = coroutineName,
    coroutineContext = coroutineContext,
).map { it.toString(charset) }

/**
 * Reads text as [ResourceIterator]<[ByteArray]> lines from the given area of file.
 * The area is set by the specified [startAreaPositionInclusive] and [endAreaPositionExclusive].
 * Since this is IO operation, the [DirectByteBuffer][ByteBuffer.allocateDirect] is preferred.
 *
 * @param [startAreaPositionInclusive][Long] the index to read from,
 * non-negative number of bytes from the beginning of area
 * @param [endAreaPositionExclusive][Long] the bytes' index to read to,
 * non-negative number of bytes from the beginning of area
 * @param [delimiter] e.g. for UTF-16 `" " = [0, 32]`
 * @param [bomSymbolsLength] e.g. for UTF-16 `[-2, -1]`
 * @param [listener] callback to monitor the process that accepts current position (index); no listener by default
 * @param [direct][Boolean] if `true` the reading is performed from the beginning to the end of area,
 * otherwise the reading is reversed (from the end to start)
 * @param [buffer][ByteBuffer] to use while reading data from file; default `8192`;
 * for IO `DirectByteBuffer` is most appropriate
 * @param [maxLineLengthInBytes][Int] line restriction,
 * to avoid memory lack when there is no delimiter, default = `8192`
 * @param [singleOperationTimeoutInMs][Long] to prevent hangs
 * @param [internalQueueSize][Int] to hold lines before emitting
 * @param [coroutineName] the name of coroutine which processes physical (NIO) reading, to be used for async reader
 * @param [coroutineContext][CoroutineContext] to run async reader, default = [Dispatchers.IO]
 * @return [ResourceIterator]<[String]> of lines starting from the end of segment to the beginning
 * @throws [IllegalStateException] if line exceeds [maxLineLengthInBytes]
 */
fun SeekableByteChannel.readLines(
    startAreaPositionInclusive: Long = 0,
    endAreaPositionExclusive: Long = size(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    bomSymbolsLength: Int,
    listener: (Long) -> Unit = {},
    direct: Boolean = true,
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
    maxLineLengthInBytes: Int = MAX_LINE_LENGTH_IN_BYTES,
    singleOperationTimeoutInMs: Long = LINE_READER_SINGLE_OPERATION_TIMEOUT_IN_MS,
    internalQueueSize: Int = LINE_READER_INTERNAL_QUEUE_SIZE,
    coroutineName: String = "AsyncLineReader",
    coroutineContext: CoroutineContext = Dispatchers.IO,
): ResourceIterator<ByteArray> {
    require(buffer.capacity() > 0)
    require(delimiter.isNotEmpty())
    require(startAreaPositionInclusive in 0..endAreaPositionExclusive)
    require(endAreaPositionExclusive <= size())
    val startPosition = startAreaPositionInclusive + bomSymbolsLength
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
            return emptyResourceIterator()
        }
        val lines = bytes.split(delimiter).mapIndexed { index, ba ->
            check(ba.size <= maxLineLengthInBytes && ba.size <= maxLineLengthInBytes) {
                "The line #${index + 1} is too long (max length = $maxLineLengthInBytes, actual length = ${ba.size}"
            }
            ba
        }.toList()
        return if (direct) {
            lines.asResourceIterator()
        } else {
            iterator {
                lines.toList().indices.reversed().forEach {
                    yield(lines[it])
                }
            }.asResourceIterator()
        }
    }
    return asyncReadByteLines(
        startAreaPositionInclusive = startAreaPositionInclusive + bomSymbolsLength,
        endAreaPositionExclusive = endAreaPositionExclusive,
        delimiter = delimiter,
        listener = listener,
        direct = direct,
        buffer = buffer,
        maxLineLengthInBytes = maxLineLengthInBytes,
        singleOperationTimeoutInMs = singleOperationTimeoutInMs,
        internalQueueSize = internalQueueSize,
        coroutineName = coroutineName,
        coroutineContext = coroutineContext,
    )
}

/**
 * Reads text [ByteArray]-lines from the given area of file.
 * The area is set by the specified [startAreaPositionInclusive] and [endAreaPositionExclusive].
 * Since this is IO operation, the [DirectByteBuffer][ByteBuffer.allocateDirect] is preferred.
 * This method works **asynchronously**, which means that the read does not block current context:
 * each read operation is performed in a dedicated coroutine context, emitting next [ByteArray]-item on demand.
 * For control read process internal blocking queue is used,
 * when it is full IO-read stops until the method [Iterator.next] is called.
 * This allows using different reading processes in parallel.
 * **Note:** [ResourceIterator] must be closed on finish to release internal resources and stop reading,
 * but this closing won't close this [SeekableByteChannel] channel.
 *
 * @param [startAreaPositionInclusive][Long] the index to read from, non-negative number of bytes from the beginning of area
 * @param [endAreaPositionExclusive][Long] the bytes' index to read to,
 * non-negative number of bytes from the beginning of area
 * @param [direct][Boolean] if `true` the reading is performed from the beginning to the end of area,
 * otherwise the reading is reversed (from the end to start)
 * @param [buffer][ByteBuffer] to use while reading data from file; default `8192`; for IO `DirectByteBuffer` is most appropriate
 * @param [delimiter] lines-separator; default `\n`
 * @param [listener] callback to monitor the process that accepts current position (index); no listener by default
 * @param [coroutineName] the name of coroutine which processes physical (NIO) reading, to be used for async reader
 * @param [coroutineContext][CoroutineContext] to run async reader, default = [Dispatchers.IO]
 * @param [maxLineLengthInBytes][Int] line restriction, to avoid memory lack e.g., when there is no delimiter, default = `8192`
 * @param [singleOperationTimeoutInMs][Long] to prevent hangs
 * @param [internalQueueSize][Int] to hold lines before emitting
 * @return [ResourceIterator]<[ByteArray]> of lines starting from the end of segment to the beginning
 * @throws [IllegalStateException] if line exceeds [maxLineLengthInBytes]
 */
fun SeekableByteChannel.asyncReadByteLines(
    startAreaPositionInclusive: Long = 0,
    endAreaPositionExclusive: Long = size(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    listener: (Long) -> Unit = {},
    direct: Boolean = true,
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
    maxLineLengthInBytes: Int = MAX_LINE_LENGTH_IN_BYTES,
    singleOperationTimeoutInMs: Long = LINE_READER_SINGLE_OPERATION_TIMEOUT_IN_MS,
    internalQueueSize: Int = LINE_READER_INTERNAL_QUEUE_SIZE,
    coroutineName: String = "AsyncLineReader",
    coroutineContext: CoroutineContext = Dispatchers.IO,
): ResourceIterator<ByteArray> {
    val reader = AsyncLineReader(
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
 * Reads text [ByteArray]-lines from the given area of file.
 * The area is set by the specified [startAreaPositionInclusive] and [endAreaPositionExclusive].
 * Since this is IO operation, the [DirectByteBuffer][ByteBuffer.allocateDirect] is preferred.
 * **Note:** after [ResourceIterator.close] no further reading is possible.
 *
 * @param [startAreaPositionInclusive][Long] the index to read from, non-negative number of bytes from the beginning of area
 * @param [endAreaPositionExclusive][Long] the bytes' index to read to, non-negative number of bytes from the beginning of area
 * @param [direct][Boolean] if `true` the reading is performed from the beginning to the end of area,
 * otherwise the reading is reversed (from the end to start)
 * @param [buffer][ByteBuffer] to use while reading data from file; default `8192`; for IO `DirectByteBuffer` is most appropriate
 * @param [delimiter] lines-separator; default `\n`
 * @param [listener] callback to monitor the process that accepts current position (index); no listener by default
 * @param [maxLineLengthInBytes][Int] line restriction, to avoid memory lack e.g., when there is no delimiter, default = `8192`
 * @return [ResourceIterator]<[ByteArray]> of lines starting from the end of segment to the beginning
 * @throws [IllegalStateException] if line exceeds [maxLineLengthInBytes]
 */
fun SeekableByteChannel.syncReadByteLines(
    startAreaPositionInclusive: Long = 0,
    endAreaPositionExclusive: Long = size(),
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    listener: (Long) -> Unit = {},
    direct: Boolean = true,
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
    maxLineLengthInBytes: Int = MAX_LINE_LENGTH_IN_BYTES,
): ResourceIterator<ByteArray> = SyncLineReader(
    source = this,
    direct = direct,
    startAreaPositionInclusive = startAreaPositionInclusive,
    endAreaPositionExclusive = endAreaPositionExclusive,
    listener = listener,
    buffer = buffer,
    delimiter = delimiter,
    maxLineLength = maxLineLengthInBytes,
).lines()

/**
 * Reads file from the [endAreaPositionExclusive] position to the [startAreaPositionInclusive] synchronously.
 * The reader is [AutoCloseable], no further processing is possible after closing.
 * Note that [source][SeekableByteChannel] will not be closed.
 */
class SyncLineReader(
    source: SeekableByteChannel,
    listener: (Long) -> Unit,
    buffer: ByteBuffer,
    delimiter: ByteArray,
    maxLineLength: Int,
    direct: Boolean,
    startAreaPositionInclusive: Long,
    endAreaPositionExclusive: Long,
) : LineReader(
    source = source,
    direct = direct,
    startAreaPositionInclusive = startAreaPositionInclusive,
    endAreaPositionExclusive = endAreaPositionExclusive,
    listener = listener,
    buffer = buffer,
    delimiter = delimiter,
    maxLineLength = maxLineLength
) {
    override fun lines(): ResourceIterator<ByteArray> = iterator {
        if (direct) directRead {
            yield(it)
        } else reverseRead {
            yield(it)
        }
    }.asResourceIterator {
        close()
    }
}

/**
 * Reads file from the [endAreaPositionExclusive] position to the [startAreaPositionInclusive] asynchronously.
 * The reader is [AutoCloseable] to release resources, no further processing is possible after closing.
 * Note that [source][SeekableByteChannel] will not be closed.
 */
class AsyncLineReader internal constructor(
    source: SeekableByteChannel,
    listener: (Long) -> Unit,
    buffer: ByteBuffer,
    delimiter: ByteArray,
    maxLineLength: Int,
    queueSize: Int,
    direct: Boolean,
    startAreaPositionInclusive: Long,
    endAreaPositionExclusive: Long,
    private val itemTimeoutInMs: Long,
) : LineReader(
    source = source,
    direct = direct,
    startAreaPositionInclusive = startAreaPositionInclusive,
    endAreaPositionExclusive = endAreaPositionExclusive,
    listener = listener,
    buffer = buffer,
    delimiter = delimiter,
    maxLineLength = maxLineLength
) {

    private val error: AtomicReference<Throwable> = AtomicReference()
    private val queue: BlockingQueue<ByteArray> = ArrayBlockingQueue(queueSize)

    override fun close() {
        end.set(true)
        queue.clear()
    }

    override fun lines(): ResourceIterator<ByteArray> = iterator {
        while (!end.get() || queue.isNotEmpty()) {
            error.get()?.let { throw it }
            queue.poll()?.let {
                yield(it)
            }
        }
        error.get()?.let { throw it }
    }.asResourceIterator {
        close()
    }

    fun run() {
        try {
            if (direct) directRead {
                check(queue.offer(it, itemTimeoutInMs, TimeUnit.MILLISECONDS))
            } else reverseRead {
                check(queue.offer(it, itemTimeoutInMs, TimeUnit.MILLISECONDS))
            }
        } catch (ex: Throwable) {
            error.set(ex)
            throw ex
        } finally {
            end.set(true)
        }
    }
}

abstract class LineReader internal constructor(
    private val source: SeekableByteChannel,
    protected val direct: Boolean,
    protected val startAreaPositionInclusive: Long,
    protected val endAreaPositionExclusive: Long,
    protected val listener: (Long) -> Unit,
    protected val buffer: ByteBuffer,
    private val delimiter: ByteArray,
    private val maxLineLength: Int,
) : AutoCloseable {

    protected val end: AtomicBoolean = AtomicBoolean(false)

    override fun close() {
        end.set(true)
    }

    abstract fun lines(): ResourceIterator<ByteArray>

    protected inline fun directRead(onEach: (ByteArray) -> Unit) {
        var startIndex = startAreaPositionInclusive
        var remainder = ByteArray(0)
        while (startIndex < endAreaPositionExclusive && !end.get()) {
            val endIndex = min(endAreaPositionExclusive - 1, startIndex + buffer.capacity() - 1)
            val readBytes = read(startIndex, endIndex)
            listener(endIndex)
            val linesToRemainder = buffer.directLines(length = readBytes, remainder = ByteBuffer.wrap(remainder))
            remainder = linesToRemainder.second
            val readLines = linesToRemainder.first
            readLines.forEach {
                onEach(it)
            }
            if (endIndex == endAreaPositionExclusive - 1) {
                onEach(remainder)
            }
            startIndex = endIndex + 1
        }
    }

    protected inline fun reverseRead(onEach: (ByteArray) -> Unit) {
        listener(endAreaPositionExclusive)
        var endIndex = endAreaPositionExclusive - 1
        var remainder = ByteArray(0)
        while (endIndex >= startAreaPositionInclusive && !end.get()) {
            val startIndex = max(startAreaPositionInclusive, endIndex + 1 - buffer.capacity())
            val readBytes = read(startIndex, endIndex)
            listener(startIndex)
            buffer.rewind()
            val linesToRemainder = buffer.reverseLines(length = readBytes, remainder = ByteBuffer.wrap(remainder))
            remainder = linesToRemainder.second
            val readLines = linesToRemainder.first
            readLines.indices.reversed().forEach {
                onEach(readLines[it])
            }
            if (startIndex == startAreaPositionInclusive) {
                onEach(remainder)
            }
            endIndex = startIndex - 1
        }
    }

    protected fun read(startIndex: Long, endIndex: Long): Int {
        val readBytes = (endIndex - startIndex + 1).toInt()
        buffer.rewind()
        buffer.limit(readBytes)
        source.position(startIndex)
        source.read(buffer)
        return readBytes
    }

    protected fun ByteBuffer.directLines(
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

    protected fun ByteBuffer.reverseLines(
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
}