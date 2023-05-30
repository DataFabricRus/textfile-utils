package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.deleteExisting

/**
 * Inserts the given [data] at the [specified position][beforePosition] of channel.
 *
 * @param [data][ByteArray] to write
 * @param [beforePosition][Long] the position in the source before which the data should be inserted
 * @param [buffer][ByteBuffer] non empty buffer
 */
fun SeekableByteChannel.insert(
    data: ByteArray,
    beforePosition: Long = 0,
    buffer: ByteBuffer = ByteBuffer.allocate(8192),
) {
    require(buffer.array().isNotEmpty())
    if (data.isEmpty()) {
        return
    }
    if (data.size + size() <= buffer.capacity()) {
        buffer.rewind()
        position(beforePosition)
        var readBytes = read(buffer)
        val dst = buffer.array()
        if (readBytes == -1) { // empty file
            readBytes = 0
        }
        System.arraycopy(dst, 0, dst, data.size, readBytes)
        System.arraycopy(data, 0, dst, 0, data.size)
        buffer.rewind()
        buffer.limit(data.size + readBytes)
        position(beforePosition)
        write(buffer)
        return
    }
    var index = size() - 1
    while (index > beforePosition) {
        var readPosition = index - buffer.limit() + 1
        if (readPosition < beforePosition) {
            val newBufferLimit = (buffer.capacity() + readPosition - beforePosition).toInt()
            buffer.limit(newBufferLimit)
            readPosition = beforePosition
        }
        index = readPosition
        buffer.rewind()
        position(readPosition)
        val readBytes = read(buffer)
        check(readBytes == buffer.limit()) {
            "read-bytes = $readBytes, buffer-limit = ${buffer.limit()}, position = ${this.position()}"
        }
        buffer.rewind()
        val writePosition = readPosition + data.size
        position(writePosition)
        val writeBytes = write(buffer)
        check(writeBytes == buffer.limit()) {
            "write-bytes = $writeBytes, buffer-limit = ${buffer.limit()}, position = ${this.position()}"
        }
    }
    val dataBuffer = ByteBuffer.wrap(data)
    position(beforePosition)
    val writeBytes = write(dataBuffer)
    check(writeBytes == data.size) {
        "write-bytes = $writeBytes, data-size = ${data.size}"
    }
}

/**
 * Inverts the file content, `a,b,c` -> `c,b,a`
 * @param [source][Path]
 * @param [target][Path]
 * @param [deleteSourceFiles] if `true` source files will be truncated while process and completely deleted at the end of it;
 * this allows to save diskspace
 * @param [delimiter]
 * @param [charset][Charset]
 */
fun invert(
    source: Path,
    target: Path,
    deleteSourceFiles: Boolean = true,
    delimiter: String = "\n",
    charset: Charset = Charsets.UTF_8,
) {
    Files.newByteChannel(target, StandardOpenOption.WRITE).use { dst ->
        Files.newByteChannel(source, StandardOpenOption.READ, StandardOpenOption.WRITE).use { src ->
            var position = src.size()
            val delimiterBytes = delimiter.toByteArray(charset)
            src.readLinesAsByteArrays(
                startPositionInclusive = 0,
                endPositionExclusive = src.size(),
                delimiter = delimiterBytes,
                direct = false,
            ).forEach { b ->
                position -= b.size
                dst.write(ByteBuffer.wrap(b))
                if (position != 0L) {
                    position -= delimiterBytes.size
                    dst.write(ByteBuffer.wrap(delimiterBytes))
                }
                if (deleteSourceFiles) {
                    src.truncate(position)
                }
            }
        }
    }
    if (deleteSourceFiles) {
        source.deleteExisting()
    }
}

/**
 * Answers `true` if the file sorted lexicographically in accordance with [comparator].
 */
fun isSorted(
    file: Path,
    comparator: Comparator<String> = defaultComparator<String>(),
    delimiter: String = "\n",
    charset: Charset = Charsets.UTF_8,
    buffer: ByteBuffer = ByteBuffer.allocate(8192),
): Boolean = file.use(StandardOpenOption.READ) {
    var prev: String? = null
    it.readLines(
        startPositionInclusive = 0,
        endPositionExclusive = it.size(),
        delimiter = delimiter,
        charset = charset,
        buffer = buffer,
    ).all { line ->
        if (prev == null) {
            prev = line
        }
        if (comparator.compare(prev, line) > 0) {
            false
        } else {
            prev = line
            true
        }
    }
}

/**
 * Opens or creates file, executes the [block], then closes the channel.
 * Please note: any stream must be closed inside [block], otherwise [java.nio.channels.ClosedChannelException] is expected.
 */
fun <X> Path.use(
    vararg options: OpenOption = arrayOf(
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
    ),
    block: (SeekableByteChannel) -> X,
) = channel(*options).use(block)

/**
 * Opens or creates a file, returning a seekable byte channel to access the file.
 * @see Files.newByteChannel
 */
fun Path.channel(
    vararg options: OpenOption = arrayOf(
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
    )
): SeekableByteChannel = Files.newByteChannel(this, *options)