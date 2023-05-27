package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * Inserts the given [data] at the beginning of channel.
 * TODO: will be changed.
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