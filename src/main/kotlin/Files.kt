package com.gitlab.sszuev.textfiles

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import kotlin.io.path.deleteExisting
import kotlin.io.path.deleteIfExists

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
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
) {
    require(buffer.capacity() > 0)
    if (data.isEmpty()) {
        return
    }
    if (data.size + size() <= buffer.capacity()) {
        buffer.rewind()
        position(beforePosition)
        var readBytes = read(buffer)
        if (readBytes == -1) { // empty file
            readBytes = 0
        }

        buffer.rewind()
        val tmp = ByteArray(readBytes)
        buffer.get(tmp)
        buffer.position(data.size)
        buffer.put(tmp)
        buffer.rewind()
        buffer.put(data)
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
    val bomSymbols = charset.bomSymbols()
    val delimiterBytes = delimiter.bytes(charset)
    Files.newByteChannel(target, StandardOpenOption.WRITE).use { dst ->
        Files.newByteChannel(source, StandardOpenOption.READ, StandardOpenOption.WRITE).use { src ->
            dst.write(ByteBuffer.wrap(bomSymbols))
            var position = src.size()
            src.syncReadByteLines(
                startAreaPositionInclusive = bomSymbols.size.toLong(),
                endAreaPositionExclusive = src.size(),
                delimiter = delimiterBytes,
                direct = false,
            ).forEach { line ->
                position -= line.size
                dst.write(ByteBuffer.wrap(line))
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
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
): Boolean = file.use(StandardOpenOption.READ) {
    var prev: String? = null
    it.readLines(
        startAreaPositionInclusive = 0,
        endAreaPositionExclusive = it.size(),
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
 * Please note: any IO operation (stream) must be closed (collected) inside the [block],
 * otherwise [java.nio.channels.ClosedChannelException] is expected.
 */
fun <X> Path.use(
    vararg options: OpenOption = arrayOf(
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
    ),
    block: (SeekableByteChannel) -> X,
) = channel(*options).use(block)

/**
 * Opens or creates files, executes the [block], then closes the corresponding channels.
 * Please note: any IO operation (stream) must be closed (collected) inside the [block],
 * otherwise [java.nio.channels.ClosedChannelException] is expected.
 */
fun <X> Collection<Path>.use(
    vararg options: OpenOption = arrayOf(
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
    ),
    block: (Map<Path, SeekableByteChannel>) -> X,
): X {
    val channels = this.associateWith { it.channel(*options) }
    return try {
        block(channels)
    } finally {
        channels.values.closeAll { IllegalStateException(it) }
    }
}

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

/**
 * Determines whether two given paths are equivalent (i.e. specified paths point to a single physical file, possibly non-existent).
 */
fun sameFilePaths(left: Path, right: Path): Boolean = left.normalizeToString() == right.normalizeToString()

/**
 * Transforms this [Path] to the unambiguous form,
 * such that a different path pointing to the same physical file will result in the same form.
 */
fun Path.normalizeToString(): String = if (System.getProperty("os.name").lowercase().startsWith("win")) {
    Paths.get(normalize().toString()).toUri().toString().lowercase()
} else {
    Paths.get(normalize().toString()).toUri().toString()
}

/**
 * Deletes this collection of [paths][Path] returning `true` on success.
 * If some any file does not exist the method returns `false`.
 * If the collection is empty the method returns `true`.
 * @throws IOException
 */
fun Collection<Path>.deleteAll(): Boolean {
    val ex = IOException("Exception while deleting all files")
    var res = true
    toSet().forEach {
        res = try {
            it.deleteIfExists()
        } catch (e: Exception) {
            ex.addSuppressed(e)
            false
        }
    }
    if (ex.suppressed.isNotEmpty()) {
        throw ex
    }
    return res
}

/**
 * Appends String suffix returning new [Path].
 */
internal operator fun Path.plus(suffix: String): Path = parent.resolve(fileName.toString() + suffix)

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