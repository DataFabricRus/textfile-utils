package cc.datafabric.textfileutils.files

import cc.datafabric.iterators.all
import cc.datafabric.iterators.closeAll
import cc.datafabric.textfileutils.iterators.defaultComparator
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
import kotlin.io.path.fileSize
import kotlin.io.path.inputStream

/**
 * Inserts given [lines] before the specified [position] in assumption
 * that [target] file contains lines separated by [delimiter] and
 * the given [position] is a first byte of some line (or the size of the file to insert at its end).
 * @param target [Path]
 * @param lines [List]<[String]>
 * @param position
 * @param delimiter
 * @param charset [Charset]
 * @param buffer [ByteBuffer]
 */
fun insertLines(
    target: Path,
    lines: List<String>,
    position: Long = 0,
    delimiter: String,
    charset: Charset,
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
) = target.use {
    val block = if (position == it.size()) {
        lines.joinToString(delimiter, delimiter, "")
    } else {
        lines.joinToString(delimiter, "", delimiter)
    }
    val data = block.toByteArray(charset)
    it.insert(data, position, buffer)
}

/**
 * Inserts the given [data] at the [specified position][beforePosition] of the channel.
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
    var index = size()
    while (index > beforePosition) {
        var readPosition = index - buffer.limit()
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
 * @param [controlDiskspace] if `true` source files will be truncated while processing and completely deleted at the end of it;
 * this allows saving diskspace
 * @param [delimiter]
 * @param [charset][Charset]
 */
fun invert(
    source: Path,
    target: Path,
    controlDiskspace: Boolean = true,
    delimiter: String = "\n",
    charset: Charset = Charsets.UTF_8,
) = invert(
    source = source,
    target = target,
    controlDiskspace = controlDiskspace,
    delimiter = delimiter.bytes(charset),
    bomSymbols = charset.bomSymbols()
)

/**
 * Inverts the file content, `a,b,c` -> `c,b,a`
 * @param [source][Path]
 * @param [target][Path]
 * @param [controlDiskspace][Boolean]
 * if `true` source files will be truncated while processing and completely deleted at the end of it;
 * this allows saving diskspace
 * @param [delimiter][ByteArray] e.g. for UTF-16 `" " = [0, 32]`
 * @param [bomSymbols][ByteArray] e.g. for UTF-16 `[-2, -1]`
 */
fun invert(
    source: Path,
    target: Path,
    controlDiskspace: Boolean = true,
    delimiter: ByteArray = "\n".toByteArray(Charsets.UTF_8),
    bomSymbols: ByteArray = byteArrayOf(),
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES),
) {
    val bom = ByteBuffer.wrap(bomSymbols)
    Files.newByteChannel(target, StandardOpenOption.WRITE).use { dst ->
        Files.newByteChannel(source, StandardOpenOption.READ, StandardOpenOption.WRITE).use { src ->
            if (bomSymbols.isNotEmpty()) {
                dst.write(bom)
                bom.rewind()
            }
            var position = src.size()
            src.syncReadByteLines(
                startAreaPositionInclusive = bomSymbols.size.toLong(),
                endAreaPositionExclusive = src.size(),
                delimiter = delimiter,
                direct = false,
                buffer = buffer,
            ).forEach { line ->
                position -= line.size
                dst.write(ByteBuffer.wrap(line))
                if (position != 0L) {
                    position -= delimiter.size
                    dst.write(ByteBuffer.wrap(delimiter))
                }
                if (controlDiskspace) {
                    src.truncate(position)
                }
            }
        }
    }
    if (controlDiskspace) {
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
 * Answers `true` if content of two files is identical.
 */
fun contentEquals(left: Path, right: Path): Boolean {
    if (left.fileSize() != right.fileSize()) {
        return false
    }
    left.inputStream().buffered().use { leftStream ->
        right.inputStream().buffered().use { rightStream ->
            val leftBytes = leftStream.iterator()
            val rightBytes = rightStream.iterator()
            while (leftBytes.hasNext()) {
                if (!rightBytes.hasNext()) {
                    return false
                }
                if (leftBytes.nextByte() != rightBytes.nextByte()) {
                    return false
                }
            }
            if (rightBytes.hasNext()) {
                return false
            }
        }
    }
    return true
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
        channels.values.closeAll(IllegalStateException("exception occurred while closing channels"))
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
 * Determines whether two given paths are equivalent (i.e., specified paths point to a single physical file, possibly non-existent).
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