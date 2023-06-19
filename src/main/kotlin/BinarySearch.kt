package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import kotlin.math.max
import kotlin.math.min

/**
 * Searches the source channel for strings that match the specified [search-string][searchLine]
 * using the binary search algorithm.
 * The data in the channel must be sorted, otherwise the result is unpredictable.
 * @param searchLine [ByteArray] pattern
 * @param startAreaInclusive [Long] the starting position in the file, default `0`
 * @param endAreaExclusive [Long] the end position in the file, default [SeekableByteChannel.size]
 * @param delimiter [ByteArray] default `\n`
 * @param buffer [ByteBuffer] to use while reading data from file; default `16384`; for IO `DirectByteBuffer` is most appropriate;
 * note that due to implementation restriction [buffer] size must be greater or equal than [maxLineLengthInBytes]
 * @param charset [Charset] default `UTF-8`
 * @param comparator [Comparator]<[String]> to compare lines
 * @param maxLineLengthInBytes [Int] line restriction, to avoid memory lack e.g. when there is no delimiter , default = `8192`
 * @param maxOfLinesPerBlock [Int] maximum number of lines in a paragraph
 * @return
 */
fun SeekableByteChannel.binarySearch(
    searchLine: ByteArray,
    startAreaInclusive: Long = 0,
    endAreaExclusive: Long = size(),
    buffer: ByteBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE_IN_BYTES * 2),
    charset: Charset = Charsets.UTF_8,
    delimiter: ByteArray = "\n".toByteArray(charset),
    comparator: Comparator<String> = defaultComparator(),
    maxOfLinesPerBlock: Int = 1024,
    maxLineLengthInBytes: Int = MAX_LINE_LENGTH_IN_BYTES,
): Pair<Long, List<ByteArray>> {
    require(buffer.capacity() >= maxLineLengthInBytes * 2) {
        "buffer${buffer.capacity()} must be greater than 2 * max-line-length=$maxLineLengthInBytes"
    }
    var foundLines: Lines? = null
    var absoluteLowInclusive = startAreaInclusive
    var absoluteHighExclusive = endAreaExclusive
    while (Lines.NULL != foundLines) {
        val searchArea = absoluteBounds(absoluteLowInclusive, absoluteHighExclusive, buffer)
        position(searchArea.first)
        buffer.position(0)
        read(buffer)
        buffer.position(0)

        foundLines = byteArrayBinarySearch(
            source = buffer,
            searchLine = searchLine,
            sourceStartInclusive = 0,
            sourceEndExclusive = buffer.capacity(),
            delimiter = delimiter,
            comparator = comparator.toByteArrayComparator(charset = charset, hashMapOf()),
            includeRightBound = searchArea.first == startAreaInclusive,
            includeLeftBound = searchArea.second == endAreaExclusive,
        )
        if (foundLines.endExclusive == -1 && foundLines.startInclusive == -1) { // Lines.NULL
            // cannot find
            return -1L to emptyList()
        }
        if (foundLines.endExclusive == -1) { // right
            absoluteLowInclusive = searchArea.first + foundLines.startInclusive
            continue
        }
        if (foundLines.startInclusive == -1) { // left
            absoluteHighExclusive = searchArea.first + foundLines.endExclusive
            continue
        }
        if (foundLines.endExclusive == foundLines.startInclusive) {
            // cannot be found but can be inserted
            return searchArea.first + foundLines.startInclusive to emptyList()
        }
        // found
        return this.expandLines(
            foundLines = foundLines.lines,
            startLinesPositionInclusive = searchArea.first + foundLines.startInclusive,
            endLinesPositionExclusive = searchArea.first + foundLines.endExclusive,
            buffer = buffer,
            searchLine = searchLine,
            delimiter = delimiter,
            charset = charset,
            comparator = comparator,
            maxOfLines = maxOfLinesPerBlock,
            maxLineLengthInBytes = maxLineLengthInBytes
        )
    }
    throw IllegalStateException("must be unreachable")
}

private fun absoluteBounds(
    startAreaInclusive: Long,
    endAreaExclusive: Long,
    buffer: ByteBuffer,
): Pair<Long, Long> {
    val middle = startAreaInclusive + endAreaExclusive ushr 1
    // size = buffer * 2
    val low = max(startAreaInclusive, (2 * middle - buffer.capacity()) / 2)
    val high = min(endAreaExclusive, (2 * middle + buffer.capacity()) / 2)
    check(low <= high)
    if (low == high) {
        return low to low + 1
    }
    return low to high
}

private fun SeekableByteChannel.expandLines(
    foundLines: List<ByteArray>,
    startLinesPositionInclusive: Long,
    endLinesPositionExclusive: Long,
    buffer: ByteBuffer,
    searchLine: ByteArray,
    delimiter: ByteArray,
    charset: Charset,
    comparator: Comparator<String>,
    maxOfLines: Int,
    maxLineLengthInBytes: Int,
): Pair<Long, List<ByteArray>> {
    val byteArrayComparator = comparator.toByteArrayLinearSearchComparator(searchLine, charset)
    val lines = mutableListOf<ByteArray>()
    val startIndex = readLeftLines(
        readPosition = startLinesPositionInclusive,
        buffer = buffer,
        searchLine = searchLine,
        delimiter = delimiter,
        comparator = byteArrayComparator,
        maxOfLines = maxOfLines,
        maxLineLengthInBytes = maxLineLengthInBytes,
        res = lines,
    )
    lines.addAll(foundLines)
    readRightLines(
        readPosition = endLinesPositionExclusive,
        buffer = buffer,
        searchLine = searchLine,
        delimiter = delimiter,
        comparator = byteArrayComparator,
        maxOfLines = maxOfLines,
        maxLineLengthInBytes = maxLineLengthInBytes,
        res = lines,
    )
    return startIndex to lines
}

internal fun SeekableByteChannel.readLeftLines(
    readPosition: Long, // start (inclusive) position of previous found line on the left
    buffer: ByteBuffer,
    searchLine: ByteArray,
    delimiter: ByteArray,
    comparator: Comparator<ByteArray>,
    maxOfLines: Int,
    maxLineLengthInBytes: Int,
    res: MutableList<ByteArray>,
): Long {
    var blockStartInclusive = readPosition
    this.readLinesAsByteArrays(
        startAreaPositionInclusive = 0,
        endAreaPositionExclusive = blockStartInclusive - delimiter.size,
        delimiter = delimiter,
        maxLineLengthInBytes = maxLineLengthInBytes,
        buffer = buffer,
        direct = false,
        internalQueueSize = 1,
    ).forEach {
        if (comparator.compare(it, searchLine) != 0) {
            return blockStartInclusive
        }
        require(res.size <= maxOfLines) {
            "max-number-of-lines=${maxOfLines} is exceeded"
        }
        blockStartInclusive -= (it.size + delimiter.size)
        res.add(0, it)
    }
    return blockStartInclusive
}

internal fun SeekableByteChannel.readRightLines(
    readPosition: Long, // end (exclusive) position of previous found line on the right
    buffer: ByteBuffer,
    searchLine: ByteArray,
    delimiter: ByteArray,
    comparator: Comparator<ByteArray>,
    maxOfLines: Int,
    maxLineLengthInBytes: Int,
    res: MutableList<ByteArray>,
) {
    this.readLinesAsByteArrays(
        startAreaPositionInclusive = readPosition + delimiter.size,
        endAreaPositionExclusive = size(),
        delimiter = delimiter,
        maxLineLengthInBytes = maxLineLengthInBytes,
        buffer = buffer,
        direct = true,
        internalQueueSize = 1,
    ).forEach {
        if (comparator.compare(it, searchLine) != 0) {
            return
        }
        require(res.size <= maxOfLines) {
            "max-number-of-lines=${maxOfLines} is exceeded"
        }
        res.add(it)
    }
}
