package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset


private fun SeekableByteChannel.expandLines(
    foundLines: List<ByteArray>,
    startLinesPositionInclusive: Long,
    endLinesPositionInclusive: Long,
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
        res = lines
    )
    lines.addAll(foundLines)
    readRightLines(
        readPosition = endLinesPositionInclusive,
        buffer = buffer,
        searchLine = searchLine,
        delimiter = delimiter,
        comparator = byteArrayComparator,
        maxOfLines = maxOfLines,
        maxLineLengthInBytes = maxLineLengthInBytes,
        res = lines
    )
    return startIndex to lines
}

private fun Comparator<String>.toByteArrayLinearSearchComparator(
    searchLine: ByteArray,
    charset: Charset
): Comparator<ByteArray> {
    val searchLineString = searchLine.toString(charset)
    val asString: ByteArray.() -> String = {
        if (this.size == searchLine.size && this.contentEquals(searchLine)) {
            searchLineString
        } else {
            toString(charset)
        }
    }
    return toByteArrayComparator(asString) { this }
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
    this.readLinesAsByteArrays( // TODO: need sync line-reader
        startAreaPositionInclusive = 0,
        endAreaPositionExclusive = blockStartInclusive - delimiter.size,
        delimiter = delimiter,
        maxLineLengthInBytes = maxLineLengthInBytes,
        buffer = buffer,
        direct = false,
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
    this.readLinesAsByteArrays( // TODO: need sync line-reader
        startAreaPositionInclusive = readPosition + delimiter.size,
        endAreaPositionExclusive = size(),
        delimiter = delimiter,
        maxLineLengthInBytes = maxLineLengthInBytes,
        buffer = buffer,
        direct = true,
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
