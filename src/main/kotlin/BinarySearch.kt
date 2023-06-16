package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel


internal fun SeekableByteChannel.readLeftLines(
    readPosition: Long, // start (inclusive) position of previous found line on the left
    buffer: ByteBuffer,
    searchLine: ByteArray,
    delimiter: ByteArray,
    comparator: Comparator<ByteArray>,
    maxOfLines: Int,
    maxLineLengthInBytes: Int = MAX_LINE_LENGTH_IN_BYTES,
    res: MutableList<Pair<ByteArray, Long>>,
) {
    var lineEndInclusive = readPosition - delimiter.size
    this.readLinesAsByteArrays(
        startAreaPositionInclusive = 0,
        endAreaPositionExclusive = lineEndInclusive,
        delimiter = delimiter,
        maxLineLengthInBytes = maxLineLengthInBytes,
        buffer = buffer,
        direct = false,
    ).forEach {
        if (comparator.compare(it, searchLine) != 0) {
            return
        }
        require(res.size <= maxOfLines) {
            "max-number-of-lines=${maxOfLines} is exceeded"
        }
        lineEndInclusive -= it.size
        res.add(0, it to lineEndInclusive)
        lineEndInclusive -= delimiter.size
    }
}

internal fun SeekableByteChannel.readRightLines(
    readPosition: Long, // end (exclusive) position of previous found line on the right
    buffer: ByteBuffer,
    searchLine: ByteArray,
    delimiter: ByteArray,
    comparator: Comparator<ByteArray>,
    maxOfLines: Int,
    maxLineLengthInBytes: Int = MAX_LINE_LENGTH_IN_BYTES,
    res: MutableList<Pair<ByteArray, Long>>,
) {
    var lineStartInclusive = readPosition + delimiter.size
    this.readLinesAsByteArrays(
        startAreaPositionInclusive = lineStartInclusive,
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
        res.add(it to lineStartInclusive)
        lineStartInclusive += it.size + delimiter.size
    }
}
