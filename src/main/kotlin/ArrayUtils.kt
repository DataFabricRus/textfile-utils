package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.charset.Charset
import kotlin.math.max
import kotlin.math.min


/**
 * Encodes the contents of this string using the specified character set and returns the resulting byte array,
 * which does not contain leading BOM bytes.
 */
fun String.bytes(charset: Charset): ByteArray {
    val bom = charset.bomSymbols().size
    return if (bom == 0) {
        toByteArray(charset)
    } else {
        toByteArray(charset).drop(bom).toByteArray()
    }
}

internal fun findLineBlock(
    line: Pair<ByteArray, Int>,
    source: ByteBuffer,
    sourceStartInclusive: Int,
    sourceEndExclusive: Int,
    delimiter: ByteArray,
    includeLeftBound: Boolean = true,
    includeRightBound: Boolean = true,
): Lines {
    require(sourceStartInclusive in 0 until sourceEndExclusive)
    require(line.second in sourceStartInclusive until sourceEndExclusive) {
        "line-index=${line.second}; source-start=$sourceStartInclusive; source-end=$sourceEndExclusive"
    }
    val step = line.first.size + delimiter.size
    val rangeEndExclusive = min(line.second + delimiter.size + max(line.first.size, 1), sourceEndExclusive)
    val rangeStartInclusive = max(line.second - delimiter.size, sourceStartInclusive)

    val res = mutableListOf<Pair<ByteArray, Int>>()

    // left
    var leftRangeEnd = rangeEndExclusive
    var leftRangeStart = rangeStartInclusive
    var left: Pair<ByteArray, Int>
    var prevLeft: Pair<ByteArray, Int>? = null
    while (leftRangeStart >= sourceStartInclusive) {
        val position = leftRangeStart + leftRangeEnd ushr 1
        left = findLineNearPosition(
            source = source,
            sourceStartInclusive = leftRangeStart,
            sourceEndExclusive = leftRangeEnd,
            position = position,
            delimiter = delimiter,
            includeLeftBound = includeLeftBound,
            includeRightBound = includeRightBound,
        ) ?: break
        if (prevLeft != null && prevLeft.second - prevLeft.first.size - delimiter.size != left.second) {
            break
        }
        if (!line.first.contentEquals(left.first)) {
            break
        }
        prevLeft = left
        res.add(0, left)
        leftRangeEnd -= step
        leftRangeStart -= step
    }
    if (res.isEmpty()) {
        // should contain the given line at least, if not -> wrong argument, nothing can be found
        return Lines.NULL
    }
    // right
    var rightRangeEnd = rangeEndExclusive + step
    var rightRangeStart = rangeStartInclusive + step
    var right: Pair<ByteArray, Int>
    var prevRight: Pair<ByteArray, Int> = line
    while (rightRangeEnd <= sourceEndExclusive) {
        val position = rightRangeStart + rightRangeEnd ushr 1
        right = findLineNearPosition(
            source = source,
            sourceStartInclusive = rightRangeStart,
            sourceEndExclusive = rightRangeEnd,
            position = position,
            delimiter = delimiter,
            includeLeftBound = includeLeftBound,
            includeRightBound = includeRightBound,
        ) ?: break
        if (right.second != prevRight.second + prevRight.first.size + delimiter.size) {
            break
        }
        if (!line.first.contentEquals(right.first)) {
            break
        }
        prevRight = right
        res.add(right)
        rightRangeStart += step
        rightRangeEnd += step
    }
    val n = res.first().second.toLong()
    val m = res.last().second + max( line.first.size, 1).toLong()
    return Lines(n, m, res.map { it.first })
}

/**
 * Finds the first line near the specified [position].
 *
 * If [includeLeftBound] or/and [includeRightBound] are `true`
 * the respective bounds of search area are accepted as a line bounds.
 * If [includeLeftBound] or/and [includeRightBound] are `false`,
 * only [delimiter] determines the line boundaries (left, right or both depending on parameters)
 *
 * @return a [Pair] of found `Line` and its start position in the [source] (inclusive)
 */
@Suppress("UnnecessaryVariable")
fun findLineNearPosition(
    source: ByteBuffer,
    sourceStartInclusive: Int,
    sourceEndExclusive: Int,
    position: Int,
    delimiter: ByteArray,
    includeLeftBound: Boolean = true,
    includeRightBound: Boolean = true,
): Pair<ByteArray, Int>? {
    require(sourceStartInclusive < source.limit())
    require(sourceEndExclusive <= source.limit())
    require(sourceStartInclusive in 0 until sourceEndExclusive)
    require(position in sourceStartInclusive until sourceEndExclusive)
    require(delimiter.isNotEmpty())

    if (sourceStartInclusive == sourceEndExclusive - 1) {
        if (isDelimiter(source, sourceStartInclusive, sourceEndExclusive, delimiter)) {
            return null
        }
        if (includeLeftBound && includeRightBound) {
            return byteArrayOf(source[sourceStartInclusive]) to sourceStartInclusive
        }
        return null
    }
    if (sourceEndExclusive - sourceStartInclusive < delimiter.size) {
        if (includeLeftBound && includeRightBound) {
            return source.toByteArray(sourceStartInclusive, sourceEndExclusive) to sourceStartInclusive
        }
        return null
    }

    val firstBackwardIndex: Int
    val firstForwardIndex: Int
    if (position > sourceEndExclusive - delimiter.size) {
        // no need forward index
        firstForwardIndex = sourceEndExclusive
        firstBackwardIndex = position
    } else if (position < sourceStartInclusive + delimiter.size - 1) {
        // no need backward index
        firstForwardIndex = position
        firstBackwardIndex = sourceStartInclusive - 1
    } else {
        firstForwardIndex = position
        firstBackwardIndex = position + delimiter.size - 2
    }

    val minBackwardIndex = sourceStartInclusive + delimiter.size - 1
    val maxForwardIndex = sourceEndExclusive - delimiter.size

    val iterNumber = max(firstBackwardIndex - minBackwardIndex + 1, maxForwardIndex - firstForwardIndex + 1)

    var backwardIndex = firstBackwardIndex // from the position to the start of range
    var forwardIndex = firstForwardIndex // from the position to the end of range

    val forwardDelimiters = sortedSetOf<Int>() // left delimiters
    val backwardDelimiters = sortedSetOf<Int>() // right delimiters
    var it = 0
    while (it++ < iterNumber) {
        if (backwardIndex >= minBackwardIndex) {
            val delimiterSourceStartIndexInclusive = backwardIndex - delimiter.size + 1
            val delimiterSourceEndIndexExclusive = backwardIndex + 1
            if (isDelimiter(
                    source = source,
                    sourceStartInclusive = delimiterSourceStartIndexInclusive,
                    sourceEndExclusive = delimiterSourceEndIndexExclusive,
                    delimiter = delimiter,
                )
            ) {
                backwardDelimiters.add(delimiterSourceStartIndexInclusive)
            }
        }
        if (forwardIndex <= maxForwardIndex) {
            val delimiterSourceStartIndexInclusive = forwardIndex
            val delimiterSourceEndIndexExclusive = forwardIndex + delimiter.size
            if (isDelimiter(
                    source = source,
                    sourceStartInclusive = delimiterSourceStartIndexInclusive,
                    sourceEndExclusive = delimiterSourceEndIndexExclusive,
                    delimiter = delimiter,
                )
            ) {
                forwardDelimiters.add(forwardIndex)
            }
        }
        if (backwardIndex <= minBackwardIndex) {
            val startsWithDelimiter =
                backwardDelimiters.firstOrNull() == 0 || forwardDelimiters.firstOrNull() == 0
            if (includeLeftBound && !startsWithDelimiter) {
                // add "virtual" delimiter before range
                backwardDelimiters.add(sourceStartInclusive - delimiter.size)
            }
        }
        if (forwardIndex >= maxForwardIndex) {
            val endsWithDelimiter =
                backwardDelimiters.lastOrNull() == sourceEndExclusive - 1 || forwardDelimiters.lastOrNull() == sourceEndExclusive - 1
            if (includeRightBound && !endsWithDelimiter) {
                // add "virtual" delimiter after range
                forwardDelimiters.add(sourceEndExclusive)
            }
        }
        if (backwardDelimiters.isNotEmpty() && forwardDelimiters.isNotEmpty()) {
            break
        }
        backwardIndex--
        forwardIndex++
    }
    if (backwardDelimiters.size + forwardDelimiters.size < 2) {
        return null
    }
    val leftDelimiters = ArrayList(backwardDelimiters)
    val rightDelimiters = ArrayList(forwardDelimiters)
    val leftDelimiter: Int
    val rightDelimiter: Int
    if (leftDelimiters.isEmpty()) {
        leftDelimiter = rightDelimiters.removeFirst()
        rightDelimiter = rightDelimiters.removeFirst()
    } else if (rightDelimiters.isEmpty()) {
        rightDelimiter = leftDelimiters.removeLast()
        leftDelimiter = leftDelimiters.removeLast()
    } else {
        rightDelimiter = rightDelimiters.removeFirst()
        leftDelimiter = leftDelimiters.removeLast()
    }
    val firstLineIndex = leftDelimiter + delimiter.size
    val lastLineIndex = rightDelimiter
    check(firstLineIndex <= lastLineIndex) { "first-line-index = $firstBackwardIndex, last-line-index = $lastLineIndex" }
    return source.toByteArray(startInclusive = firstLineIndex, endExclusive = lastLineIndex) to firstLineIndex
}

/**
 * Extracts byte array from the buffer.
 */
internal fun ByteBuffer.toByteArray(startInclusive: Int, endExclusive: Int): ByteArray {
    require(startInclusive >= 0)
    require(startInclusive in 0..endExclusive)
    require(limit() >= endExclusive)
    if (startInclusive == endExclusive) {
        return byteArrayOf()
    }
    val prev = position()
    return try {
        position(startInclusive)
        val res = ByteArray(endExclusive - startInclusive)
        get(res)
        res
    } finally {
        position(prev)
    }
}

internal fun split(
    left: ByteBuffer,
    leftSize: Int,
    right: ByteBuffer,
    rightSize: Int,
    delimiter: ByteArray
): List<List<Byte>> {
    val size = leftSize + rightSize
    if (size == 0) {
        return emptyList()
    }
    val res = mutableListOf<MutableList<Byte>>()
    res.add(mutableListOf())
    var i = 0
    while (i < size) {
        if (isDelimiter(left, leftSize, right, rightSize, i, delimiter)) {
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
    source: ByteBuffer,
    sourceStartInclusive: Int,
    sourceEndExclusive: Int,
    delimiter: ByteArray,
): Boolean {
    if (sourceEndExclusive - sourceStartInclusive < delimiter.size) {
        return false
    }
    for (i in sourceStartInclusive until sourceEndExclusive) {
        if (source[i] != delimiter[i - sourceStartInclusive] ) {
            return false
        }
    }
    return true
}

private fun isDelimiter(
    left: ByteBuffer,
    leftSize: Int,
    right: ByteBuffer,
    rightSize: Int,
    index: Int,
    delimiter: ByteArray
): Boolean {
    if (index > leftSize + rightSize - delimiter.size) {
        return false
    }
    for (i in delimiter.indices) {
        if (get(left, leftSize, right, i + index) != delimiter[i]) {
            return false
        }
    }
    return true
}

private fun get(left: ByteBuffer, leftSize: Int, right: ByteBuffer, index: Int): Byte {
    return if (index < leftSize) {
        left[index]
    } else {
        right[index - leftSize]
    }
}

/**
 * - `Lines(-1, -1, emptyList())` - cannot find in the source buffer
 * - `Lines(-1, N, emptyList())` - not found but definitely less than the first left `Line` in the source buffer,
 * where `N` is the left position (in the source) of the left `Line` (inclusive)
 * - `Lines(N, -1, emptyList())` - not found but definitely greater than last right `Line` in the source buffer,
 * where `N` is the right position (in the source) of the right `Line` (exclusive)
 * - `Lines(N, N, emptyList())` - not found but can be inserted at the position `N` (in the source) with shifting source to the right,
 * this is the start position of the next (right) `Line`
 * - `Lines(N, M, listOf(ByteArray ..))` - found, `N` and `M` positions (in the source) of block, inclusive and exclusive
 */
data class Lines(val startInclusive: Long, val endExclusive: Long, val lines: List<ByteArray>) {
    companion object {
        val NULL = Lines(-1, -1, emptyList())
    }

    fun lines(charset: Charset): List<String> =
        if (lines.isEmpty()) emptyList() else lines.map { it.toString(charset) }
}