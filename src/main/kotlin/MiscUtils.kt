package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.charset.Charset
import kotlin.math.max


/**
 * [Closes][AutoCloseable.close] all resources from this [Collection].
 */
fun <X : AutoCloseable> Iterable<X>.closeAll(exception: (String) -> Throwable = { Exception(it) }) {
    val ex = exception("Error while closing")
    forEach {
        try {
            it.close()
        } catch (ex: Exception) {
            ex.addSuppressed(ex)
        }
    }
    if (ex.suppressed.isNotEmpty()) {
        throw ex
    }
}

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

/**
 * Returns a BOM symbols as a byte array.
 */
fun Charset.bomSymbols(): ByteArray = if (this == Charsets.UTF_16) byteArrayOf(-2, -1) else byteArrayOf()

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

internal fun isDelimiter(
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

internal fun get(left: ByteBuffer, leftSize: Int, right: ByteBuffer, index: Int): Byte {
    return if (index < leftSize) {
        left[index]
    } else {
        right[index - leftSize]
    }
}

/**
 * Finds first line near specified position.
 * Line should be surrounded by delimiters, otherwise we can't determine its bounds.
 */
internal fun findLineNearPosition(
    source: ByteBuffer,
    sourceStartInclusive: Int,
    sourceEndExclusive: Int,
    position: Int,
    delimiter: ByteArray,
    includeLeftBound: Boolean = true,
    includeRightBound: Boolean = true,
): Pair<ByteArray, Int>? {
    require(sourceStartInclusive in 0 until sourceEndExclusive)
    require(position in sourceStartInclusive until sourceEndExclusive)
    require(delimiter.isNotEmpty())
    if (sourceStartInclusive == sourceEndExclusive - 1) {
        if (includeLeftBound && includeRightBound) {
            return byteArrayOf() to sourceStartInclusive
        }
        return null
    }
    if (sourceEndExclusive - sourceStartInclusive < delimiter.size) {
        if (includeLeftBound && includeRightBound) {
            return source.toByteArray(sourceStartInclusive, sourceEndExclusive) to sourceStartInclusive
        }
        return null
    }

    val firstBackwardIndex = if (position < sourceEndExclusive - 1) position + delimiter.size - 2 else position
    val firstForwardIndex = if (position < sourceEndExclusive - 1) position else position - delimiter.size + 2
    val minBackwardIndex = sourceStartInclusive + delimiter.size - 1
    val maxForwardIndex = sourceEndExclusive - delimiter.size
    val itUpBound = max(firstBackwardIndex - minBackwardIndex + 1, maxForwardIndex - firstForwardIndex + 1)

    var backwardIndex = firstBackwardIndex
    var forwardIndex = firstForwardIndex

    val delimiters = sortedSetOf<Int>()
    repeat((0..itUpBound).count()) {
        if (delimiters.size == 2) {
            return@repeat
        }
        if (backwardIndex >= minBackwardIndex) {
            val searchStartDelimiterIndex = backwardIndex - delimiter.size + 1
            if (includeRightBound && backwardIndex == sourceEndExclusive - 1) {
                delimiters.add(backwardIndex + 1)
                if (delimiters.size == 2) {
                    return@repeat
                }
            }
            if (isDelimiter(source, searchStartDelimiterIndex, delimiter)) {
                if (includeLeftBound && searchStartDelimiterIndex == sourceStartInclusive) {
                    delimiters.add(sourceStartInclusive - 1)
                }
                delimiters.add(searchStartDelimiterIndex)
                if (delimiters.size == 2) {
                    return@repeat
                }
            }
        }
        if (forwardIndex <= maxForwardIndex) {
            if (includeLeftBound && forwardIndex == sourceStartInclusive) {
                delimiters.add(forwardIndex - 1)
                if (delimiters.size == 2) {
                    return@repeat
                }
            }
            if (isDelimiter(source, forwardIndex, delimiter)) {
                delimiters.add(forwardIndex)
                if (delimiters.size == 2) {
                    return@repeat
                }
            }
        }
        backwardIndex--
        forwardIndex++
    }
    if (delimiters.size != 2) {
        if (includeLeftBound) {
            return source.toByteArray(
                sourceStartInclusive,
                if (delimiters.isEmpty()) sourceEndExclusive else delimiters.single()
            ) to sourceStartInclusive
        }
        return null
    }
    val firstLineIndex = delimiters.first() + delimiter.size
    val lastLineIndexExclusive = delimiters.last()
    check(firstLineIndex <= lastLineIndexExclusive)
    return source.toByteArray(firstLineIndex, lastLineIndexExclusive) to firstLineIndex
}

private fun isDelimiter(
    source: ByteBuffer,
    sourceStartInclusive: Int,
    delimiter: ByteArray,
): Boolean {
    for (i in delimiter.indices) {
        if (source[i + sourceStartInclusive] != delimiter[i]) {
            return false
        }
    }
    return true
}
