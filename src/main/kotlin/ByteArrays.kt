package com.gitlab.sszuev.textfiles

import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * Returns a BOM symbols as a byte array.
 */
fun Charset.bomSymbols(): ByteArray = if (this == Charsets.UTF_16) byteArrayOf(-2, -1) else byteArrayOf()

/**
 * Returns the index of the first occurrence of the specified the [array][other]
 * that is present to the left of the [endExclusive] position.
 */
fun ByteBuffer.lastIndexOf(
    startInclusive: Int,
    endExclusive: Int,
    other: ByteArray,
): Int {
    require(other.isNotEmpty())
    require(startInclusive in 0..endExclusive)
    require(endExclusive <= limit())
    if (endExclusive - startInclusive < other.size) {
        return -1
    }
    for (i in (startInclusive..endExclusive - other.size).reversed()) {
        if (equals(left = this, leftStartInclusive = i, leftEndExclusive = i + other.size, right = other)) {
            return i
        }
    }
    return -1
}

/**
 * Returns the index of the first occurrence of the specified the [array][other]
 * that is present to the right of the [startInclusive] position.
 */
fun ByteBuffer.firstIndexOf(
    startInclusive: Int,
    endExclusive: Int,
    other: ByteArray,
): Int {
    require(other.isNotEmpty())
    require(startInclusive in 0..endExclusive)
    require(endExclusive <= limit())
    if (endExclusive - startInclusive < other.size) {
        return -1
    }
    for (i in (startInclusive..endExclusive - other.size)) {
        if (equals(left = this, leftStartInclusive = i, leftEndExclusive = i + other.size, right = other)) {
            return i
        }
    }
    return -1
}

/**
 * Answers `true` if [ByteBuffer] contains array[right] within the specified interval.
 */
fun equals(
    left: ByteBuffer,
    leftStartInclusive: Int,
    leftEndExclusive: Int,
    right: ByteArray,
): Boolean {
    if (leftEndExclusive - leftStartInclusive != right.size) {
        return false
    }
    for (i in right.indices) {
        if (left[i + leftStartInclusive] != right[i]) {
            return false
        }
    }
    return true
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
 * Searches a range of the specified [source] for the specified [searchLine] using the binary search algorithm.
 * The range must be sorted into ascending order according to the [comparator].
 * @param searchLine [ByteArray]
 * @param source [ByteBuffer]
 * @param sourceStartInclusive [Int]
 * @param sourceEndExclusive [Int]
 * @param delimiter [ByteArray]
 * @param comparator [Comparator]
 * @param includeLeftBound if `true` the first line in the range can have star index equal `[sourceStartInclusive]`,
 * otherwise [delimiter] is required before first line;
 * in other word the [source] is treated as if there is a [delimiter] before the left border
 * @param includeRightBound if `true` the last line in the range can have end index equal `[sourceEndExclusive] - 1`,
 * otherwise [delimiter] is required after first line;
 * in other word the [source] is treated as if there is a [delimiter] after the right border
 * @return [Lines]:
 * - `Lines(-1, -1, emptyList())` - cannot find in the [source]; unexpected. on a sorted Array must not happen.
 * - `Lines(-1, N, emptyList())` - not found but definitely less than first `Line` in the [source],
 * where `N` is the left position (in the [source]) of the left `Line` (inclusive)
 * - `Lines(N, -1, emptyList())` - not found but definitely greater than last `Line` in the [source],
 * where `N` is the right position (in the [source]) of the right `Line` (exclusive)
 * - `Lines(N, N, emptyList())` - not found but can be inserted at the position `N` (in the [source]) with shifting [source] to the right,
 * this is the start position of the next (right) `Line`, note that the inserted data should contain delimiter bytes at the beginning
 * - `Lines(N, M, listOf(ByteArray ..))` - found, `N` (inclusive) and `M` (exclusive) positions (in the [source]) of block
 */
fun byteArrayBinarySearch(
    searchLine: ByteArray,
    source: ByteBuffer,
    sourceStartInclusive: Int,
    sourceEndExclusive: Int,
    delimiter: ByteArray,
    comparator: Comparator<ByteArray>,
    includeLeftBound: Boolean = true,
    includeRightBound: Boolean = true,
): Lines {
    checkLineSearchParameters(source, sourceStartInclusive, sourceEndExclusive, delimiter)

    var low = sourceStartInclusive
    var high = sourceEndExclusive
    while (low <= high) {
        if (low == high) {
            if (includeLeftBound && low == sourceStartInclusive) {
                val n = sourceStartInclusive
                return Lines(startInclusive = -1, endExclusive = n, lines = emptyList())
            }
            if (includeRightBound && high == sourceEndExclusive) {
                val n = sourceEndExclusive
                return Lines(startInclusive = n, endExclusive = -1, lines = emptyList())
            }
            throw IllegalStateException()
        }
        val middle = low + high ushr 1
        val current = findLineNearPosition(
            source = source,
            sourceStartInclusive = low,
            sourceEndExclusive = high,
            position = middle,
            delimiter = delimiter,
            includeLeftBound = includeLeftBound && low == sourceStartInclusive,
            includeRightBound = includeRightBound && high == sourceEndExclusive,
        )
        if (current == null) {
            if (!includeLeftBound && low == sourceStartInclusive) {
                val n = (high - 1)
                return Lines(startInclusive = -1, endExclusive = n, lines = emptyList())
            }
            if (!includeRightBound && high == sourceEndExclusive) {
                // end
                val n = low
                return Lines(startInclusive = n, endExclusive = -1, lines = emptyList())
            }
            val n = high
            return Lines(startInclusive = n, endExclusive = n, lines = emptyList())
        }
        val res = comparator.compare(searchLine, current.first)
        val nextHigh = current.second // exclusive
        val nextLow = current.second + current.first.size // inclusive
        if (nextHigh == high && nextLow == low) {
            return Lines.NULL
        }
        if (res < 0) {
            high = nextHigh
        } else if (res > 0) {
            low = nextLow
        } else {
            return findLineBlock(
                foundLine = current,
                source = source,
                sourceStartInclusive = sourceStartInclusive,
                sourceEndExclusive = sourceEndExclusive,
                delimiter = delimiter,
                comparator = comparator,
                includeLeftBound = includeLeftBound && low == sourceStartInclusive,
                includeRightBound = includeRightBound && high == sourceEndExclusive,
            )
        }
    }
    throw IllegalStateException("must be unreachable")
}

/**
 * Finds the boundaries of the block containing the specified string.
 * E.g. for string `xx:13`, source `hh;ee;c;d;xx;xx;w;w;qq` and delimiter `;` the found block will be `xx:[10,15)`.
 * @param foundLine [ByteArray]
 * @param source [ByteBuffer]
 * @param sourceStartInclusive [Int]
 * @param sourceEndExclusive [Int]
 * @param delimiter [ByteArray]
 * @param comparator [Comparator] to select adjacent lines which equal to the given [foundLine] ((compareTo = 0))
 * @param includeLeftBound if `true` the first line in the range can have star index equal `[sourceStartInclusive]`,
 * otherwise [delimiter] is required before first line;
 * in other word the [source] is treated as if there is a [delimiter] before the left border
 * @param includeRightBound if `true` the last line in the range can have end index equal `[sourceEndExclusive] - 1`,
 * otherwise [delimiter] is required after first line;
 * in other word the [source] is treated as if there is a [delimiter] after the right border
 * @return [Lines]
 */
internal fun findLineBlock(
    foundLine: Pair<ByteArray, Int>,
    source: ByteBuffer,
    sourceStartInclusive: Int,
    sourceEndExclusive: Int,
    delimiter: ByteArray,
    comparator: Comparator<ByteArray>,
    includeLeftBound: Boolean = true,
    includeRightBound: Boolean = true,
): Lines {
    checkLineSearchParameters(source, sourceStartInclusive, sourceEndExclusive, delimiter)
    // check left bound
    if (foundLine.startInclusive() - delimiter.size < sourceStartInclusive) {
        if (!includeLeftBound) {
            return Lines.NULL
        }
        if (foundLine.startInclusive() != sourceStartInclusive) {
            return Lines.NULL
        }
    }
    // check right bound
    if (foundLine.endExclusive() > sourceEndExclusive - delimiter.size) {
        if (!includeRightBound) {
            return Lines.NULL
        }
        if (foundLine.endExclusive() != sourceEndExclusive) {
            return Lines.NULL
        }
    }
    val res = mutableListOf<Pair<ByteArray, Int>>()
    readLeftLinesBlock(
        source = source,
        sourceStartInclusive = sourceStartInclusive,
        sourceEndExclusive = foundLine.endExclusive(),
        searchLine = foundLine.bytes(),
        delimiter = delimiter,
        comparator = comparator,
        includeLeftBound = includeLeftBound
    ).forEach {
        res.add(0, it)
    }
    if (res.isEmpty()) {
        // should contain the given line at least, if it is from the source withing given bounds
        // if not -> nothing can be found
        return Lines.NULL
    }
    readRightLinesBlock(
        source = source,
        sourceStartInclusive = foundLine.endExclusive() + delimiter.size,
        sourceEndExclusive = sourceEndExclusive,
        searchLine = foundLine.bytes(),
        delimiter = delimiter,
        comparator = comparator,
        includeRightBound = includeRightBound
    ).forEach {
        res.add(it)
    }
    val n = res.first().startInclusive()
    val m = res.last().endExclusive()
    return Lines(n, m, res.map { it.first })
}

/**
 * Lists all connected lines to the left of the [sourceEndExclusive] position.
 * @param searchLine [ByteArray]
 * @param source [ByteBuffer]
 * @param sourceStartInclusive [Int]
 * @param sourceEndExclusive [Int]
 * @param delimiter [ByteArray]
 * @param comparator [Comparator] to select adjacent lines which equal to the given [searchLine] ((compareTo = 0))
 * @param includeLeftBound if `true` the first line in the range can have star index equal `[sourceStartInclusive]`,
 * otherwise [delimiter] is required before first line;
 * in other word the [source] is treated as if there is a [delimiter] before the left border
 * @return [Sequence]<[Lines]>
 */
internal fun readLeftLinesBlock(
    source: ByteBuffer,
    sourceStartInclusive: Int,
    sourceEndExclusive: Int,
    searchLine: ByteArray,
    delimiter: ByteArray,
    comparator: Comparator<ByteArray>,
    includeLeftBound: Boolean = true,
): Sequence<Pair<ByteArray, Int>> = sequence {
    var leftLineStartInclusive: Int
    var leftLineEndExclusive = sourceEndExclusive
    while (leftLineEndExclusive >= sourceStartInclusive) {
        val nextDelimiterStartInclusive = source.lastIndexOf(
            startInclusive = sourceStartInclusive,
            endExclusive = leftLineEndExclusive,
            other = delimiter,
        )
        leftLineStartInclusive = if (nextDelimiterStartInclusive < sourceStartInclusive) {
            if (includeLeftBound) {
                sourceStartInclusive
            } else {
                break
            }
        } else {
            nextDelimiterStartInclusive + delimiter.size
        }
        val other = source.toByteArray(leftLineStartInclusive, leftLineEndExclusive)
        if (comparator.compare(other, searchLine) != 0) {
            break
        }
        yield(other to leftLineStartInclusive)
        leftLineEndExclusive = leftLineStartInclusive - delimiter.size
    }
}

/**
 * Lists all connected lines to the right of the [sourceStartInclusive] position.
 * @param searchLine [ByteArray]
 * @param source [ByteBuffer]
 * @param sourceStartInclusive [Int]
 * @param sourceEndExclusive [Int]
 * @param delimiter [ByteArray]
 * @param comparator [Comparator] to select adjacent lines which equal to the given [searchLine] ((compareTo = 0))
 * @param includeRightBound if `true` the last line in the range can have end index equal `[sourceEndExclusive] - 1`,
 * otherwise [delimiter] is required after first line;
 * in other word the [source] is treated as if there is a [delimiter] after the right border
 * @return [Sequence]<[Lines]>
 */
internal fun readRightLinesBlock(
    source: ByteBuffer,
    sourceStartInclusive: Int,
    sourceEndExclusive: Int,
    searchLine: ByteArray,
    delimiter: ByteArray,
    comparator: Comparator<ByteArray>,
    includeRightBound: Boolean = true,
): Sequence<Pair<ByteArray, Int>> = sequence {
    var rightLineStartInclusive = sourceStartInclusive
    var rightLineEndExclusive: Int
    while (rightLineStartInclusive <= sourceEndExclusive) {
        val nextDelimiterStartInclusive = source.firstIndexOf(
            startInclusive = rightLineStartInclusive,
            endExclusive = sourceEndExclusive,
            other = delimiter,
        )
        rightLineEndExclusive = if (nextDelimiterStartInclusive == -1) {
            if (includeRightBound) {
                sourceEndExclusive
            } else {
                break
            }
        } else {
            nextDelimiterStartInclusive
        }
        val other = source.toByteArray(rightLineStartInclusive, rightLineEndExclusive)
        if (comparator.compare(other, searchLine) != 0) {
            break
        }
        yield(other to rightLineStartInclusive)
        rightLineStartInclusive = rightLineEndExclusive + delimiter.size
    }
}

/**
 * Finds the first line near the specified [position].
 *
 * @param includeLeftBound if `true` the first line in the range can have star index equal `[sourceStartInclusive]`,
 * otherwise [delimiter] is required before first line;
 * in other word the [source] is treated as if there is a [delimiter] before the left border
 * @param includeRightBound if `true` the last line in the range can have end index equal `[sourceEndExclusive] - 1`,
 * otherwise [delimiter] is required after first line;
 * in other word the [source] is treated as if there is a [delimiter] after the right border
 *
 * @return a [Pair] of found `Line` and its start position in the [source] (inclusive)
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
    checkLineSearchParameters(source, sourceStartInclusive, sourceEndExclusive, delimiter)
    require(position in sourceStartInclusive until sourceEndExclusive) {
        "position=$position !e [$sourceStartInclusive, $sourceEndExclusive)"
    }
    var leftDelimiterStartInclusive: Int? = source.lastIndexOf(
        startInclusive = sourceStartInclusive,
        endExclusive = position,
        other = delimiter,
        default = if (includeLeftBound) sourceStartInclusive - delimiter.size else null,
    )
    var rightDelimiterStartInclusive: Int?
    if (leftDelimiterStartInclusive != null) {
        rightDelimiterStartInclusive = source.firstIndexOf(
            startInclusive = leftDelimiterStartInclusive + delimiter.size,
            endExclusive = sourceEndExclusive,
            other = delimiter,
            default = if (includeRightBound) sourceEndExclusive else null,
        )
        if (rightDelimiterStartInclusive == null) {
            val nextLeftDelimiterStartInclusive = source.lastIndexOf(
                startInclusive = sourceStartInclusive,
                endExclusive = leftDelimiterStartInclusive,
                other = delimiter,
                default = if (includeLeftBound) sourceStartInclusive - delimiter.size else null,
            )
            if (nextLeftDelimiterStartInclusive != null) {
                rightDelimiterStartInclusive = leftDelimiterStartInclusive
                leftDelimiterStartInclusive = nextLeftDelimiterStartInclusive
            }
        }
    } else {
        rightDelimiterStartInclusive = source.firstIndexOf(
            startInclusive = position,
            endExclusive = sourceEndExclusive,
            other = delimiter,
            default = if (includeRightBound) sourceEndExclusive else null,
        )
        if (rightDelimiterStartInclusive != null) {
            leftDelimiterStartInclusive = source.lastIndexOf(
                startInclusive = sourceStartInclusive,
                endExclusive = rightDelimiterStartInclusive,
                other = delimiter,
                default = if (includeLeftBound) sourceStartInclusive - delimiter.size else null,
            )
            if (leftDelimiterStartInclusive == null) {
                val nextRightDelimiterStartInclusive = source.firstIndexOf(
                    startInclusive = rightDelimiterStartInclusive + delimiter.size,
                    endExclusive = sourceEndExclusive,
                    other = delimiter,
                    default = if (includeRightBound) sourceEndExclusive else null,
                )
                if (nextRightDelimiterStartInclusive != null) {
                    leftDelimiterStartInclusive = rightDelimiterStartInclusive
                    rightDelimiterStartInclusive = nextRightDelimiterStartInclusive
                }
            }
        }
    }
    if (leftDelimiterStartInclusive == null || rightDelimiterStartInclusive == null) {
        return null
    }
    val startLineInclusive = leftDelimiterStartInclusive + delimiter.size
    if (startLineInclusive > sourceEndExclusive - 1) {
        return null
    }
    val endLineExclusive = rightDelimiterStartInclusive
    return source.toByteArray(
        startInclusive = startLineInclusive,
        endExclusive = endLineExclusive
    ) to startLineInclusive
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

private fun ByteBuffer.lastIndexOf(
    startInclusive: Int,
    endExclusive: Int,
    other: ByteArray,
    default: Int?,
): Int? {
    val res = lastIndexOf(startInclusive, endExclusive, other)
    return if (res == -1) {
        default
    } else {
        res
    }
}

private fun ByteBuffer.firstIndexOf(
    startInclusive: Int,
    endExclusive: Int,
    other: ByteArray,
    default: Int?,
): Int? {
    val res = firstIndexOf(startInclusive, endExclusive, other)
    return if (res == -1) {
        default
    } else {
        res
    }
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

private fun checkLineSearchParameters(
    source: ByteBuffer,
    sourceStartInclusive: Int,
    sourceEndExclusive: Int,
    delimiter: ByteArray,
) {
    require(sourceStartInclusive < source.limit()) { "start=$sourceStartInclusive >= size=${source.limit()}" }
    require(sourceEndExclusive <= source.limit()) { "end=$sourceEndExclusive > size=${source.limit()}" }
    require(sourceStartInclusive in 0 until sourceEndExclusive) { "start=$sourceStartInclusive !e [0, $sourceEndExclusive)" }
    require(delimiter.isNotEmpty()) { "empty delimiter" }
}

private fun Pair<ByteArray, Int>.startInclusive() = this.second

private fun Pair<ByteArray, Int>.endExclusive() = this.second + this.size()

private fun Pair<ByteArray, Int>.size() = this.first.size

private fun Pair<ByteArray, Int>.bytes() = this.first

/**
 * - `Lines(-1, -1, emptyList())` - cannot find in the source buffer
 * - `Lines(-1, N, emptyList())` - not found, but definitely less than the first left `Line` in the source buffer,
 * where `N` is the left position (in the source) of the left `Line` (inclusive)
 * - `Lines(N, -1, emptyList())` - not found, but definitely greater than the last right `Line` in the source buffer,
 * where `N` is the right position (in the source) of the right `Line` (exclusive)
 * - `Lines(N, N, emptyList())` - not found, but can be inserted at the position `N` (in the source) with shifting source to the right,
 * this is the start position of the next (right) `Line`, note that the inserted bytes should contain delimiter at the beginning
 * - `Lines(N, M, listOf(ByteArray ..))` - found, `N` and `M` the positions (in the source) of block, inclusive and exclusive
 */
data class Lines(val startInclusive: Int, val endExclusive: Int, val lines: List<ByteArray>) {
    companion object {
        val NULL = Lines(-1, -1, emptyList())
    }

    fun lines(charset: Charset): List<String> =
        if (lines.isEmpty()) emptyList() else lines.map { it.toString(charset) }
}