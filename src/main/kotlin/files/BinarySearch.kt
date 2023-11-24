package cc.datafabric.textfileutils.files

import cc.datafabric.textfileutils.iterators.defaultComparator
import cc.datafabric.textfileutils.iterators.toByteArrayComparator
import cc.datafabric.textfileutils.iterators.toByteArrayLinearSearchComparator
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.Charset
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.math.max
import kotlin.math.min

/**
 * Searches the [source] file for strings that match the specified [search-string][searchLine]
 * using the binary search algorithm.
 * The data in the source must be sorted, otherwise the result is unpredictable.
 * **Note!
 * The lexicographically sorted content of a file may not be considered sorted if,
 * for example, there is an empty line at the end of the file.**
 * @param source [Path] file
 * @param searchLine [String] - pattern to search
 * @param delimiter [ByteArray] default `\n`
 * @param buffer [ByteBuffer] to use while reading data from file; default `16384`; for IO `DirectByteBuffer` is most appropriate;
 * note that due to implementation restriction [buffer] size must be greater or equal than [maxLineLengthInBytes]
 * @param charset [Charset] default `UTF-8`
 * @param comparator [Comparator]<[String]> to compare lines
 * @param maxLineLengthInBytes [Int] line restriction, to avoid memory lack, e.g.,
 * when there is no delimiter, default = `8192`
 * @param maxOfLinesPerBlock [Int] maximum number of lines in a paragraph
 * @return [Pair]<[Long], [List]<[String]>> - the position of bytes in the source channel to the block of found strings;
 * if nothing is found, then the first member of the pair is the position of the next existing string
 */
fun binarySearch(
    source: Path,
    searchLine: String,
    buffer: ByteBuffer = ByteBuffer.allocateDirect(BINARY_SEARCH_DEFAULT_BUFFER_SIZE_IN_BYTES),
    charset: Charset = Charsets.UTF_8,
    delimiter: String = "\n",
    comparator: Comparator<String> = defaultComparator(),
    maxOfLinesPerBlock: Int = BINARY_SEARCH_MAX_NUM_OF_LINES_PER_BLOCK,
    maxLineLengthInBytes: Int = MAX_LINE_LENGTH_IN_BYTES,
): Pair<Long, List<String>> = source.use(StandardOpenOption.READ) {
    it.binarySearch(
        startAreaInclusive = 0,
        endAreaExclusive = it.size(),
        searchLine = searchLine.toByteArray(charset),
        buffer = buffer,
        delimiter = delimiter.bytes(charset),
        comparator = comparator,
        maxLineLengthInBytes = maxLineLengthInBytes,
        maxOfLinesPerBlock = maxOfLinesPerBlock,
    )
}.let { res ->
    res.first to res.second.map { it.toString(charset) }
}

/**
 * Searches the source channel for strings that match the specified [search-string][searchLine]
 * using the binary search algorithm.
 * The data in the channel must be sorted, otherwise the result is unpredictable.
 * **Note!
 * The lexicographically sorted content of a file may not be considered sorted if,
 * for example, there is an empty line at the end of the file.**
 * @param searchLine [ByteArray] pattern
 * @param startAreaInclusive [Long] the starting position in the file, default `0`
 * @param endAreaExclusive [Long] the end position in the file, default [SeekableByteChannel.size]
 * @param delimiter [ByteArray] default `\n`
 * @param buffer [ByteBuffer] to use while reading data from file; default `16386`;
 * for IO `DirectByteBuffer` is most appropriate;
 * note that due to implementation restriction [buffer] size must be greater or equal than `([maxLineLengthInBytes] + [delimiter].size) * 2`
 * @param charset [Charset] default `UTF-8`
 * @param comparator [Comparator]<[String]> to compare lines
 * @param maxLineLengthInBytes [Int] default = `8192`;
 * note that duet to implementation restriction [maxLineLengthInBytes] must be less or equal than `([buffer].size / 2 - [delimiter].size)`
 * @param maxOfLinesPerBlock [Int] maximum number of lines in a paragraph, to avoid memory leaks with huge blocks lines of equal by comparator
 * @return [Pair]<[Long], [List]<[ByteArray]>> - the position of bytes in the source channel to the block of found strings;
 * if nothing is found, then the first member of the pair is the position of the next existing string;
 * to insert new line at this position append delimiter to beginning of inserted string, if the position is not 0
 */
fun SeekableByteChannel.binarySearch(
    searchLine: ByteArray,
    startAreaInclusive: Long = 0,
    endAreaExclusive: Long = size(),
    buffer: ByteBuffer = ByteBuffer.allocateDirect(BINARY_SEARCH_DEFAULT_BUFFER_SIZE_IN_BYTES),
    charset: Charset = Charsets.UTF_8,
    delimiter: ByteArray = "\n".toByteArray(charset),
    comparator: Comparator<String> = defaultComparator(),
    maxOfLinesPerBlock: Int = BINARY_SEARCH_MAX_NUM_OF_LINES_PER_BLOCK,
    maxLineLengthInBytes: Int = MAX_LINE_LENGTH_IN_BYTES,
): Pair<Long, List<ByteArray>> {
    require(startAreaInclusive in 0 until endAreaExclusive) {
        "wrong range: start-position-inclusive=$startAreaInclusive must be less than end-position-exclusive=$endAreaExclusive)"
    }
    require(delimiter.isNotEmpty())
    require(maxLineLengthInBytes > 1)
    require(maxOfLinesPerBlock >= 1)
    val minBufferSize = (maxLineLengthInBytes + delimiter.size) * 2
    require(buffer.capacity() >= minBufferSize) {
        "buffer size ${buffer.capacity()} must be >= 2 * (max-line-length + delimiter-length) = $minBufferSize"
    }
    var foundLines: Lines? = null
    var absoluteLowInclusive = startAreaInclusive
    var absoluteHighExclusive = endAreaExclusive
    var prevSearchArea: Pair<Long, Long>? = null
    buffer.clear()
    while (Lines.NULL != foundLines) {
        val searchArea = absoluteBounds(absoluteLowInclusive, absoluteHighExclusive, buffer)
        if (absoluteLowInclusive to absoluteHighExclusive == prevSearchArea) {
            // Cycle. Is the file sorted?"
            return searchArea.second to emptyList()
        }
        prevSearchArea = absoluteLowInclusive to absoluteHighExclusive
        this.position(searchArea.first)
        buffer.position(0)
        this.read(buffer)
        if (buffer.position() == 0) {
            return searchArea.second to emptyList()
        }

        foundLines = byteArrayBinarySearch(
            source = buffer,
            searchLine = searchLine,
            sourceStartInclusive = 0,
            sourceEndExclusive = buffer.position(),
            delimiter = delimiter,
            comparator = comparator.toByteArrayComparator(charset = charset),
            includeLeftBound = searchArea.first == startAreaInclusive,
            includeRightBound = searchArea.second == endAreaExclusive,
        )
        if (foundLines.endExclusive == -1 && foundLines.startInclusive == -1) { // Lines.NULL
            // cannot find
            throw IllegalStateException("can't find line '${searchLine.toString(charset)}'")
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
    throw IllegalStateException("must be unreachable; please report a bug")
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
    check(low <= high) { "low=$low > high=$high; please report bug" }
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
    this.syncReadByteLines(
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
    readPosition: Long, // end (exclusive) position of the previous found line on the right
    buffer: ByteBuffer,
    searchLine: ByteArray,
    delimiter: ByteArray,
    comparator: Comparator<ByteArray>,
    maxOfLines: Int,
    maxLineLengthInBytes: Int,
    res: MutableList<ByteArray>,
) {
    this.syncReadByteLines(
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
