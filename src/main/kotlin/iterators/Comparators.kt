package cc.datafabric.textfileutils.iterators

import cc.datafabric.textfileutils.files.isDelimiter
import java.nio.charset.Charset
import kotlin.math.min


/**
 * @throws ClassCastException
 */
@Throws(ClassCastException::class)
@Suppress("UNCHECKED_CAST")
inline fun <reified X> defaultComparator(): Comparator<X> {
    if (X::class.isInstance(Comparable::class)) {
        throw ClassCastException("${X::class} is not Comparable")
    }
    return Comparator { left, right -> (left as Comparable<X>).compareTo(right) }
}

/**
 * For comparing two [ByteArray] strings lexicographically just as common strings.
 * @param charset [Charset] to convert bytes -> string
 */
fun byteArrayStringComparator(
    charset: Charset = Charsets.UTF_8,
): Comparator<ByteArray> {
    return Comparator { a, b ->
        if (a.size == b.size && a.contentEquals(b)) {
            0
        } else {
            val leftString = a.toString(charset)
            val rightString = b.toString(charset)
            leftString.compareTo(rightString)
        }
    }
}

/**
 * For comparing two [ByteArray] strings lexicographically just as common strings.
 * @param charset [Charset]
 * @param cache [MutableMap] used to reduce the number of creations of new [String]s when comparing;
 * note that produced cache-[Map] has [ByteArray] as key-type and
 * therefore use identity-hash-code that would be different for two arrays with the same content
 */
fun byteArrayCachedStringComparator(
    charset: Charset = Charsets.UTF_8,
    cache: MutableMap<ByteArray, String> = hashMapOf(),
): Comparator<ByteArray> {
    return Comparator { a, b ->
        if (a.size == b.size && a.contentEquals(b)) {
            0
        } else {
            val leftString = a.toString(charset, cache)
            val rightString = b.toString(charset, cache)
            leftString.compareTo(rightString)
        }
    }
}

/**
 * Creates [Comparator]<[ByteArray]> using [mapping][map] function and cache optionally.
 * The mapping function transforms [String] to comparable object [X], which actually is used while comparing.
 * @param charset [Charset]
 * @param cache can be `null`; used to reduce the number of creations of new [String]s when comparing;
 * note that produced cache-[Map] has [ByteArray] as key-type and
 * therefore use identity-hash-code that would be different for two arrays with the same content
 * @param map [String] -> [X] mapping
 */
inline fun <reified X : Comparable<X>> byteArrayMapComparator(
    charset: Charset = Charsets.UTF_8,
    cache: MutableMap<ByteArray, String>? = null,
    noinline map: (String) -> X,
): Comparator<ByteArray> = defaultComparator<X>().toByteArrayComparator(charset, cache, map)

/**
 * Creates simple [Comparator]<[ByteArray]> with straightforward implementations,
 * for ASCII strings it should behave just like common [String] comparator, but faster.
 */
fun byteArraySimpleComparator(): Comparator<ByteArray> = Comparator { left, right ->
    val length = min(left.size, right.size)
    for (i in 0 until length) {
        val a = left[i].toInt() and 0xFF
        val b = right[i].toInt() and 0xFF
        if (a != b) {
            return@Comparator a - b
        }
    }
    left.size - right.size
}

/**
 * Creates simple [Comparator]<[ByteArray]> which compares
 * for ASCII strings it should be equivalent to
 * ```
 * left.substringBefore(separator).compareTo(right.substringBefore(separator))
 * ```
 */
fun byteArrayPrefixSimpleComparator(delimiter: ByteArray): Comparator<ByteArray> = Comparator { left, right ->
    val min = min(left.size, right.size)
    for (i in 0..min) {
        val foundLeftDelimiter = isDelimiter(array = left, startIndex = i, delimiter = delimiter)
        val foundRightDelimiter = isDelimiter(array = right, startIndex = i, delimiter = delimiter)
        if (foundLeftDelimiter && foundRightDelimiter) {
            // left == right
            return@Comparator 0
        }
        if (foundLeftDelimiter) {
            // left < right
            return@Comparator -1
        }
        if (foundRightDelimiter) {
            // left > right
            return@Comparator 1
        }
        if (i == min) {
            break
        }
        val a = left[i].toInt() and 0xFF
        val b = right[i].toInt() and 0xFF
        if (a != b) {
            return@Comparator a - b
        }
    }
    left.size - right.size
}

/**
 * Equivalent to
 * ```
 * left.substringBefore(separator).compareTo(right.substringBefore(separator))
 * ```
 * for ASCII symbols.
 */
fun byteArrayPrefixSimpleComparator(delimiter: String): Comparator<ByteArray> =
    byteArrayPrefixSimpleComparator(delimiter.toByteArray(Charsets.UTF_8))

/**
 * Transforms [Comparator]<[ByteArray]> -> [Comparator]<[String]>
 */
fun Comparator<ByteArray>.toStringComparator(charset: Charset): Comparator<String> = Comparator { left, right ->
    this.compare(left.toByteArray(charset), right.toByteArray(charset))
}

/**
 * Creates [ByteArray] comparator from this comparator.
 * Uses cache while comparing to reduce the number of bytes->String transformation.
 * @param charset [Charset]
 * @param cache can be `null`; used to reduce the number of creations of new [String]s when comparing;
 * note that produced cache-[Map] has [ByteArray] as key-type and
 * therefore use identity-hash-code that would be different for two arrays with the same content
 */
internal fun Comparator<String>.toByteArrayComparator(
    charset: Charset = Charsets.UTF_8,
    cache: MutableMap<ByteArray, String>? = null,
) = toByteArrayComparator(charset, cache) { it }

/**
 * Creates [ByteArray] comparator from this comparator.
 * Uses cache while comparing to reduce the number of bytes->String transformation.
 * @param charset [Charset]
 * @param cache can be `null`; used to reduce the number of creations of new [String]s when comparing;
 * note that produced cache-[Map] has [ByteArray] as key-type and
 * therefore use identity-hash-code that would be different for two arrays with the same content
 * @param toX [String] -> [X] mapping
 */
fun <X> Comparator<X>.toByteArrayComparator(
    charset: Charset = Charsets.UTF_8,
    cache: MutableMap<ByteArray, String>? = null,
    toX: (String) -> X,
): Comparator<ByteArray> {
    val toString: ByteArray.() -> String = { toString(charset, cache) }
    return this.toByteArrayComparator(toString, toX)
}

/**
 * Creates comparator for [ByteArray] using the specified mappers.
 */
fun <X> Comparator<X>.toByteArrayComparator(
    asString: ByteArray.() -> String,
    asX: String.() -> X,
): Comparator<ByteArray> {
    return Comparator { a, b ->
        val leftX = a.asString().asX()
        val rightX = b.asString().asX()
        this.compare(leftX, rightX)
    }
}

private fun ByteArray.toString(charset: Charset, cache: MutableMap<ByteArray, String>?): String =
    cache?.getOrPut(this) { toString(charset) } ?: toString(charset)