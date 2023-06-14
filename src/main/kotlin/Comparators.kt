package com.gitlab.sszuev.textfiles

import java.nio.charset.Charset

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
 * @param charset [Charset]
 * @param cache can be `null`; used to reduce the number of creation of new [String]s when comparing;
 * note that produced cache-[Map] has [ByteArray] as key-type and
 * therefore use identity-has-code that would be different for two arrays with the same content
 */
fun defaultByteArrayComparator(
    charset: Charset = Charsets.UTF_8,
    cache: () -> MutableMap<ByteArray, String>? = { hashMapOf() },
): Comparator<ByteArray> {
    val theCache = cache()
    return Comparator { a, b ->
        if (a.size == b.size && a.contentEquals(b)) {
            0
        } else {
            val leftString = a.toString(charset, theCache)
            val rightString = b.toString(charset, theCache)
            leftString.compareTo(rightString)
        }
    }
}

/**
 * Creates [ByteArray] comparator from this comparator.
 * Uses cache while comparing to reduce the number of bytes->String transformation.
 * @param charset [Charset]
 * @param cache can be `null`; used to reduce the number of creation of new [String]s when comparing;
 * note that produced cache-[Map] has [ByteArray] as key-type and
 * therefore use identity-has-code that would be different for two arrays with the same content
 */
internal fun Comparator<String>.toByteComparator(
    charset: Charset = Charsets.UTF_8,
    cache: () -> MutableMap<ByteArray, String>? = { hashMapOf() },
) = toByteComparator(charset, cache) { it }

/**
 * Creates [ByteArray] comparator from this comparator.
 * Uses cache while comparing to reduce the number of bytes->String transformation.
 * @param charset [Charset]
 * @param cache can be `null`; used to reduce the number of creation of new [String]s when comparing;
 * note that produced cache-[Map] has [ByteArray] as key-type and
 * therefore use identity-has-code that would be different for two arrays with the same content
 * @param map [String] -> [X] mapping
 */
fun <X> Comparator<X>.toByteComparator(
    charset: Charset = Charsets.UTF_8,
    cache: () -> MutableMap<ByteArray, String>? = { hashMapOf() },
    map: (String) -> X,
): Comparator<ByteArray> {
    val theCache = cache()
    return Comparator { a, b ->
        val leftX = map(a.toString(charset, theCache))
        val rightX = map(b.toString(charset, theCache))
        this.compare(leftX, rightX)
    }
}

/**
 * Creates [Comparator]<[ByteArray]>
 * @param charset [Charset]
 * @param cache can be `null`; used to reduce the number of creation of new [String]s when comparing;
 * note that produced cache-[Map] has [ByteArray] as key-type and
 * therefore use identity-has-code that would be different for two arrays with the same content
 * @param map [String] -> [X] mapping
 */
inline fun <reified X : Comparable<X>> byteArrayComparator(
    charset: Charset = Charsets.UTF_8,
    noinline cache: () -> MutableMap<ByteArray, String>? = { hashMapOf() },
    noinline map: (String) -> X,
): Comparator<ByteArray> = defaultComparator<X>().toByteComparator(charset, cache, map)

private fun ByteArray.toString(charset: Charset, cache: MutableMap<ByteArray, String>?): String =
    cache?.getOrPut(this) { toString(charset) } ?: toString(charset)