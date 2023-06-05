package com.gitlab.sszuev.textfiles

import java.nio.charset.Charset


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
 * Returns a BOM symbols as a byte array.
 */
fun Charset.bomSymbols(): ByteArray = if (this == Charsets.UTF_16) byteArrayOf(-2, -1) else byteArrayOf()

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
 * Creates [ByteArray] comparator from this comparator.
 * Uses cache while comparing to reduce the number of bytes->String transformation.
 */
internal fun Comparator<String>.toByteComparator(
    charset: Charset,
    cache: () -> MutableMap<ByteArray, String> = { hashMapOf() },
): Comparator<ByteArray> {
    val theCache = cache()
    return Comparator { a, b ->
        val left = theCache.getOrPut(a) { a.toString(charset) }
        val right = theCache.getOrPut(b) { b.toString(charset) }
        this.compare(left, right)
    }
}

internal fun <X> MutableCollection<X>.put(item: X): X {
    add(item)
    return item
}

internal fun Array<ByteArray>.sort(comparator: Comparator<ByteArray>): Array<ByteArray> {
    sortWith(comparator)
    return this
}