package com.gitlab.sszuev.textfiles

inline fun <reified X> merge(
    sourceLeft: Iterator<X>,
    sourceRight: Iterator<X>,
    comparator: Comparator<X> = defaultComparator(),
    target: (X) -> Unit,
) {
    val right = StepBackIterator(sourceRight)
    sourceLeft.forEach { leftItem ->
        while (right.hasNext()) {
            val rightItem = right.next()
            if (comparator.compare(rightItem, leftItem) < 0) {
                target(rightItem)
            } else {
                right.prev()
                break
            }
        }
        target(leftItem)
    }
    while (right.hasNext()) {
        target(right.next())
    }
}

/**
 * @throws ClassCastException on invoke
 */
@Throws(ClassCastException::class)
@Suppress("UNCHECKED_CAST")
inline fun <reified X> defaultComparator(): Comparator<X> {
    if (X::class.isInstance(Comparable::class)) {
        throw ClassCastException("${X::class} is not Comparable")
    }
    return Comparator { left, right -> (left as Comparable<X>).compareTo(right) }
}

class StepBackIterator<X>(private val base: Iterator<X>) : Iterator<X> {
    private var current: X? = null
    private var previous: X? = null

    override fun hasNext(): Boolean {
        if (previous != null) {
            return true
        }
        return base.hasNext()
    }

    override fun next(): X {
        if (previous != null) {
            val res = checkNotNull(previous)
            previous = null
            return res
        }
        val res = base.next()
        current = res
        return res
    }

    fun prev() {
        previous = current
    }
}