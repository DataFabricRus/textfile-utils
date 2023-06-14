package com.gitlab.sszuev.textfiles


fun <X> Iterator<X>.nextOrNull() = if (hasNext()) next() else null

/**
 * An iterator that can return back one step.
 */
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