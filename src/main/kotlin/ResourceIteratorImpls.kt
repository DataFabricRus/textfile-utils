package com.gitlab.sszuev.textfiles

@Suppress("INAPPLICABLE_JVM_NAME")
@OptIn(kotlin.experimental.ExperimentalTypeInference::class)
internal abstract class BaseResourceIterator<X>(
    private val onClose: () -> Unit,
) : ResourceIterator<X> {
    protected var open: Boolean = true

    override fun filter(predicate: (X) -> Boolean): ResourceIterator<X> {
        checkOpen()
        return FilteringResourceIterator(this, onClose, predicate)
    }

    override fun <R> map(transform: (X) -> R): ResourceIterator<R> {
        checkOpen()
        return TransformingResourceIterator(this, onClose, transform)
    }

    @OverloadResolutionByLambdaReturnType
    @JvmName("flatMapIterable")
    override fun <R> flatMap(transform: (X) -> Iterable<R>): ResourceIterator<R> {
        checkOpen()
        return FlatteningResourceIterator(this, transform, Iterable<R>::iterator)
    }

    @OverloadResolutionByLambdaReturnType
    @JvmName("flatMapIterator")
    override fun <R> flatMap(transform: (X) -> Iterator<R>): ResourceIterator<R> {
        checkOpen()
        return FlatteningResourceIterator(this, transform) { it }
    }

    override fun distinct(): ResourceIterator<X> {
        val seen = hashSetOf<X>()
        return filter { seen.add(it) }
    }

    override fun plus(other: Iterator<X>): ResourceIterator<X> {
        return CompoundResourceIterator(this, other)
    }

    override fun close() {
        open = false
        onClose()
    }

    protected fun checkOpen() {
        check(open) { "Iterator is closed." }
    }

    internal fun asInternalSequence(): Sequence<X> {
        return Sequence { this }.constrainOnce()
    }
}

internal open class WrappedResourceIterator<X>(
    protected val source: Iterator<X>,
    onClose: () -> Unit,
) : BaseResourceIterator<X>(onClose) {

    internal fun withOnClose(onClose: () -> Unit): WrappedResourceIterator<X> {
        return WrappedResourceIterator(this.source, onClose)
    }

    override fun hasNext(): Boolean {
        if (!open) {
            return false
        }
        if (source.hasNext()) {
            return true
        }
        close()
        return false
    }

    override fun next(): X {
        checkOpen()
        try {
            return source.next()
        } catch (ex: NoSuchElementException) {
            close()
            throw ex
        }
    }

    override fun close() {
        super.close()
        (source as? AutoCloseable)?.close()
    }
}

internal class FilteringResourceIterator<X>(
    source: Iterator<X>,
    onClose: () -> Unit,
    private val predicate: (X) -> Boolean
) : WrappedResourceIterator<X>(source, onClose) {

    private var nextState: Int = -1 // -1 for unknown, 0 for done, 1 for continue
    private var nextItem: X? = null

    private fun calcNext() {
        while (source.hasNext()) {
            val item = source.next()
            if (predicate(item)) {
                nextItem = item
                nextState = 1
                return
            }
        }
        nextState = 0
        close()
    }

    override fun next(): X {
        checkOpen()
        if (nextState == -1)
            calcNext()
        if (nextState == 0)
            throw NoSuchElementException()
        val result = nextItem
        nextItem = null
        nextState = -1
        @Suppress("UNCHECKED_CAST")
        return result as X
    }

    override fun hasNext(): Boolean {
        if (nextState == -1)
            calcNext()
        return nextState == 1
    }
}

internal class TransformingResourceIterator<T, R>(
    private val source: Iterator<T>,
    onClose: () -> Unit,
    private val transformer: (T) -> (R),
) : BaseResourceIterator<R>(onClose) {

    override fun hasNext(): Boolean {
        if (!open) {
            return false
        }
        if (source.hasNext()) {
            return true
        }
        close()
        return false
    }

    override fun next(): R {
        checkOpen()
        try {
            return transformer(source.next())
        } catch (ex: NoSuchElementException) {
            close()
            throw ex
        }
    }
}

internal class GeneratorResourceIterator<T>(
    private val getNext: () -> T?,
    onClose: () -> Unit,
) : BaseResourceIterator<T>(onClose) {
    private var nextItem: T? = null
    private var nextState: Int = -1 // -1 for next unknown, 0 for done, 1 for continue

    private fun calcNext() {
        nextItem = getNext()
        nextState = if (nextItem == null) 0 else 1
        if (nextState == 0) {
            close()
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun next(): T {
        checkOpen()
        if (nextState < 0) {
            calcNext()
        }
        if (nextState == 0) {
            throw NoSuchElementException()
        }
        val res = nextItem as T
        // Do not clean nextItem (to avoid keeping reference on yielded instance) -- need to keep state for getNextValue
        nextState = -1
        return res
    }

    override fun hasNext(): Boolean {
        if (!open) {
            return false
        }
        if (nextState < 0) {
            calcNext()
        }
        return nextState == 1
    }
}

internal class FlatteningResourceIterator<T, R, E>(
    private val source: ResourceIterator<T>,
    private val transformer: (T) -> R,
    private val iteratorMap: (R) -> Iterator<E>,
) : BaseResourceIterator<E>({ source.close() }) {
    private var itemIterator: Iterator<E>? = null

    override fun next(): E {
        checkOpen()
        if (!ensureItemIterator())
            throw NoSuchElementException()
        return itemIterator!!.next()
    }

    override fun hasNext(): Boolean {
        return ensureItemIterator()
    }

    private fun ensureItemIterator(): Boolean {
        if (!open) {
            return false
        }
        if (itemIterator?.hasNext() == false) {
            itemIterator = null
        }
        while (itemIterator == null) {
            if (!source.hasNext()) {
                close()
                return false
            } else {
                val element = source.next()
                val nextItemIterator = iteratorMap(transformer(element))
                if (nextItemIterator.hasNext()) {
                    itemIterator = nextItemIterator
                    return true
                }
            }
        }
        return true
    }
}

internal class CompoundResourceIterator<T>(
    left: Iterator<T>,
    right: Iterator<T>,
) : BaseResourceIterator<T>({}) {
    private var pending = mutableListOf<Iterator<T>?>()
    private var handled = mutableSetOf<Iterator<T>>()
    private var index = 0
    private var current: Iterator<T> = left

    init {
        pending.add(right)
        handled.add(current)
    }

    override fun hasNext(): Boolean {
        if (!open) {
            return false
        }
        while (!current.hasNext() && index < pending.size) {
            current = advance()
        }
        if (current.hasNext()) {
            return true
        }
        close()
        return false
    }

    override fun next(): T {
        checkOpen()
        if (!hasNext()) {
            throw NoSuchElementException()
        }
        return current.next()
    }

    override fun plus(other: Iterator<T>): ResourceIterator<T> {
        pending.add(other)
        return this
    }

    override fun close() {
        open = false
        handled.forEach { (it as? AutoCloseable)?.close() }
        pending.clear()
        handled.clear()
    }

    private fun advance(): Iterator<T> {
        val res = checkNotNull(pending[index])
        handled.add(res)
        pending[index] = null
        index += 1
        return res
    }
}

@Suppress("INAPPLICABLE_JVM_NAME")
@OptIn(kotlin.experimental.ExperimentalTypeInference::class)
internal class EmptyResourceIterator<X> : BaseResourceIterator<X>({}) {
    override fun filter(predicate: (X) -> Boolean): ResourceIterator<X> {
        return EmptyResourceIterator()
    }

    override fun <R> map(transform: (X) -> R): ResourceIterator<R> {
        return EmptyResourceIterator()
    }

    @OverloadResolutionByLambdaReturnType
    @JvmName("flatMapIterable")
    override fun <R> flatMap(transform: (X) -> Iterable<R>): ResourceIterator<R> {
        return EmptyResourceIterator()
    }

    @OverloadResolutionByLambdaReturnType
    @JvmName("flatMapIterator")
    override fun <R> flatMap(transform: (X) -> Iterator<R>): ResourceIterator<R> {
        return EmptyResourceIterator()
    }

    override fun distinct(): ResourceIterator<X> {
        return EmptyResourceIterator()
    }

    override fun close() {
    }

    override fun plus(other: Iterator<X>): ResourceIterator<X> {
        return if (other is AutoCloseable) {
            other.asResourceIterator { other.close() }
        } else {
            other.asResourceIterator()
        }
    }

    override fun hasNext(): Boolean {
        return false
    }

    override fun next(): X {
        throw NoSuchElementException()
    }
}