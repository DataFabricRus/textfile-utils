package com.gitlab.sszuev.textfiles

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import java.util.function.Consumer

/**
 * An extended iterator with resource closing functionality (extends [AutoCloseable]).
 *
 * It is not [Sequence] but [Iterator], since we can't control every method of [Sequence].
 *
 * The implementation must [close] the underling resource when there is no more items as well.
 * This will make all the _terminal_ operations (e.g. [toList]) and
 * the _intermediate_ operations described by this interface (e.g. [forEach]) safe to use,
 * which means no explicit closing is required (but desirable).
 * Closing the iterator at its end will also make certain methods
 * of the child sequence ([Iterator.asSequence]) and the child flow ([kotlinx.coroutines.flow.asFlow]) safe.
 * But **not** all the methods: such methods as [Sequence.take] is not terminal,
 * so the source resource iterator must be closed explicitly.
 * Or instead of [Iterator.asSequence] and [kotlinx.coroutines.flow.asFlow]
 * the methods [asSafeSequence] and [asSafeFlow] could be used.
 *
 * In any case, the explicit iterator closing is highly recommended since any operations may throw an exceptions.
 */
@Suppress("INAPPLICABLE_JVM_NAME")
@OptIn(kotlin.experimental.ExperimentalTypeInference::class)
interface ResourceIterator<out X> : Iterator<X>, AutoCloseable {

    /**
     * Returns a resource iterator containing only elements matching the given [predicate].
     * The operation is _intermediate_ and _stateless_.
     */
    fun filter(predicate: (X) -> Boolean): ResourceIterator<X>

    /**
     * Returns a resource iterator containing the results of applying the given [transform] function
     * to each element in this iterator.
     * The operation is _intermediate_ and _stateless_.
     */
    fun <R> map(transform: (X) -> R): ResourceIterator<R>

    /**
     * Returns a single resource iterator of all elements from results of [transform] function
     * being invoked on each element of this iterator.
     * The operation is _intermediate_ and _stateless_.
     */
    @OverloadResolutionByLambdaReturnType
    @JvmName("flatMapIterable")
    fun <R> flatMap(transform: (X) -> Iterable<R>): ResourceIterator<R>

    /**
     * Returns a single resource iterator of all elements from results of [transform] function
     * being invoked on each element of this iterator.
     * The operation is _intermediate_ and _stateless_.
     */
    @OverloadResolutionByLambdaReturnType
    @JvmName("flatMapIterator")
    fun <R> flatMap(transform: (X) -> Iterator<R>): ResourceIterator<R>

    /**
     * Returns a resource iterator containing only distinct elements from this iterator.
     * Among equal elements of the iterator, only the first one will be present in the resulting iterator.
     * The elements in the resulting iterator are in the same order as they were in the source.
     * The operation is _intermediate_ and _stateful_.
     */
    fun distinct(): ResourceIterator<X>

    /**
     * Returns a resource iterator containing all elements of this iterator
     * and then all elements of the given "[other]" iterator.
     * The operation is _intermediate_ and _stateless_.
     */
    operator fun plus(other: Iterator<@UnsafeVariance X>): ResourceIterator<X>

    /**
     * Performs the given [action] for each remaining element until all elements
     * have been processed or the [action] throws an exception.
     */
    override fun forEachRemaining(action: Consumer<in X>) {
        use {
            while (hasNext()) {
                action.accept(next())
            }
        }
    }


    /**
     * Returns the first element.
     * The operation is _terminal_.
     * @throws NoSuchElementException if the iterator is empty
     */
    fun first(): X {
        use {
            if (!hasNext())
                throw NoSuchElementException("Iterator is empty.")
            return next()
        }
    }

    /**
     * Returns the first element, or `null` if the iterator is empty.
     * The operation is _terminal_.
     */
    fun firstOrNull(): X? {
        use {
            if (!hasNext())
                return null
            return next()
        }
    }

    /**
     * Returns the single element, or throws an exception if the iterator is empty or has more than one element.
     * The operation is _terminal_.
     */
    fun single(): X {
        use {
            if (!hasNext())
                throw NoSuchElementException("Iterator is empty.")
            val single = next()
            if (hasNext())
                throw IllegalArgumentException("Iterator has more than one element.")
            return single
        }
    }

    /**
     * Returns the number of elements in this iterator.
     * The operation is _terminal_.
     */
    fun count(): Long {
        use {
            var res = 0L
            while (hasNext()) {
                next()
                res++
            }
            return res
        }
    }

    /**
     * Returns a new [MutableList] filled with all elements of this iterator.
     * The operation is _terminal_.
     */
    fun toMutableList(): MutableList<@UnsafeVariance X> {
        use {
            val res = mutableListOf<X>()
            while (hasNext()) {
                res.add(next())
            }
            return res
        }
    }

    /**
     * Returns a [List] containing all elements.
     * The operation is _terminal_.
     */
    fun toList(): List<X> {
        use {
            return toMutableList()
        }
    }

    /**
     * Returns a [Set] of all elements.
     * The returned set preserves the element iteration order of the original iterator.
     * The operation is _terminal_.
     */
    fun toSet(): Set<X> {
        use {
            val res = mutableSetOf<X>()
            while (hasNext()) {
                res.add(next())
            }
            return res
        }
    }

    /**
     * Creates a sequence that returns all elements from this iterator.
     * The sequence is constrained to be iterated only once.
     * It is safe: the resource iterator is closed on method returning,
     * subsequent operations do not require resource closure anymore.
     * The operation is _terminal_.
     */
    fun asSafeSequence(): Sequence<X> {
        use {
            // can't control sequence, so collect everything in mem and close iterator
            return toList().asSequence()
        }
    }

    /**
     * Creates a _cold_ flow that produces values from this iterator.
     * The operation is _intermediate_ and _stateless_.
     * This is safe: the resource iterator is closed when the flow is collected.
     */
    fun asSafeFlow(): Flow<X> {
        return flow {
            for (e in this@ResourceIterator) {
                emit(e)
            }
        }.onCompletion {
            close()
        }
    }
}