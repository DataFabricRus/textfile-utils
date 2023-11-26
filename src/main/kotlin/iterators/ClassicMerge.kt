@file:Suppress("unused")

package cc.datafabric.textfileutils.iterators


/**
 * Classic merge algorithm for [kotlin-sequences][Sequence].
 * The source data must be sorted,
 * i.e., the next element in the iterator must be less than or equal to the previous element.
 * If given data is not sorted, strange output is expected
 * @param [other][Sequence]<[X]>
 * @param [comparator][Comparator] if not specified the type [X] is required to be [Comparable]
 * @param [X] must be [Comparable] if no [comparator] is provided
 * @return [Sequence]<[X]> merged data
 * @throws ClassCastException if no [comparator] is provided and [X] is not [Comparable]
 */
inline fun <reified X> Sequence<X>.mergeWith(
    other: Sequence<X>,
    comparator: Comparator<X> = defaultComparator()
): Sequence<X> = mergeSequences(listOf(this, other), comparator)

/**
 * Classic merge algorithm for [kotlin-sequences][Sequence].
 * The source data must be sorted,
 * i.e., the next element in the iterator must be less than or equal to the previous element.
 * @param [sources][Iterable]<[Sequence]<[X]>>
 * @param [comparator][Comparator] if not specified the type [X] is required to be [Comparable]
 * @param [X] must be [Comparable] if no [comparator] is provided
 * @return [Sequence]<[X]> merged data
 * @throws ClassCastException if no [comparator] is not provided and [X] is not [Comparable]
 */
inline fun <reified X> mergeSequences(
    sources: Iterable<Sequence<X>>,
    comparator: Comparator<X> = defaultComparator()
): Sequence<X> = sequence {
    mergeIterators(sources.map { it.iterator() }, comparator) {
        yield(it)
    }
}

/**
 * Classic merge algorithm for [Iterator].
 * The source data must be sorted,
 * i.e., the next element in the iterator must be less than or equal to the previous element.
 * @param [sources][Iterable]<[Iterator]<[X]>>
 * @param [comparator][Comparator] if not specified the type [X] is required to be [Comparable]
 * @param [X] must be [Comparable] if no [comparator] is provided
 * @return [Iterator]<[X]> merged data
 * @throws ClassCastException if no [comparator] is not provided and [X] is not [Comparable]
 */
inline fun <reified X> mergeIterators(
    sources: Iterable<Iterator<X>>,
    comparator: Comparator<X> = defaultComparator()
): Iterator<X> = iterator {
    mergeIterators(sources, comparator) {
        yield(it)
    }
}

/**
 * Classic merge algorithm for [iterators][Iterator].
 * The source data must be sorted,
 * i.e., the next element in the iterator must be greater than or equal to the previous element.
 * @param [sourceLeft][Iterator]<[X]>
 * @param [sourceRight][Iterator]<[X]>
 * @param [comparator][Comparator] if not specified the type [X] is required to be [Comparable]
 * @param [target] storage to write merged data
 * @param [X] must be [Comparable] if no [comparator] is provided
 * @throws ClassCastException if no [comparator] is not provided and [X] is not [Comparable]
 */
inline fun <reified X> mergeIterators(
    sourceLeft: Iterator<X>,
    sourceRight: Iterator<X>,
    comparator: Comparator<X> = defaultComparator(),
    target: (X) -> Unit,
) = mergeIterators(listOf(sourceLeft, sourceRight), comparator, target)

/**
 * Classic merge algorithm for [collection][Collection] of [iterators][Iterator].
 * The source iterators must produce a sorted sequence of items,
 * i.e., the next element in the iterator must be greater than or equal to the previous element.
 * @param [sources][Iterable]<[Iterator]<[X]>>
 * @param [comparator][Comparator] if not specified the type [X] is required to be [Comparable]
 * @param [target] storage to write merged data
 * @param [X] must be [Comparable] if no [comparator] is provided
 * @throws ClassCastException if no [comparator] is provided and [X] is not [Comparable]
 */
inline fun <reified X> mergeIterators(
    sources: Iterable<Iterator<X>>,
    comparator: Comparator<X> = defaultComparator(),
    target: (X) -> Unit,
) {
    val sourcesMap = sources.mapIndexed { index, iterator -> index to StepBackIterator(iterator) }.toMap(mutableMapOf())
    require(sourcesMap.isNotEmpty())
    while (sourcesMap.isNotEmpty()) {
        val values = sourcesMap.keys.toList().mapNotNull {
            val value = sourcesMap[it]?.nextOrNull()
            if (value == null) {
                sourcesMap.remove(it)
                null
            } else {
                it to value
            }
        }
        if (values.isEmpty()) {
            break
        }
        var min: Pair<Int, X>? = null
        values.forEach {
            if (min == null) {
                min = it
            } else if (comparator.compare(it.second, checkNotNull(min).second) <= 0) {
                min = it
            }
        }
        val m = checkNotNull(min?.second)
        values.forEach {
            if (it.second == m) {
                target(m)
            } else {
                checkNotNull(sourcesMap[it.first]).prev()
            }
        }
    }
}