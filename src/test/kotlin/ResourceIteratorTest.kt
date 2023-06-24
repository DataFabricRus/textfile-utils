package cc.datafabric.textfileutils

import cc.datafabric.textfileutils.iterators.any
import cc.datafabric.textfileutils.iterators.asResourceIterator
import cc.datafabric.textfileutils.iterators.emptyResourceIterator
import cc.datafabric.textfileutils.iterators.forEach
import cc.datafabric.textfileutils.iterators.generateResourceIterator
import cc.datafabric.textfileutils.iterators.resourceIteratorOf
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.TimeUnit

internal class ResourceIteratorTest {

    companion object {
        private fun assertIsClosed(isClosed: Boolean, iterator: Iterator<Any>) {
            Assertions.assertTrue(isClosed)
            repeat(2) {
                Assertions.assertThrows(IllegalStateException::class.java) {
                    iterator.next()
                }
                Assertions.assertFalse(iterator.hasNext())
            }
        }

        private fun consume(any: Any) {
            Assertions.assertNotNull(any)
        }
    }

    @Test
    fun testFirst() {
        var isClosed = false
        var i = 0
        val n = 10
        val iterator = generateResourceIterator({ if (i++ < n) i else null }) { isClosed = true }
        Assertions.assertEquals(1, iterator.first())
        assertIsClosed(isClosed, iterator)
    }

    @Test
    fun testFirstOnNull() {
        var isClosed1 = false
        val iterator1 = generateResourceIterator({ null }) { isClosed1 = true }
        Assertions.assertNull(iterator1.firstOrNull())
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = false
        var i = 0
        val n = 10
        val iterator2 = generateResourceIterator({ if (i++ < n) i else null }) { isClosed2 = true }
        Assertions.assertEquals(1, iterator2.firstOrNull())
        assertIsClosed(isClosed2, iterator2)
    }

    @Test
    fun testToList() {
        var isClosed = false
        val iterator = resourceIteratorOf("a", "b") { isClosed = true }
        val res = iterator.toList()
        Assertions.assertEquals(listOf("a", "b"), res)
        assertIsClosed(isClosed, iterator)
    }

    @Test
    fun testToSet() {
        var isClosed = false
        val iterator = resourceIteratorOf("a", "b", "c", "b", "c") { isClosed = true }
        val res = iterator.toSet()
        Assertions.assertEquals(setOf("a", "b", "c"), res)
        assertIsClosed(isClosed, iterator)
    }

    @Test
    fun testForEach() {
        var isClosed = false
        val iterator = resourceIteratorOf("a", "b") { isClosed = true }
        val res = mutableListOf<String>()
        iterator.forEach { res.add(it) }
        Assertions.assertEquals(listOf("a", "b"), res)
        assertIsClosed(isClosed, iterator)
    }

    @Test
    fun testForEachRemaining() {
        var isClosed = false
        val iterator = resourceIteratorOf("a", "b") { isClosed = true }
        val res = mutableListOf<String>()
        iterator.forEachRemaining { res.add(it) }
        Assertions.assertEquals(listOf("a", "b"), res)
        assertIsClosed(isClosed, iterator)
    }

    @Test
    fun testFilterToList() {
        var isClosed1 = false
        val source1 = resourceIteratorOf("a", "b", "c", "d") { isClosed1 = true }
        val iterator1 = source1.filter { it == "b" || it == "c" }
        Assertions.assertEquals(listOf("b", "c"), iterator1.toList())
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = false
        val source2 = resourceIteratorOf(1, 2, 3, 4) { isClosed2 = true }
        val iterator2 = source2.filter { it > 2 }.filter { it < 6 }
        Assertions.assertEquals(listOf(3, 4), iterator2.toList())
        assertIsClosed(isClosed2, iterator2)
    }

    @Test
    fun testMapToSet() {
        var isClosed1 = false
        val source1 = resourceIteratorOf(1, 2, 3) { isClosed1 = true }
        val iterator1 = source1.map { it.toString() }
        Assertions.assertEquals(setOf("1", "2", "3"), iterator1.toSet())
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = false
        val source2 = resourceIteratorOf("a", "b") { isClosed2 = true }
        val iterator2 = source2.map { it.toCharArray()[0] }.map { it.uppercase() }
        Assertions.assertEquals(setOf("A", "B"), iterator2.toSet())
        assertIsClosed(isClosed2, iterator2)
    }

    @Test
    fun testFilterMapForEach() {
        var isClosed = false
        val source = resourceIteratorOf(1, 2, 3) { isClosed = true }
        val iterator = source.filter { it > 1 }.map { it.toString() }.map { it.toInt() }.filter { it < 42 }
        val res = mutableListOf<Int>()
        iterator.forEach { res.add(it) }
        Assertions.assertEquals(listOf(2, 3), res)
        assertIsClosed(isClosed, iterator)
    }

    @Test
    fun testFlatMapIterableToList() {
        var isClosed1 = false
        val source1 = resourceIteratorOf(listOf(1, 2), listOf(3, 4), listOf(5)) { isClosed1 = true }
        val iterator1 = source1.flatMap { it }
        Assertions.assertEquals(listOf(1, 2, 3, 4, 5), iterator1.toList())
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = false
        val source2 = resourceIteratorOf(
            listOf(listOf(1), emptyList()),
            listOf(listOf(2, 3, 4), listOf(5, 6)),
            listOf(listOf(7))
        ) { isClosed2 = true }
        val iterator2 = source2.flatMap { it }.flatMap { it }
        Assertions.assertEquals(listOf(1, 2, 3, 4, 5, 6, 7), iterator2.toList())
        assertIsClosed(isClosed2, iterator2)

        var isClosed3 = false
        val source3 = resourceIteratorOf(
            "a,b,c",
            "d,e",
            "f",
            "g,h"
        ) { isClosed3 = true }
        val iterator3 = source3.flatMap { it.split(",") }
        Assertions.assertEquals(listOf("a", "b", "c", "d", "e", "f", "g", "h"), iterator3.toList())
        assertIsClosed(isClosed3, iterator3)
    }

    @Test
    fun testFlatMapIteratorForEach() {
        var isClosed1 = false
        val source1 = resourceIteratorOf(listOf(1, 2), listOf(3, 4), listOf(5)) { isClosed1 = true }
        val iterator1 = source1.flatMap { it.iterator() }
        Assertions.assertEquals(listOf(1, 2, 3, 4, 5), iterator1.toList())
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = 0
        val iterator2 = resourceIteratorOf("a", "b") { isClosed2++ }
            .asResourceIterator().flatMap {
                resourceIteratorOf("${it}1", "${it}2", "${it}3") { isClosed2++ }
            }
        val res = mutableListOf<String>()
        iterator2.forEach { res.add(it) }
        Assertions.assertEquals(listOf("a1", "a2", "a3", "b1", "b2", "b3"), res)
        Assertions.assertEquals(6, isClosed2)
    }

    @Test
    fun testDistinctForEach() {
        var isClosed = false
        val source = resourceIteratorOf("a", "b", "c", "c", "b", "b", "b", "d") { isClosed = true }
        val iterator = source.distinct()
        val items = setOf("a", "b", "c", "d")
        var count = 0
        iterator.forEachRemaining {
            count++
            Assertions.assertTrue(items.contains(it))
        }
        Assertions.assertEquals(items.size, count)
        assertIsClosed(isClosed, iterator)
    }

    @Test
    fun testConcatForEach() {
        var isClosed1 = false
        var isClosed2 = false
        var isClosed3 = false
        val source1 = resourceIteratorOf("a", "b") { isClosed1 = true }
        val source2 = resourceIteratorOf("c", "d") { isClosed2 = true }
        val source3 = resourceIteratorOf("e") { isClosed3 = true }
        val iterator = source1 + source2 + source3
        val actual = mutableListOf<String>()
        iterator.forEachRemaining {
            actual.add(it)
        }
        Assertions.assertEquals(listOf("a", "b", "c", "d", "e"), actual)
        assertIsClosed(isClosed1, source1)
        assertIsClosed(isClosed2, source2)
        assertIsClosed(isClosed3, source3)
    }

    @Test
    fun testForCycle() {
        var isClosed1 = false
        val iterator1 = resourceIteratorOf("a", "b") { isClosed1 = true }
        var count1 = 0
        for (e in iterator1) {
            consume(e)
            count1++
        }
        Assertions.assertEquals(2, count1)
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = false
        var i = 0
        val n = 10
        val iterator2 = generateResourceIterator({ if (i++ < n) i else null }) { isClosed2 = true }
        var count2 = 0
        for (e in iterator2) {
            consume(e)
            count2++
        }
        Assertions.assertEquals(n, count2)
        assertIsClosed(isClosed2, iterator2)
    }

    @Test
    fun testAnyForEach() {
        var isClosed1 = false
        val source1 = resourceIteratorOf(1, 2, 3) { isClosed1 = true }
        val res1 = source1.asResourceIterator().any { it > 1 }
        Assertions.assertTrue(res1)
        Assertions.assertTrue(isClosed1)

        var isClosed2 = false
        val source2 = resourceIteratorOf(1, 2, 3) { isClosed2 = true }
        val res2 = source2.asResourceIterator().any { it > 4 }
        Assertions.assertFalse(res2)
        Assertions.assertTrue(isClosed2)
    }

    @Test
    fun testEmptyIteratorConcatToList() {
        Assertions.assertFalse(emptyResourceIterator<String>().map { 42 }.hasNext())
        Assertions.assertFalse(emptyResourceIterator<String>().filter { true }.hasNext())
        Assertions.assertFalse(emptyResourceIterator<String>().flatMap { resourceIteratorOf("a") }.hasNext())

        var isClosed1 = false
        val iterator1 = emptyResourceIterator<Int>() + resourceIteratorOf(42) { isClosed1 = true }
        Assertions.assertEquals(listOf(42), iterator1.toList())
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = false
        val iterator2 = resourceIteratorOf(42) { isClosed2 = true } + emptyResourceIterator()
        Assertions.assertEquals(listOf(42), iterator2.toList())
        assertIsClosed(isClosed2, iterator2)
    }

    @Test
    fun testCount() {
        var isClosed = false
        val iterator = resourceIteratorOf(1, 2, 3) { isClosed = true }
        Assertions.assertEquals(3, iterator.count())
        Assertions.assertTrue(isClosed)
    }

    @Test
    fun testAsSafeSequence() {
        var i = 0
        var isClosed1 = false
        val iterator1 = generateResourceIterator({ if (i++ < 10) i else null }) { isClosed1 = true }
        val count1 = iterator1.asSafeSequence().count()
        Assertions.assertEquals(10, count1)
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = false
        val iterator2 = resourceIteratorOf(1, 2) { isClosed2 = true }
        val count2 =
            iterator2.asSafeSequence().filter { it < 5 }.map { it * it }.filter { it > 4 }.map { it + it }.count()
        Assertions.assertEquals(0, count2)
        assertIsClosed(isClosed2, iterator2)

        var isClosed3 = false
        var count3 = 0
        val iterator3 = resourceIteratorOf(1, 2, 3) { isClosed3 = true }
        iterator3.asSafeSequence().take(1).forEach { _ ->
            count3++
        }
        Assertions.assertEquals(1, count3)
        assertIsClosed(isClosed3, iterator3)
    }

    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @Test
    fun testAsFlow() = runBlocking {
        var i = 0
        var isClosed1 = false
        val iterator1 = generateResourceIterator({ if (i++ < 10) i else null }) { isClosed1 = true }
        var count1 = 0
        iterator1.asSafeFlow().take(2).buffer(42).collect {
            count1++
        }
        Assertions.assertEquals(2, count1)
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = false
        val iterator2 = resourceIteratorOf(1, 2, 3, 4) { isClosed2 = true }
        var count2 = 0
        iterator2.asSafeFlow().drop(2).filter { it > 3 }.map { it.toString() }.collect {
            count2++
        }
        Assertions.assertEquals(1, count2)
        assertIsClosed(isClosed2, iterator2)
    }

    @Test
    fun rerunTest() {
        var i = 0
        var isClosed1 = false
        val iterator1 = generateResourceIterator({ if (i++ < 10) i else null }) { isClosed1 = true }
        Assertions.assertEquals(10, iterator1.toList().size)
        assertIsClosed(isClosed1, iterator1)
        Assertions.assertEquals(0, iterator1.toList().size)
        assertIsClosed(isClosed1, iterator1)

        var isClosed2 = false
        val iterator2 = resourceIteratorOf(1, 2, 3, 4) { isClosed2 = true }
        Assertions.assertEquals(4, iterator2.toList().size)
        assertIsClosed(isClosed2, iterator2)
        Assertions.assertEquals(0, iterator2.toList().size)
        assertIsClosed(isClosed2, iterator2)

        var isClosed3 = false
        val iterator3 = listOf("a", "b").asResourceIterator { isClosed3 = true }
        Assertions.assertEquals(2, iterator3.toList().size)
        assertIsClosed(isClosed3, iterator3)
        Assertions.assertEquals(0, iterator3.toList().size)
        assertIsClosed(isClosed3, iterator3)
    }

}