package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.random.Random

internal class ClassicMergeTest {
    companion object {
        @JvmStatic
        fun twoSortedLists(): List<Arguments> = listOf(
            Arguments.arguments(
                listOf(8, 45, 221, 1, 21).sorted(),
                listOf(5, 65, 32, 10, 4).sorted()
            ),
            Arguments.arguments(
                listOf(0, 21),
                listOf(1, 42)
            ),
            Arguments.arguments(
                listOf(1, 2, 3, 4, 5),
                listOf(234, 234234, 5651, 0, -32, -2, 7, 8, 9).sorted()
            ),
            Arguments.arguments(
                (1..42).map { Random.Default.nextInt() }.sorted(),
                (1..13).map { Random.Default.nextInt() }.sorted()
            ),
            Arguments.arguments(
                listOf(5, 6, 7, 8),
                listOf(1, 2, 3, 4),
            ),
            Arguments.arguments(
                emptyList<Int>(),
                listOf(42, 42)
            ),
            Arguments.arguments(
                listOf(42, 42),
                emptyList<Int>()
            ),
            Arguments.arguments(
                emptyList<Int>(),
                emptyList<Int>()
            ),
        )

        @JvmStatic
        fun manySortedLists(): Array<List<List<Any>>> = arrayOf(
            listOf(
                listOf(8, 45, 221, 1, 21).sorted(),
                listOf(8, 45, 221, 1, 21).sorted(),
                listOf(5, 7887).sorted(),
                listOf(42).sorted(),
                listOf(42, 54, 0, -54, -3, -9, -6878, 799, 820, 762, 95, 72, 4017, 83654).sorted(),
                listOf(5, 65, 32, 10, 4).sorted()
            ),
            listOf(
                listOf()
            ),
            listOf(
                listOf(2.3, 1.2, 0.44444546).sorted(),
                listOf(-1.0, -4.9, -2.222, 0.004242, -22.9).sorted(),
                listOf(-1.042, -4.942, -2.222, 0.00424242, -22.942).sorted(),
                listOf(-1.07, -4.97, -2.2227, -22.97).sorted(),
            )
        )
    }

    @ParameterizedTest
    @MethodSource("twoSortedLists")
    fun `test direct iterator-merge`(left: List<Int>, right: List<Int>) {
        val actual: MutableList<Int> = mutableListOf()
        mergeIterators(left.iterator(), right.iterator()) {
            actual.add(it)
        }
        val expected = (left + right).sorted()
        Assertions.assertEquals(expected, actual)
    }

    @ParameterizedTest
    @MethodSource("twoSortedLists")
    fun `test inverse iterator merge`(left: List<Int>, right: List<Int>) {
        val actual: MutableList<Int> = mutableListOf()
        mergeIterators(left.reversed().iterator(), right.reversed().iterator(), { a, b -> b.compareTo(a) }) {
            actual.add(0, it)
        }
        val expected = (left + right).sorted()
        Assertions.assertEquals(expected, actual)
    }

    @ParameterizedTest
    @MethodSource("twoSortedLists")
    fun `test sequence merge`(left: List<Int>, right: List<Int>) {
        val expected = (left + right).sorted()
        val actual = left.asSequence().mergeWith(right.asSequence()).toList()
        Assertions.assertEquals(expected, actual)
    }

    @ParameterizedTest
    @MethodSource("manySortedLists")
    fun `test merge several sources`(given: List<List<Comparable<Any>>>) {
        val expected = given.asSequence().flatMap { it.asSequence() }.sorted().toList()
        val actual = mergeSequences(given.map { it.asSequence() }).toList()
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `test merge single source`() {
        val expected = listOf(4, 2, 42)
        val actual = mergeSequences(listOf(expected.asSequence())).toList()
        Assertions.assertEquals(expected, actual)
    }
}