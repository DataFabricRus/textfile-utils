package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.random.Random

internal class ClassicMergeTest {
    companion object {
        @JvmStatic
        fun sortedLists(): List<Arguments> {
            return listOf(
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
        }
    }

    @ParameterizedTest
    @MethodSource("sortedLists")
    fun `test direct iterator-merge`(left: List<Int>, right: List<Int>) {
        val actual: MutableList<Int> = mutableListOf()
        mergeIterators(left.iterator(), right.iterator()) {
            actual.add(it)
        }
        val expected = (left + right).sorted()
        Assertions.assertEquals(expected, actual)
    }

    @ParameterizedTest
    @MethodSource("sortedLists")
    fun `test inverse iterator merge`(left: List<Int>, right: List<Int>) {
        val actual: MutableList<Int> = mutableListOf()
        mergeIterators(left.reversed().iterator(), right.reversed().iterator(), { a, b -> b.compareTo(a) }) {
            actual.add(0, it)
        }
        val expected = (left + right).sorted()
        Assertions.assertEquals(expected, actual)
    }

    @ParameterizedTest
    @MethodSource("sortedLists")
    fun `test sequence merge`(left: List<Int>, right: List<Int>) {
        val expected = (left + right).sorted()
        val actual = left.asSequence().mergeWith(right.asSequence()).toList()
        Assertions.assertEquals(expected, actual)
    }
}



