package com.gitlab.sszuev.textfiles

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.random.Random

internal class MergeSortTests {

    companion object {

        private fun assertPartsSize(
            expectedSize: Long,
            parts: List<Path>,
            delimiterLength: Int,
            bomSymbolsLength: Int
        ) {
            var actualSize = 0L
            parts.forEachIndexed { index, path ->
                actualSize += (path.fileSize() - bomSymbolsLength)
                if (index != parts.size - 1) {
                    actualSize += (delimiterLength - bomSymbolsLength)
                }
            }
            Assertions.assertEquals(expectedSize - bomSymbolsLength, actualSize)
        }

        private fun assertPartsContent(
            expected: Map<String, Int>,
            parts: List<Path>,
            comparator: Comparator<String>,
            content: (Path) -> List<String>
        ) {
            val actual = mutableMapOf<String, AtomicInteger>()
            parts.forEach {
                var prev: String? = null
                content(it).forEach { line ->
                    if (prev == null) {
                        prev = line
                    }
                    Assertions.assertTrue(comparator.compare(prev, line) <= 0) { "'$prev' > '$line'" }
                    actual.computeIfAbsent(line) { AtomicInteger() }.incrementAndGet()
                }
            }
            var res = true
            expected.forEach { (expectedLine, expectedCount) ->
                val actualCount = actual[expectedLine]
                if (actualCount == null) {
                    System.err.println("Can't find expected line '$expectedLine'")
                    res = false
                } else if (actualCount.get() != expectedCount) {
                    System.err.println("For expected line '$expectedLine' wrong count: expected $expectedCount, actual $actualCount")
                    res = false
                }
            }
            actual.forEach { (actualLine, actualValue) ->
                val expectedCount = expected[actualLine]
                if (expectedCount == null) {
                    System.err.println("Can't find actual line '$actualLine'")
                    res = false
                } else if (expectedCount != actualValue.get()) {
                    System.err.println("For actual line '$actualLine' wrong count: expected $expectedCount, actual $actualValue")
                    res = false
                }
            }
            Assertions.assertEquals(expected.size, actual.size)
            Assertions.assertTrue(res)
        }
    }

    @Test
    fun `test split and sort (small file, IO dispatcher, direct, UTF-8, new-line delimiter)`(@TempDir dir: Path) =
        runBlocking(Dispatchers.IO) {
            testSplitAndSort(
                dir = dir,
                content = (1..424).map { Random.nextInt().toString() },
                coroutineContext = coroutineContext,
                charset = Charsets.UTF_8,
                delimiter = "\n",
                comparator = defaultComparator(),
                allocatedMemorySizeInBytes = 102,
            )
        }

    @Test
    fun `test split and sort (big file, default dispatcher, reverse, UTF-16, semicolon delimiter)`(@TempDir dir: Path) =
        runBlocking(Dispatchers.Default) {
            testSplitAndSort(
                dir = dir,
                content = (1..42).map { Random.nextDouble().toString() },
                coroutineContext = coroutineContext,
                charset = Charsets.UTF_16,
                delimiter = ";",
                comparator =
                defaultComparator(),
                allocatedMemorySizeInBytes = 8912,
            )
        }

    private suspend fun testSplitAndSort(
        dir: Path,
        content: List<String>,
        coroutineContext: CoroutineContext,
        charset: Charset,
        delimiter: String,
        comparator: Comparator<String>,
        allocatedMemorySizeInBytes: Int,
    ) {
        val source = Files.createTempFile(dir, "merge-sort-", ".xxx")
        val txt = content.joinToString(delimiter)
        source.writeText(txt, charset)
        val expectedSize = source.fileSize()

        val parts = suspendSplitAndSort(
            source = source,
            coroutineContext = coroutineContext,
            delimiter = delimiter,
            charset = charset,
            comparator = comparator,
            deleteSourceFile = true,
            allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
        )

        assertPartsSize(expectedSize, parts, delimiter.toByteArray(charset).size, charset.bomSymbols().size)

        val expectedContent = content.groupBy { it }.mapValues { it.value.map { 1 }.sum() }
        assertPartsContent(expectedContent, parts, comparator) {
            it.readText(charset).split(delimiter)
        }

        Assertions.assertFalse(source.exists())
    }
}