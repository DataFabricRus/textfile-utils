package com.gitlab.sszuev.textfiles

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.random.Random
import kotlin.streams.toList

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
                content = (1..424242).map { Random.nextDouble().toString() },
                coroutineContext = coroutineContext,
                charset = Charsets.UTF_16,
                delimiter = ";",
                comparator =
                defaultComparator(),
                allocatedMemorySizeInBytes = 8912,
            )
        }

    @Test
    fun `test sort small file`(@TempDir dir: Path): Unit = runBlocking {
        testSortRelativelySmallFile(
            dir = dir,
            charset = Charsets.UTF_32LE,
            delimiter = ":::",
            deleteSourceFile = false,
            comparator = defaultComparator(),
            context = Dispatchers.Default,
            lines = 420,
            inMemory = true,
        )
    }

    @Test
    fun `test sort big file`(@TempDir dir: Path): Unit = runBlocking {
        testSortRelativelySmallFile(
            dir = dir,
            charset = Charsets.UTF_32,
            delimiter = "\n",
            deleteSourceFile = true,
            comparator = defaultComparator<String>().reversed(),
            context = Dispatchers.IO,
            lines = 42424, // ~6MB
            inMemory = false,
        )
    }

    private suspend fun testSortRelativelySmallFile(
        dir: Path,
        charset: Charset,
        delimiter: String,
        deleteSourceFile: Boolean,
        context: CoroutineContext,
        comparator: Comparator<String>,
        inMemory: Boolean,
        lines: Int,
    ) {
        val source = Files.createTempFile(dir, "xxx-merge-sort-source-", ".xxx")
        val target = Files.createTempFile(dir, "xxx-merge-sort-target-", ".xxx")

        val content = (1..lines).map { UUID.randomUUID().toString() }.toList()
        source.writeText(content.joinToString(delimiter), charset)
        val fileSize = source.fileSize()
        val allocatedMemory = if (inMemory) {
            fileSize.toInt() * 2
        } else {
            fileSize.toInt() / 3
        }
        suspendSort(
            source = source,
            target = target,
            delimiter = delimiter,
            deleteSourceFile = deleteSourceFile,
            charset = charset,
            allocatedMemorySizeInBytes = allocatedMemory,
            coroutineContext = context,
            comparator = comparator,
        )

        val expected = content.sortedWith(comparator)
        Assertions.assertTrue(target.exists())
        val actualContent = target.readText(charset)
        val actual = actualContent.split(delimiter)
        Assertions.assertEquals(expected, actual)

        if (deleteSourceFile) {
            Assertions.assertFalse(source.exists())
        } else {
            Assertions.assertTrue(source.exists())
        }

        val expectedFiles = if (deleteSourceFile) listOf(target) else listOf(source, target).sorted()
        Assertions.assertEquals(
            expectedFiles,
            Files.walk(dir).filter { it.fileName.toString().startsWith("xxx-") }.sorted().toList()
        )

        Assertions.assertEquals(fileSize, target.fileSize())
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
            comparator = comparator,
            delimiter = delimiter,
            allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
            deleteSourceFile = true,
            charset = charset,
            coroutineContext = coroutineContext,
        )

        Assertions.assertTrue(parts.size > 1)

        assertPartsSize(expectedSize, parts, delimiter.toByteArray(charset).size, charset.bomSymbols().size)

        val expectedContent = content.groupBy { it }.mapValues { it.value.map { 1 }.sum() }
        assertPartsContent(expectedContent, parts, comparator) {
            it.readText(charset).split(delimiter)
        }

        Assertions.assertFalse(source.exists())
    }
}