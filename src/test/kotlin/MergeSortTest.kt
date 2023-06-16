package com.gitlab.sszuev.textfiles

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.random.Random
import kotlin.streams.toList

internal class MergeSortTest {

    companion object {

        internal fun assertPartsSize(
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

        internal fun assertPartsContent(
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

        internal fun generateLargeFile(numUniqueLines: Int, numDuplicateLines: Int, target: () -> Path): Path {
            val res = target()
            val numLines = numUniqueLines + numDuplicateLines
            val ratio = numDuplicateLines / numLines.toDouble()
            Random.Default.nextDouble()
            val duplicates = mutableListOf<String>()
            var duplicatesCount = 0
            while (duplicatesCount < numDuplicateLines) {
                val v = UUID.randomUUID().toString()
                val n = Random.Default.nextInt(4) + 1
                repeat((1..n).count()) {
                    duplicates.add(v)
                }
                duplicatesCount += n
            }
            duplicates.shuffle()
            Files.newBufferedWriter(res, Charsets.UTF_8).use {
                (1..numLines).forEach { index ->
                    val line = if (duplicates.isNotEmpty() && Random.Default.nextDouble() < ratio) {
                        duplicates.removeLast()
                    } else {
                        UUID.randomUUID().toString()
                    }
                    it.write("$line::$index")
                    if (index != numLines) {
                        it.write("\n")
                    }
                }
            }
            return res
        }

        internal fun checkIsSorted(
            file: Path,
            comparator: Comparator<String>,
            lines: Int,
        ) {
            var num: Long = (lines + 1L) * lines / 2
            Files.newBufferedReader(file, Charsets.UTF_8).useLines {
                var prev: String? = null
                it.forEach { line ->
                    if (prev == null) {
                        prev = line
                    }
                    Assertions.assertTrue(comparator.compare(prev, line) <= 0) { "'$prev' > '$line'" }
                    val index = line.split("::")[1].toInt()
                    num -= index
                }
            }
            Assertions.assertEquals(0, num)
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
            deleteSourceFile = true,
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
            deleteSourceFile = false,
            comparator = defaultComparator<String>().reversed(),
            context = Dispatchers.IO,
            lines = 42424, // ~6MB
            inMemory = false,
        )
    }

    @Test
    fun `test suspended sort large file`(@TempDir dir: Path): Unit = runBlocking {
        val numLines = 200_000
        val numDuplicates = 10_000
        val source = generateLargeFile(numLines, numDuplicates) {
            Files.createTempFile(dir, "xxx-merge-sort-source-", ".xxx")
        }
        val target = Paths.get(source.toString().replace("-source-", "-target-"))

        val fileSize = source.fileSize()
        val allocatedMemory = fileSize.toInt() / 3
        val comparator = Comparator<String> { left, right ->
            val a = left.substringBefore("::")
            val b = right.substringBefore("::")
            a.compareTo(b)
        }

        suspendSort(
            source = source,
            target = target,
            delimiter = "\n",
            controlDiskspace = true,
            charset = Charsets.UTF_8,
            allocatedMemorySizeInBytes = allocatedMemory,
            coroutineContext = Dispatchers.IO,
            comparator = comparator,
        )

        checkIsSorted(target, comparator, numLines + numDuplicates)
        Assertions.assertEquals(fileSize, target.fileSize())
    }

    @Test
    fun `test blocking sort large file`(@TempDir dir: Path) {
        val numLines = 142_000
        val numDuplicates = 100_000
        val source = generateLargeFile(numLines, numDuplicates) {
            Files.createTempFile(dir, "xxx-merge-sort-source-", ".xxx")
        }
        val target = Paths.get(source.toString().replace("-source-", "-target-"))

        val fileSize = source.fileSize()
        val allocatedMemory = fileSize.toInt() / 4
        val comparator = Comparator<String> { left, right ->
            val a = left.substringBefore("::")
            val b = right.substringBefore("::")
            a.compareTo(b)
        }.reversed()

        sort(
            source = source,
            target = target,
            delimiter = "\n",
            controlDiskspace = false,
            charset = Charsets.UTF_8,
            allocatedMemorySizeInBytes = allocatedMemory,
            comparator = comparator,
        )

        checkIsSorted(target, comparator, numLines + numDuplicates)
        Assertions.assertEquals(fileSize, target.fileSize())
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
        val target = Paths.get(source.toString().replace("-source-", "-target-"))

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
            controlDiskspace = deleteSourceFile,
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
            controlDiskspace = true,
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