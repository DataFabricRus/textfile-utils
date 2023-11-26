package cc.datafabric.textfileutils

import cc.datafabric.textfileutils.files.blockingSort
import cc.datafabric.textfileutils.files.bomSymbols
import cc.datafabric.textfileutils.files.bytes
import cc.datafabric.textfileutils.files.contentEquals
import cc.datafabric.textfileutils.files.sort
import cc.datafabric.textfileutils.files.suspendSort
import cc.datafabric.textfileutils.files.suspendSplitAndSort
import cc.datafabric.textfileutils.iterators.byteArrayPrefixSimpleComparator
import cc.datafabric.textfileutils.iterators.defaultComparator
import cc.datafabric.textfileutils.iterators.toByteArrayComparator
import cc.datafabric.textfileutils.iterators.toStringComparator
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
                comparator = defaultComparator(),
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
        val comparator = byteArrayPrefixSimpleComparator("::").reversed()

        blockingSort(
            source = source,
            target = target,
            delimiter = "\n".toByteArray(Charsets.UTF_8),
            bomSymbols = byteArrayOf(),
            controlDiskspace = false,
            allocatedMemorySizeInBytes = allocatedMemory,
            comparator = comparator,
        )

        checkIsSorted(target, comparator.toStringComparator(Charsets.UTF_8), numLines + numDuplicates)
        Assertions.assertEquals(fileSize, target.fileSize())
    }

    @Test
    fun `test suspended sort another large file`(@TempDir dir: Path): Unit = runBlocking {
        val charset = Charsets.UTF_8
        val givenContent =
            MergeSortTest::class.java.getResourceAsStream("/shuffled.csv")!!.bufferedReader(charset).readText()
        val expectedContent =
            MergeSortTest::class.java.getResourceAsStream("/sorted.csv")!!.bufferedReader(charset).readText()

        val source = Files.createTempFile(dir, "xxx-merge-sort-source-", ".xxx")
        source.writeText(givenContent, charset)
        val expected = Files.createTempFile(dir, "xxx-merge-sort-expected-", ".xxx")
        expected.writeText(expectedContent, charset)

        val target = Paths.get(source.toString().replace("-source-", "-target-"))

        val fileSize = source.fileSize()
        val allocatedMemory = fileSize.toInt() / 10
        val comparator = Comparator<String> { left, right ->
            val a = left.substringBefore(":")
            val b = right.substringBefore(":")
            a.compareTo(b)
        }.thenComparing { a, b ->
            a.substringAfter(":").compareTo(b.substringAfter(":"))
        }

        sort(
            source = source,
            target = target,
            delimiter = "\n",
            controlDiskspace = false,
            charset = charset,
            allocatedMemorySizeInBytes = allocatedMemory,
            comparator = comparator,
        )

        Assertions.assertTrue(contentEquals(expected, target))
    }

    @Test
    fun `test sort n-triples file with windows new-line symbols`(@TempDir dir: Path) {
        testDefaultSortResourceFile(dir, "/random-win.nt")
    }

    @Test
    fun `test sort n-triples file with linux new-line symbols`(@TempDir dir: Path) {
        testDefaultSortResourceFile(dir, "/random-lin.nt")
    }

    @Test
    fun `test sort small csv-file with empty lines at the end`(@TempDir dir: Path) {
        val content = """
            #_ffdf27f2-9acf-4a39-9c9d-66aa77b37ba3|A
            #_ffdf5659-17d3-4de2-9397-7fbef06ed785|B
            #_ffe4adb7-3e1c-435c-ab1f-11d58b78e227|C
            #_ffe4d7f7-fe0f-4788-931e-99c09a588a5c|D
            #_fffa83e1-6dd7-41c7-8878-415dd02068ba|E
            
            
        """.trimIndent()
        val source = Files.createTempFile(dir, "xxx-merge-sort-source-", ".xxx")
        val target = Paths.get(source.toString().replace("-source-", "-target-"))
        source.writeText(content, Charsets.UTF_8)

        sort(
            source = source,
            target = target,
            charset = Charsets.UTF_8,
            delimiter = "\n",
            comparator = { leftLine, rightLine ->
                leftLine.substringBefore("|").compareTo(rightLine.substringBefore("|"))
            }
        )
        val actual = target.readText(Charsets.UTF_8)
        val expected = """
            

            #_ffdf27f2-9acf-4a39-9c9d-66aa77b37ba3|A
            #_ffdf5659-17d3-4de2-9397-7fbef06ed785|B
            #_ffe4adb7-3e1c-435c-ab1f-11d58b78e227|C
            #_ffe4d7f7-fe0f-4788-931e-99c09a588a5c|D
            #_fffa83e1-6dd7-41c7-8878-415dd02068ba|E
        """.trimIndent()
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `test sort small csv-file`(@TempDir dir: Path) {
        val content = """
            #_ffdf27f2-9acf-4a39-9c9d-66aa77b37ba3|A
            #_ffdf5659-17d3-4de2-9397-7fbef06ed785|B
            #_ffe4adb7-3e1c-435c-ab1f-11d58b78e227|C
            #_008a7b89-bc8d-4a93-8e0f-4307f018d6f7|D
            #_008ee688-e4f7-4be7-afa0-5d25c309d7aa|E
            #_009e4646-00ef-4646-a5d6-02fedec775b9|F
            #_ffe4d7f7-fe0f-4788-931e-99c09a588a5c|G
            #_fffa83e1-6dd7-41c7-8878-415dd02068ba|K
            #_00a7702b-f0a4-4e9b-8e8c-9513e4e1bd57|L
            #_00b663d6-ca2b-4b33-b952-5114e1cb63de|M
            #_00b78ab6-296a-4270-a2f2-b3394b5de8d7|N
        """.trimIndent()
        val source = Files.createTempFile(dir, "xxx-merge-sort-source-", ".xxx")
        val target = Paths.get(source.toString().replace("-source-", "-target-"))
        source.writeText(content, Charsets.UTF_8)

        sort(
            source = source,
            target = target,
            charset = Charsets.UTF_8,
            delimiter = "\n",
            comparator = { leftLine, rightLine ->
                leftLine.substringBefore("|").compareTo(rightLine.substringBefore("|"))
            },
            allocatedMemorySizeInBytes = source.fileSize().toInt(),
        )
        val actual = target.readText(Charsets.UTF_8)
        println(actual)
        val expected = """
            #_008a7b89-bc8d-4a93-8e0f-4307f018d6f7|D
            #_008ee688-e4f7-4be7-afa0-5d25c309d7aa|E
            #_009e4646-00ef-4646-a5d6-02fedec775b9|F
            #_00a7702b-f0a4-4e9b-8e8c-9513e4e1bd57|L
            #_00b663d6-ca2b-4b33-b952-5114e1cb63de|M
            #_00b78ab6-296a-4270-a2f2-b3394b5de8d7|N
            #_ffdf27f2-9acf-4a39-9c9d-66aa77b37ba3|A
            #_ffdf5659-17d3-4de2-9397-7fbef06ed785|B
            #_ffe4adb7-3e1c-435c-ab1f-11d58b78e227|C
            #_ffe4d7f7-fe0f-4788-931e-99c09a588a5c|G
            #_fffa83e1-6dd7-41c7-8878-415dd02068ba|K
        """.trimIndent()
        Assertions.assertEquals(expected, actual)
    }

    @Suppress("BlockingMethodInNonBlockingContext")
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

    @Suppress("BlockingMethodInNonBlockingContext")
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
            comparator = comparator.toByteArrayComparator(charset),
            delimiter = delimiter.bytes(charset),
            bomSymbols = charset.bomSymbols(),
            allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
            controlDiskspace = true,
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

    fun testDefaultSortResourceFile(dir: Path, resource: String) {
        val charset = Charsets.UTF_8
        val givenContent =
            MergeSortTest::class.java.getResourceAsStream(resource)!!.bufferedReader(charset).readText()
        val expected = givenContent.split("\n").sorted()

        val source = Files.createTempFile(dir, "xxx-merge-sort-source-", ".xxx")
        source.writeText(givenContent, charset)
        val target = Paths.get("$source.nt")

        sort(
            source = source,
            target = target,
        )

        val actual = target.readText(charset).split("\n")
        Assertions.assertEquals(expected, actual)
    }

}