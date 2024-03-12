package cc.datafabric.textfileutils

import cc.datafabric.textfileutils.files.MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES
import cc.datafabric.textfileutils.files.MERGE_FILES_WRITE_BUFFER_TO_TOTAL_MEMORY_ALLOCATION_RATIO
import cc.datafabric.textfileutils.files.bomSymbols
import cc.datafabric.textfileutils.files.bytes
import cc.datafabric.textfileutils.files.invert
import cc.datafabric.textfileutils.files.mergeFilesInverse
import cc.datafabric.textfileutils.iterators.defaultComparator
import cc.datafabric.textfileutils.iterators.toByteArrayComparator
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.plus
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.random.Random

internal class FileMergeTest {

    companion object {

        fun mergeTextFilesInverse(
            sources: Set<Path>,
            target: Path,
            comparator: Comparator<String> = defaultComparator<String>().reversed(),
            delimiter: String = "\n",
            charset: Charset = Charsets.UTF_8,
            allocatedMemorySizeInBytes: Int = 2 * MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES,
            writeToTotalMemRatio: Double = MERGE_FILES_WRITE_BUFFER_TO_TOTAL_MEMORY_ALLOCATION_RATIO,
            controlDiskspace: Boolean = false,
            coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.IO) + CoroutineName("mergeFilesInverse")
        ) = mergeFilesInverse(
            sources = sources,
            target = target,
            comparator = comparator.toByteArrayComparator(charset),
            delimiter = delimiter.bytes(charset),
            bomSymbols = charset.bomSymbols(),
            allocatedMemorySizeInBytes = allocatedMemorySizeInBytes,
            writeToTotalMemRatio = writeToTotalMemRatio,
            controlDiskspace = controlDiskspace,
            coroutineScope = coroutineScope,
        )

        fun mergeFilesInverseAndInvert(
            leftSource: Path,
            rightSource: Path,
            target: Path,
            allocatedMemorySize: Int,
            deleteSourceFiles: Boolean,
            comparator: Comparator<String>,
            delimiter: String = "\n",
            charset: Charset = Charsets.UTF_8,
        ) {
            val tmp = Files.createFile(Paths.get(target.toAbsolutePath().toString() + ".tmp"))
            mergeTextFilesInverse(
                sources = setOf(leftSource, rightSource),
                target = tmp,
                comparator = comparator,
                delimiter = delimiter,
                charset = charset,
                allocatedMemorySizeInBytes = allocatedMemorySize,
                controlDiskspace = deleteSourceFiles,
            )
            invert(
                source = tmp,
                target = target,
                delimiter = delimiter,
                charset = charset,
            )
        }
    }

    @Test
    fun `test merge small sources with big buffers`(@TempDir dir: Path) {
        val left = Files.createTempFile(dir, "left-", ".xxx")
        val right = Files.createTempFile(dir, "right-", ".xxx")
        val res = Files.createTempFile(dir, "res-", ".xxx")

        val leftContent = listOf(3, 5, 6, 6, 7, 7, 7, 10, 13, 17, 42, 42)
        val rightContent = listOf(2, 12, 14, 17, 23, 23, 23, 23, 39, 40, 41)
        val expectedContent = (leftContent + rightContent).sorted()
        left.writeText(leftContent.joinToString(", "))
        right.writeText(rightContent.joinToString(", "))

        mergeFilesInverseAndInvert(
            leftSource = left,
            rightSource = right,
            target = res,
            allocatedMemorySize = 424242,
            delimiter = ", ",
            deleteSourceFiles = true,
            charset = Charset.defaultCharset(),
            comparator = Comparator<String> { a, b -> a.toInt().compareTo(b.toInt()) }.reversed(),
        )
        Assertions.assertEquals(expectedContent, res.readText().split(", ").map { it.toInt() })
        Assertions.assertFalse(left.exists())
        Assertions.assertFalse(right.exists())
    }

    @Test
    fun `test merge same size sources with small buffers`(@TempDir dir: Path) {
        val left = Files.createTempFile(dir, "left-", ".xxx")
        val right = Files.createTempFile(dir, "right-", ".xxx")
        val res = Files.createTempFile(dir, "res-", ".xxx")

        val leftContent = (1..424).map { Random.Default.nextInt() }.sorted()
        val rightContent = (1..424).map { Random.Default.nextInt() }.sorted()
        val expectedContent = (leftContent + rightContent).sorted()
        left.writeText(leftContent.joinToString("\n"))
        right.writeText(rightContent.joinToString("\n"))

        mergeFilesInverseAndInvert(
            leftSource = left,
            rightSource = right,
            target = res,
            allocatedMemorySize = 42,
            deleteSourceFiles = true,
            delimiter = "\n",
            comparator = Comparator<String> { a, b -> a.toInt().compareTo(b.toInt()) }.reversed()
        )
        val actualContent = res.readText().split("\n").map { it.toInt() }

        Assertions.assertEquals(expectedContent.size, actualContent.size)
        Assertions.assertEquals(expectedContent, actualContent)
    }

    @Test
    fun `test merge small left with big right sources with small buffers`(@TempDir dir: Path) {
        val left = Files.createTempFile(dir, "left-", ".xxx")
        val right = Files.createTempFile(dir, "right-", ".xxx")
        val res = Files.createTempFile(dir, "res-", ".xxx")

        val leftContent = (1..4).map { Random.Default.nextInt() }.sorted()
        val rightContent = (1..4242).map { Random.Default.nextInt() }.sorted()
        val expectedContent = (leftContent + rightContent).sorted()
        left.writeText(leftContent.joinToString("\n"), Charsets.ISO_8859_1)
        right.writeText(rightContent.joinToString("\n"), Charsets.ISO_8859_1)

        mergeFilesInverseAndInvert(
            leftSource = left,
            rightSource = right,
            target = res,
            allocatedMemorySize = 42,
            deleteSourceFiles = true,
            delimiter = "\n",
            charset = Charsets.ISO_8859_1,
            comparator = Comparator<String> { a, b -> a.toInt().compareTo(b.toInt()) }.reversed()
        )
        val actualContent = res.readText(Charsets.ISO_8859_1).split("\n").map { it.toInt() }

        Assertions.assertEquals(expectedContent.size, actualContent.size)
        Assertions.assertEquals(expectedContent, actualContent)
        Assertions.assertFalse(left.exists())
        Assertions.assertFalse(right.exists())
    }

    @Test
    fun `test merge multiple sources`(@TempDir dir: Path) {
        val target = Files.createTempFile(dir, "res-", ".xxx")

        val content1 = (1..42).map { Random.Default.nextInt() }.sorted()
        val content2 = (1..42).map { Random.Default.nextInt() }.sorted()
        val content3 = (1..42).map { Random.Default.nextInt() }.sorted()
        val content4 = (1..420).map { Random.Default.nextInt() }.sorted()
        val content5 = (1..4242).map { Random.Default.nextInt() }.sorted()
        val sources = sequenceOf(content1, content2, content3, content4, content5).mapIndexed { index, content ->
            val source = Files.createTempFile(dir, "source-$index", ".xxx")
            source.writeText(content.joinToString("\n"), Charsets.UTF_16)
            source
        }.toSet()

        mergeTextFilesInverse(
            sources = sources,
            target = target,
            comparator = Comparator<String> { a, b -> a.toInt().compareTo(b.toInt()) }.reversed(),
            charset = Charsets.UTF_16,
            controlDiskspace = true,
            allocatedMemorySizeInBytes = 8912,
        )

        val expected = (content1 + content2 + content3 + content4 + content5).sorted().reversed().joinToString("\n")
        val actual = target.readText(Charsets.UTF_16)
        Assertions.assertEquals(expected, actual)
    }

}