package com.gitlab.sszuev.textfiles

import com.gitlab.sszuev.textfiles.files.invert
import com.gitlab.sszuev.textfiles.files.mergeFilesInverse
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
        fun mergeFiles(
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
            mergeFilesInverse(
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

        mergeFiles(
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

        mergeFiles(
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

        mergeFiles(
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

        mergeFilesInverse(
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