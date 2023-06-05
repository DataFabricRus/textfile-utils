package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.random.Random

internal class FileUtilsTest {

    @Test
    fun `test insert at the beginning of file (small buffer)`(@TempDir dir: Path) {
        testInsertAtTheBeginningOfNonEmptyFile(dir, 42)
    }

    @Test
    fun `test insert at the beginning of file (big buffer)`(@TempDir dir: Path) {
        testInsertAtTheBeginningOfNonEmptyFile(dir, 424242)
    }

    @Test
    fun `test insert into empty file (small buffer)`(@TempDir dir: Path) {
        testInsertAtTheBeginningOfEmptyFile(dir, 42)
    }

    @Test
    fun `test insert into empty file (big buffer)`(@TempDir dir: Path) {
        testInsertAtTheBeginningOfEmptyFile(dir, 424242)
    }

    @Test
    fun `test insert at the beginning of file (big file, small buffer)`(@TempDir dir: Path) {
        val originalContent = (0..4242).map { Random.Default.nextInt() }
        val file = Files.createTempFile(dir, "test-insert-", ".xxx")
        file.writeText(originalContent.joinToString("\n"))

        val insertContent = (0..42).map { Random.Default.nextInt() }
        file.use {
            it.insert(
                data = insertContent.joinToString("\n", "", "\n").toByteArray(),
                beforePosition = 0,
                buffer = ByteBuffer.allocateDirect(42),
            )
        }
        val expectedContent = insertContent + originalContent
        val actualContent = file.readText().split("\n").map { it.toInt() }
        Assertions.assertEquals(expectedContent, actualContent)
    }

    @Test
    fun `test insert to the end of file (big file, small buffer)`(@TempDir dir: Path) {
        val originalContent = (0..4242).map { Random.Default.nextInt() }
        val file = Files.createTempFile(dir, "test-insert-", ".xxx")
        file.writeText(originalContent.joinToString("\n"))

        val insertContent = (0..42).map { Random.Default.nextInt() }
        file.use {
            it.insert(
                data = insertContent.joinToString("\n", "\n", "").toByteArray(),
                beforePosition = file.fileSize(),
                buffer = ByteBuffer.allocateDirect(42),
            )
        }
        val expectedContent = originalContent + insertContent
        val actualContent = file.readText().split("\n").map { it.toInt() }
        Assertions.assertEquals(expectedContent, actualContent)
    }

    @Test
    fun `test insert to the beginning of file (big file, small buffer)`(@TempDir dir: Path) {
        testInsertAtRandomPosition(
            dir = dir,
            insertPositionInLines = 39,
            originalContent = (0..4242).map { Random.Default.nextInt() },
            insertContent = (0..42).map { Random.Default.nextInt() },
            bufferSize = 42,
            charset = Charsets.UTF_8,
        )
    }

    @Test
    fun `test insert to the middle of file (big file, small buffer)`(@TempDir dir: Path) {
        testInsertAtRandomPosition(
            dir = dir,
            insertPositionInLines = 210,
            originalContent = (0..420).map { Random.Default.nextInt() },
            insertContent = (0..7).map { Random.Default.nextInt() },
            bufferSize = 4,
            charset = Charsets.UTF_16,
        )
    }

    @Test
    fun `test insert near the end of file (big file, small buffer)`(@TempDir dir: Path) {
        testInsertAtRandomPosition(
            dir = dir,
            insertPositionInLines = 415,
            originalContent = (0..420).map { Random.Default.nextInt() },
            insertContent = (0..7).map { Random.Default.nextInt() },
            bufferSize = 4,
            charset = Charsets.UTF_8,
        )
    }

    @Test
    fun `test insert to the middle of file (small file, big buffer)`(@TempDir dir: Path) {
        testInsertAtRandomPosition(
            dir = dir,
            insertPositionInLines = 210,
            originalContent = (0..420).map { Random.Default.nextInt() },
            insertContent = (0..7).map { Random.Default.nextInt() },
            bufferSize = 424242,
            charset = Charset.defaultCharset(),
        )
    }

    @Test
    fun `test invert file content`(@TempDir dir: Path) {
        val source = Files.createTempFile(dir, "test-insert-source-", ".xxx")
        val target = Files.createTempFile(dir, "test-insert-target-", ".xxx")
        val sourceContent = (1..42).map { Random.nextDouble() }
        source.writeText(sourceContent.joinToString(";"), Charsets.UTF_8)

        invert(source = source, target = target, delimiter = ";", deleteSourceFiles = true, charset = Charsets.UTF_8)

        val targetContent = target.readText(Charsets.UTF_8).split(";").map { it.toDouble() }
        Assertions.assertEquals(sourceContent.reversed(), targetContent)
        Assertions.assertFalse(source.exists())
    }

    @Test
    fun `test is sorted`(@TempDir dir: Path) {
        val f1 = Files.createTempFile(dir, "test-is-sorted-1-", ".xxx")
        f1.writeText((1..42).map { Random.nextDouble() }.sorted().joinToString(","), Charsets.UTF_8)
        Assertions.assertTrue(
            isSorted(
                file = f1,
                delimiter = ",",
                charset = Charsets.UTF_8,
                comparator = { a, b -> a.toDouble().compareTo(b.toDouble()) }
            )
        )

        val f2 = Files.createTempFile(dir, "test-is-sorted-2-", ".xxx")
        f2.writeText((1..42).map { Random.nextLong() }.reversed().joinToString("\n"), Charsets.UTF_8)
        Assertions.assertFalse(
            isSorted(
                file = f2,
                delimiter = "\n",
                charset = Charsets.UTF_8,
                comparator = { a, b -> a.toDouble().compareTo(b.toDouble()) }
            )
        )

        val f3 = Files.createTempFile(dir, "test-is-sorted-2-", ".xxx")
        f3.writeText(sequenceOf(989, 2, 333, 55454, -1).joinToString(";"), Charsets.UTF_32)
        Assertions.assertFalse(
            isSorted(
                file = f3,
                delimiter = ";",
                charset = Charsets.UTF_32,
                comparator = { a, b -> a.toInt().compareTo(b.toInt()) }
            )
        )

        val f4 = Files.createTempFile(dir, "test-is-sorted-2-", ".xxx")
        f4.writeText(sequenceOf(-999.9, 2, 3, 3, 3, 3, 42).joinToString("\n"), Charsets.ISO_8859_1)
        Assertions.assertTrue(
            isSorted(
                file = f4,
                charset = Charsets.ISO_8859_1,
                comparator = { a, b -> a.toFloat().compareTo(b.toFloat()) }
            )
        )
    }

    @Test
    fun `test sameFilePaths(Path, Path)`() {
        val root1 = Paths.get(".").toRealPath()
        val root2 = Paths.get(root1.toString().lowercase())
        val root3 = Paths.get(root1.toString().replaceFirst("\\", "\\.\\"))
        val root4 = Paths.get(root1.toString().replace("\\", "\\.\\"))
        Assertions.assertTrue(
            sameFilePaths(Paths.get("/tmp/.gitignore"), Paths.get("/tmp/.gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get(".gitignore"), Paths.get(".gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root1\\.gitignore"), Paths.get("$root1\\.gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root1\\.gitignore"), Paths.get(".gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root2\\.gitignore"), Paths.get("$root1\\.gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root2\\.gitignore"), Paths.get("$root3\\.gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root4\\.gitignore"), Paths.get(".gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root1\\..\\${root1.fileName}\\.gitignore"), Paths.get(".gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(
                Paths.get("W:\\tmp\\.\\text-file-sorting\\.\\.gitignore"),
                Paths.get("W:\\tmp\\..\\tmp\\.\\text-file-sorting\\.gitignore")
            )
        )

        Assertions.assertFalse(
            sameFilePaths(
                Paths.get("W:\\xxx${Random.Default.nextInt()}\\text-file-sorting\\.\\.gitignore"),
                Paths.get(".gitignore")
            )
        )
        Assertions.assertFalse(
            sameFilePaths(
                Paths.get("W:\\tmp\\text-file-sorting\\.\\.gitignore"),
                Paths.get("Q:\\tmp\\text-file-sorting\\.gitignore")
            )
        )
        Assertions.assertFalse(
            sameFilePaths(
                Paths.get("W:\\tmp\\uuu\\text-file-sorting\\.\\.gitignore"),
                Paths.get("W:\\tmp\\text-file-sorting\\.gitignore")
            )
        )
        Assertions.assertFalse(
            sameFilePaths(
                Paths.get("W:\\tmp\\text-file-sorting\\.\\.gitignore-1"),
                Paths.get("W:\\tmp\\text-file-sorting\\.gitignore-2")
            )
        )
        Assertions.assertFalse(
            sameFilePaths(Paths.get("/xxx/.gitignore"), Paths.get("/qqq/.gitignore"))
        )
    }

    private fun testInsertAtTheBeginningOfNonEmptyFile(dir: Path, bufferSize: Int) {
        val txtBefore = """
            Sed ut perspiciatis, unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, 
            totam rem aperiam eaque ipsa, quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt, 
            explicabo.
        """.trimIndent()
        val file = Files.createTempFile(dir, "test-insert-", ".xxx")
        file.writeText(TEST_DATA_2)
        file.use {
            it.insert(
                data = txtBefore.toByteArray(),
                beforePosition = 0,
                buffer = ByteBuffer.allocateDirect(bufferSize)
            )
        }
        val actualText = file.readText()
        Assertions.assertEquals(txtBefore + TEST_DATA_2, actualText)
    }

    private fun testInsertAtTheBeginningOfEmptyFile(dir: Path, bufferSize: Int) {
        val file = Files.createTempFile(dir, "test-insert-", ".xxx")
        file.use {
            it.insert(
                data = TEST_DATA_2.toByteArray(),
                beforePosition = 0,
                buffer = ByteBuffer.allocateDirect(bufferSize)
            )
        }
        val actualText = file.readText()
        Assertions.assertEquals(TEST_DATA_2, actualText)
    }

    private fun <X> testInsertAtRandomPosition(
        dir: Path,
        insertPositionInLines:
        Int,
        originalContent: List<X>,
        insertContent: List<X>,
        bufferSize: Int,
        charset: Charset,
    ) {
        val file = Files.createTempFile(dir, "test-insert-", ".xxx")

        val originalContentAsString = originalContent.joinToString("\n")
        val originalContentAsByteArray = originalContentAsString.toByteArray(charset)
        val insertContentAsString = insertContent.joinToString(separator = "\n", prefix = "", postfix = "\n")
        val insertContentAsByteArray = insertContentAsString.toByteArray(charset)
        val insertPositionInBytes = originalContent.take(insertPositionInLines)
            .joinToString(separator = "\n", prefix = "", postfix = "\n")
            .toByteArray(charset).size.toLong()
        val expectedContentAsString = (originalContentAsByteArray.take(insertPositionInBytes.toInt()).toByteArray() +
                insertContentAsByteArray +
                originalContentAsByteArray.drop(insertPositionInBytes.toInt()).toByteArray()
                ).toString(charset)

        file.writeText(originalContentAsString, charset)

        file.use {
            it.insert(
                data = insertContentAsByteArray,
                beforePosition = insertPositionInBytes,
                buffer = ByteBuffer.allocateDirect(bufferSize),
            )
        }
        val actualContentAsString = file.readText(charset)
        Assertions.assertEquals(expectedContentAsString, actualContentAsString)
    }
}