package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.fileSize
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.random.Random

class FileUtilsTest {

    companion object {
        val testTxt = """
            Lorem ipsum dolor sit amet, consectetur adipiscing elit, 
            sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
            Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. 
            Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. 
            Excepteur sint occaecat cupidatat non proident, 
            sunt in culpa qui officia deserunt mollit anim id est laborum.
        """.trimIndent()
    }

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
                buffer = ByteBuffer.allocate(42),
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
                buffer = ByteBuffer.allocate(42),
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


    private fun testInsertAtTheBeginningOfNonEmptyFile(dir: Path, bufferSize: Int) {
        val txtBefore = """
            Sed ut perspiciatis, unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, 
            totam rem aperiam eaque ipsa, quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt, 
            explicabo.
        """.trimIndent()
        val file = Files.createTempFile(dir, "test-insert-", ".xxx")
        file.writeText(testTxt)
        file.use {
            it.insert(data = txtBefore.toByteArray(), beforePosition = 0, buffer = ByteBuffer.allocate(bufferSize))
        }
        val actualText = file.readText()
        Assertions.assertEquals(txtBefore + testTxt, actualText)
    }

    private fun testInsertAtTheBeginningOfEmptyFile(dir: Path, bufferSize: Int) {
        val file = Files.createTempFile(dir, "test-insert-", ".xxx")
        file.use {
            it.insert(data = testTxt.toByteArray(), beforePosition = 0, buffer = ByteBuffer.allocate(bufferSize))
        }
        val actualText = file.readText()
        Assertions.assertEquals(testTxt, actualText)
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
                buffer = ByteBuffer.allocate(bufferSize),
            )
        }
        val actualContentAsString = file.readText(charset)
        Assertions.assertEquals(expectedContentAsString, actualContentAsString)
    }
}