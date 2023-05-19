package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
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
            it.insertBefore(insertContent.joinToString("\n", "", "\n").toByteArray(), ByteBuffer.allocate(42))
        }
        val expectedContent = insertContent + originalContent
        val actualContent = file.readText().split("\n").map { it.toInt() }
        Assertions.assertEquals(expectedContent, actualContent)
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
            it.insertBefore(txtBefore.toByteArray(), ByteBuffer.allocate(bufferSize))
        }
        val actualText = file.readText()
        Assertions.assertEquals(txtBefore + testTxt, actualText)
    }

    private fun testInsertAtTheBeginningOfEmptyFile(dir: Path, bufferSize: Int) {
        val file = Files.createTempFile(dir, "test-insert-", ".xxx")
        file.use {
            it.insertBefore(testTxt.toByteArray(), ByteBuffer.allocate(bufferSize))
        }
        val actualText = file.readText()
        Assertions.assertEquals(testTxt, actualText)
    }
}