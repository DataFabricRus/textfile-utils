package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.fileSize
import kotlin.io.path.writeText
import kotlin.random.Random

internal class InverseLineReaderTest {

    companion object {
        private const val testData = "ການທົດສອບ;îmtîhan;tɛst;시험;tès;ፈተና;provë;" +
                "පරීක්ෂණය;prøve;ꯆꯥꯡꯌꯦꯡ;امتحان;փորձարկում;" +
                "পৰীক্ষা;kɔrɔbɔli;စမ်းသပ်;परीक्षा;Bài kiểm tra;δοκιμή;" +
                "ტესტი;પરીક્ષણ;परख;מִבְחָן;פּרובירן;scrúdú;ٽيسٽpróf;" +
                "сынақ;ಪರೀಕ್ಷೆ;測試;测试;चाचणी;تاقیکردنەوە;សាកល្បង;テスト;" +
                "pārbaude;परीक्षण;പരീക്ഷ;އިމްތިޙާން;;ꯆꯥꯡꯌꯦꯡ;prüfen;ପରୀକ୍ଷା;" +
                "ਟੈਸਟ;تست;పరీక్ష;ازموینه;ทดสอบ;சோதனை;" +
                "ፈተና;Ölçek;پرکھ;sɔhwɛ;dodokpɔ;;"
    }

    @Test
    fun `test reading file lines with small buffer (utf8, new-line-symbol)`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-1-", ".xxx")
        val content = (1..42).map { Random.nextLong() }.sorted().map { it.toString() }

        file.writeText(content.joinToString("\n"))
        val areaSize = AtomicLong(file.fileSize())

        val res = Files.newByteChannel(file, StandardOpenOption.READ).use {
            it.readLines(areaSize, ByteBuffer.allocate(42)).toList()
        }

        Assertions.assertEquals(0, areaSize.get())
        Assertions.assertEquals(content, res.reversed())
    }

    @Test
    fun `test reading file lines with small buffer (utf32le, semicolon)`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-2-", ".xxx")
        file.writeText(testData, charset = Charsets.UTF_32LE)
        val areaSize = AtomicLong(file.fileSize())
        val res = Files.newByteChannel(file, StandardOpenOption.READ).use {
            it.readLines(
                segmentSize = areaSize,
                buffer = ByteBuffer.allocate(42),
                charset = Charsets.UTF_32LE,
                delimiter = ";",
            ).toList()
        }
        Assertions.assertEquals(0, areaSize.get())
        Assertions.assertEquals(testData.split(";"), res.reversed())
    }

    @Test
    fun `test reading file lines with big buffer (utf8, new-line-symbol)`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-3-", ".xxx")
        val content = (1..42).map { Random.nextLong() }.map { it.toString() }

        file.writeText(content.joinToString("\n"), charset = Charsets.UTF_8)
        val areaSize = AtomicLong(file.fileSize())

        val res = Files.newByteChannel(file, StandardOpenOption.READ).use {
            it.readLines(
                segmentSize = areaSize,
                buffer = ByteBuffer.allocate(424242),
                delimiter = "\n",
                charset = Charsets.UTF_8,
            ).toList()
        }

        Assertions.assertEquals(0, areaSize.get())
        Assertions.assertEquals(content, res.reversed())
    }

    @Test
    fun `test reading file lines with big buffer (utf16be, semicolon)`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-4-", ".xxx")
        file.writeText(testData, charset = Charsets.UTF_16BE)
        val areaSize = AtomicLong(file.fileSize())

        val res = Files.newByteChannel(file, StandardOpenOption.READ).use {
            it.readLines(
                segmentSize = areaSize,
                buffer = ByteBuffer.allocate(424242),
                delimiter = ";",
                charset = Charsets.UTF_16BE,
            ).toList()
        }

        Assertions.assertEquals(0, areaSize.get())
        Assertions.assertEquals(testData.split(";"), res.reversed())
    }

    @Test
    fun `test closed channel exception`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-4-", ".xxx")
        Assertions.assertThrows(ClosedChannelException::class.java) {
            Files.newByteChannel(file, StandardOpenOption.READ).use {
                it.readLines(AtomicLong(42), ByteBuffer.allocate(4))
            }.toList()
        }
    }
}