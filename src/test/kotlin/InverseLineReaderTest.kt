package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.charset.Charset
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
    fun `test reading file lines with small buffer (default encoding, comma and new-line-symbol)`(@TempDir dir: Path) {
        val file1 = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val given1 = listOf(-29876.0, -0.04242, 0.874242, 42.0, 1011.3333333333334)
        file1.writeText(given1.joinToString(","))
        val areaSize1 = AtomicLong(file1.fileSize())
        val actual1 = file1.use { channel ->
            channel.readLines(
                segmentSize = areaSize1.get(),
                listener = { areaSize1.set(it) },
                buffer = ByteBuffer.allocate(7),
                delimiter = ","
            ).map { it.toDouble() }.toList()
        }
        Assertions.assertEquals(0, areaSize1.get())
        Assertions.assertEquals(given1, actual1.reversed())

        val file2 = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val given2 = (1..42).map { Random.nextLong() }.sorted().map { it.toString() }
        file2.writeText(given2.joinToString("\n"))

        val areaSize2 = AtomicLong(file2.fileSize())
        val actual2 = file2.use { channel ->
            channel.readLines(
                segmentSize = areaSize2.get(),
                listener = { areaSize2.set(it) },
                buffer = ByteBuffer.allocate(7)
            ).toList()
        }
        Assertions.assertEquals(0, areaSize2.get())
        Assertions.assertEquals(given2, actual2.reversed())
    }

    @Test
    fun `test reading file lines with small buffer (utf32le, semicolon)`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        file.writeText(testData, charset = Charsets.UTF_32LE)
        val areaSize = AtomicLong(file.fileSize())
        val res = Files.newByteChannel(file, StandardOpenOption.READ).use { channel ->
            channel.readLines(
                segmentSize = areaSize.get(),
                listener = { areaSize.set(it) },
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
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val content = (1..42).map { Random.nextLong() }.map { it.toString() }

        file.writeText(content.joinToString("\n"), charset = Charsets.UTF_8)
        val areaSize = AtomicLong(file.fileSize())

        val res = Files.newByteChannel(file, StandardOpenOption.READ, StandardOpenOption.WRITE).use { channel ->
            channel.readLines(
                segmentSize = areaSize.get(),
                listener = { areaSize.set(it) },
                buffer = ByteBuffer.allocate(424242),
                delimiter = "\n",
                charset = Charsets.UTF_8,
            ).onEach { channel.truncate(areaSize.get()) }.toList()
        }

        Assertions.assertEquals(0, areaSize.get())
        Assertions.assertEquals(content, res.reversed())
        Assertions.assertEquals(0, file.fileSize())
    }

    @Test
    fun `test reading file lines with big buffer (utf16be, semicolon)`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        file.writeText(testData, charset = Charsets.UTF_16BE)
        val areaSize = AtomicLong(file.fileSize())

        val res = Files.newByteChannel(file, StandardOpenOption.READ).use { channel ->
            channel.readLines(
                segmentSize = areaSize.get(),
                listener = { areaSize.set(it) },
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
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        Assertions.assertThrows(ClosedChannelException::class.java) {
            Files.newByteChannel(file, StandardOpenOption.READ).use {
                it.readLines(42, ByteBuffer.allocate(4))
            }.toList()
        }
    }

    @Test
    fun `test read empty file (small buffer, cp866)`(@TempDir dir: Path) {
        val charset = Charset.forName("CP866")
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val areaSize = AtomicLong(file.fileSize())

        val res = Files.newByteChannel(file, StandardOpenOption.READ).use { channel ->
            channel.readLines(
                segmentSize = areaSize.get(),
                listener = { areaSize.set(it) },
                buffer = ByteBuffer.allocate(4),
                delimiter = ";",
                charset = charset,
            ).toList()
        }

        Assertions.assertEquals(0, areaSize.get())
        Assertions.assertTrue(res.isEmpty()) { "Actual: $res" }
    }

    @Test
    fun `test read sparse file (small buffer, new-line, cp1251)`(@TempDir dir: Path) {
        testSparseFile(dir, "\n", Charset.forName("CP1251"), 4)
    }

    @Test
    fun `test read sparse file (big buffer, cp1251, delimiter)`(@TempDir dir: Path) {
        testSparseFile(dir, "|", Charset.forName("CP1251"), 424242)
    }

    private fun testSparseFile(dir: Path, delimiter: String, charset: Charset, bufferSize: Int) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val given =
            "${delimiter}${delimiter}${delimiter}${delimiter}а${delimiter}${delimiter}б${delimiter}ц${delimiter}${delimiter}"
        file.writeText(given, charset)
        val areaSize = AtomicLong(file.fileSize())

        val actual = Files.newByteChannel(file, StandardOpenOption.READ).use { channel ->
            channel.readLines(
                segmentSize = areaSize.get(),
                listener = { areaSize.set(it) },
                buffer = ByteBuffer.allocate(bufferSize),
                delimiter = delimiter,
                charset = charset,
            ).toList().reversed()
        }

        Assertions.assertEquals(0, areaSize.get())
        Assertions.assertEquals(given.split(delimiter), actual) { "Actual: $actual" }
        Assertions.assertEquals(given, actual.joinToString(delimiter)) { "Actual: $actual" }
    }

}