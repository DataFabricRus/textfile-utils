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

internal class LineReaderTest {

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
    fun `test direct read (small buffer, utf16le, semicolon)`(@TempDir dir: Path) {
        testDirectRead(dir, Charsets.UTF_16LE, 4)
    }

    @Test
    fun `test direct read file lines(big buffer, utf16le, semicolon)`(@TempDir dir: Path) {
        testDirectRead(dir, Charsets.UTF_32, 424242)
    }

    @Test
    fun `test reverse read (small buffer, default encoding, comma and new-line-symbol)`(@TempDir dir: Path) {
        val file1 = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val given1 = listOf(-29876.0, -0.04242, 0.874242, 42.0, 1011.3333333333334)
        file1.writeText(given1.joinToString(","))
        val position1 = AtomicLong(file1.fileSize())
        val actual1 = file1.use { channel ->
            channel.readLines(
                direct = false,
                startPositionInclusive = 0,
                endPositionExclusive = position1.get(),
                listener = { position1.set(it) },
                buffer = ByteBuffer.allocate(7),
                delimiter = ","
            ).map { it.toDouble() }.toList()
        }
        Assertions.assertEquals(0, position1.get())
        Assertions.assertEquals(given1, actual1.reversed())

        val file2 = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val given2 = (1..42).map { Random.nextLong() }.sorted().map { it.toString() }
        file2.writeText(given2.joinToString("\n"))

        val position2 = AtomicLong(file2.fileSize())
        val actual2 = file2.use { channel ->
            channel.readLines(
                direct = false,
                startPositionInclusive = 0,
                endPositionExclusive = position2.get(),
                listener = { position2.set(it) },
                buffer = ByteBuffer.allocate(7)
            ).toList()
        }
        Assertions.assertEquals(0, position2.get())
        Assertions.assertEquals(given2, actual2.reversed())
    }

    @Test
    fun `test reverse read with (small buffer, utf32le, semicolon)`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        file.writeText(testData, charset = Charsets.UTF_32LE)
        val position = AtomicLong(file.fileSize())
        val res = Files.newByteChannel(file, StandardOpenOption.READ).use { channel ->
            channel.readLines(
                direct = false,
                startPositionInclusive = 0,
                endPositionExclusive = position.get(),
                listener = { position.set(it) },
                buffer = ByteBuffer.allocate(42),
                charset = Charsets.UTF_32LE,
                delimiter = ";",
            ).toList()
        }
        Assertions.assertEquals(0, position.get())
        Assertions.assertEquals(testData.split(";"), res.reversed())
    }

    @Test
    fun `test reverse read (big buffer, utf8, new-line-symbol)`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val content = (1..42).map { Random.nextLong() }.map { it.toString() }

        file.writeText(content.joinToString("\n"), charset = Charsets.UTF_8)
        val position = AtomicLong(file.fileSize())

        val res = Files.newByteChannel(file, StandardOpenOption.READ, StandardOpenOption.WRITE).use { channel ->
            channel.readLines(
                direct = false,
                startPositionInclusive = 0,
                endPositionExclusive = position.get(),
                listener = { position.set(it) },
                buffer = ByteBuffer.allocate(424242),
                delimiter = "\n",
                charset = Charsets.UTF_8,
            ).onEach { channel.truncate(position.get()) }.toList()
        }

        Assertions.assertEquals(0, position.get())
        Assertions.assertEquals(content, res.reversed())
        Assertions.assertEquals(0, file.fileSize())
    }

    @Test
    fun `test reverse read (big buffer, utf16be, semicolon)`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        file.writeText(testData, charset = Charsets.UTF_16BE)
        val position = AtomicLong(file.fileSize())

        val res = Files.newByteChannel(file, StandardOpenOption.READ).use { channel ->
            channel.readLines(
                direct = false,
                startPositionInclusive = 0,
                endPositionExclusive = position.get(),
                listener = { position.set(it) },
                buffer = ByteBuffer.allocate(424242),
                delimiter = ";",
                charset = Charsets.UTF_16BE,
            ).toList()
        }

        Assertions.assertEquals(0, position.get())
        Assertions.assertEquals(testData.split(";"), res.reversed())
    }

    @Test
    fun `test closed channel exception`(@TempDir dir: Path) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        file.writeText(testData)
        Assertions.assertThrows(ClosedChannelException::class.java) {
            Files.newByteChannel(file, StandardOpenOption.READ).use {
                it.readLines(
                    direct = false,
                    startPositionInclusive = 0,
                    endPositionExclusive = 42,
                    buffer = ByteBuffer.allocate(4)
                )
            }.toList()
        }
    }

    @Test
    fun `test reverse read empty file (small buffer, cp866)`(@TempDir dir: Path) {
        val charset = Charset.forName("CP866")
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val position = AtomicLong(file.fileSize())

        val res = Files.newByteChannel(file, StandardOpenOption.READ).use { channel ->
            channel.readLines(
                direct = false,
                startPositionInclusive = 0,
                endPositionExclusive = position.get(),
                listener = { position.set(it) },
                buffer = ByteBuffer.allocate(4),
                delimiter = ";",
                charset = charset,
            ).toList()
        }

        Assertions.assertEquals(0, position.get())
        Assertions.assertTrue(res.isEmpty()) { "Actual: $res" }
    }

    @Test
    fun `test reverse read sparse file (small buffer, new-line, cp1251)`(@TempDir dir: Path) {
        testReverseReadSparseFile(dir, "\n", Charset.forName("CP1251"), 4)
    }

    @Test
    fun `test reverse read sparse file (big buffer, cp1251, custom delimiter)`(@TempDir dir: Path) {
        testReverseReadSparseFile(dir, "|", Charset.forName("CP1251"), 424242)
    }

    @Test
    fun `test reverse read from region (small buffer, semicolon)`(@TempDir dir: Path) {
        testReadFromRegion(dir, 5, false)
    }

    @Test
    fun `test reverse read from region (big buffer, semicolon)`(@TempDir dir: Path) {
        testReadFromRegion(dir, 424242, false)
    }

    @Test
    fun `test direct read from region (small buffer, semicolon)`(@TempDir dir: Path) {
        testReadFromRegion(dir, 7, true)
    }

    @Test
    fun `test direct read from region (big buffer, semicolon)`(@TempDir dir: Path) {
        testReadFromRegion(dir, 42424, true)
    }

    private fun testReverseReadSparseFile(dir: Path, delimiter: String, charset: Charset, bufferSize: Int) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        val given =
            "${delimiter}${delimiter}${delimiter}${delimiter}а${delimiter}${delimiter}б${delimiter}ц${delimiter}${delimiter}"
        file.writeText(given, charset)
        val position = AtomicLong(file.fileSize())

        val actual = Files.newByteChannel(file, StandardOpenOption.READ).use { channel ->
            channel.readLines(
                direct = false,
                startPositionInclusive = 0,
                endPositionExclusive = position.get(),
                listener = { position.set(it) },
                buffer = ByteBuffer.allocate(bufferSize),
                delimiter = delimiter,
                charset = charset,
            ).toList().reversed()
        }

        Assertions.assertEquals(0, position.get())
        Assertions.assertEquals(given.split(delimiter), actual) { "Actual: $actual" }
        Assertions.assertEquals(given, actual.joinToString(delimiter)) { "Actual: $actual" }
    }

    private fun testReadFromRegion(dir: Path, bufferSize: Int, direct: Boolean) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        file.writeText(testData, Charsets.UTF_8)

        val position = AtomicLong(file.fileSize())
        val actual = file.use { channel ->
            channel.readLines(
                direct = direct,
                startPositionInclusive = 31,
                endPositionExclusive = 94,
                listener = { position.set(it) },
                buffer = ByteBuffer.allocate(bufferSize),
                delimiter = ";",
                charset = Charsets.UTF_8,
            ).toList()
        }
        Assertions.assertEquals(7, actual.size)
        Assertions.assertEquals(if (direct) 94 else 31, position.get())
        Assertions.assertEquals(
            "tîhan;tɛst;시험;tès;ፈተና;provë;පරීක්ෂණ",
            (if (direct) actual else actual.reversed()).joinToString(";")
        )
    }

    private fun testDirectRead(dir: Path, charset: Charset, bufferSize: Int) {
        val file = Files.createTempFile(dir, "inverse-line-reader-test-", ".xxx")
        file.writeText(testData, charset = charset)
        val position = AtomicLong(-1)
        val actual = Files.newByteChannel(file, StandardOpenOption.READ).use { channel ->
            channel.readLines(
                direct = true,
                startPositionInclusive = 0,
                endPositionExclusive = file.fileSize(),
                listener = { position.set(it) },
                buffer = ByteBuffer.allocate(bufferSize),
                charset = charset,
                delimiter = ";",
            ).toList()
        }
        Assertions.assertEquals(file.fileSize(), position.get())
        Assertions.assertEquals(testData.split(";"), actual)
    }
}