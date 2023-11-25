package cc.datafabric.textfileutils

import cc.datafabric.textfileutils.files.binarySearch
import cc.datafabric.textfileutils.files.readLeftLines
import cc.datafabric.textfileutils.files.readRightLines
import cc.datafabric.textfileutils.files.use
import cc.datafabric.textfileutils.iterators.byteArrayCachedStringComparator
import cc.datafabric.textfileutils.iterators.byteArrayPrefixSimpleComparator
import cc.datafabric.textfileutils.iterators.toByteArrayComparator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.TimeUnit
import kotlin.io.path.useLines
import kotlin.io.path.writeText

@Timeout(value = 2, unit = TimeUnit.MINUTES)
internal class BinarySearchTest {

    @Test
    fun `test binary-search small file #1`(@TempDir dir: Path) {
        // 42 bytes per line, 515 bytes per text
        val txt = """
            433e7ff4-f3ae-4432-8e31-e3d0d8601780:001:A
            b6b65cc6-1584-41c3-af01-42eacf18623d:002:A
            b6b65cc6-1584-41c3-af01-42eacf18623d:003:B
            433e7ff4-f3ae-4432-8e31-e3d0d8601780:004:B
            3466935b-cb0d-4586-81c6-cd5b82c8922c:005:B
            433e7ff4-f3ae-4432-8e31-e3d0d8601780:006:C
            543dc027-19f8-48e4-9e82-3c0413b91d90:007:C
            433e7ff4-f3ae-4432-8e31-e3d0d8601780:008:D
            d7411949-bc08-443e-b8fa-997187d6f73e:009:E
            3466935b-cb0d-4586-81c6-cd5b82c8922c:010:F
            3466935b-cb0d-4586-81c6-cd5b82c8922c:011:F
            f937a264-abef-4d3e-ad86-90c0a0d85e7a:012:G
        """.trimIndent()

        val charset = Charsets.UTF_8
        val delimiter = "\n"
        val comparator = Comparator<String> { left, right ->
            val a = left.substringAfterLast(":")
            val b = right.substringAfterLast(":")
            a.compareTo(b)
        }

        val file = Files.createTempFile("xxx-binary-search-", ".xxx")
        file.writeText(txt, charset)

        val resA = file.use {
            it.binarySearch(
                searchLine = "aaa:A".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(92),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(0, resA.first)
        Assertions.assertEquals(
            listOf(
                "433e7ff4-f3ae-4432-8e31-e3d0d8601780:001:A",
                "b6b65cc6-1584-41c3-af01-42eacf18623d:002:A",
            ),
            resA.second
        )

        val resB = file.use {
            it.binarySearch(
                searchLine = "x:B".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(92),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(86, resB.first)
        Assertions.assertEquals(
            listOf(
                "b6b65cc6-1584-41c3-af01-42eacf18623d:003:B",
                "433e7ff4-f3ae-4432-8e31-e3d0d8601780:004:B",
                "3466935b-cb0d-4586-81c6-cd5b82c8922c:005:B",
            ),
            resB.second
        )

        val resC = file.use {
            it.binarySearch(
                searchLine = "433e7ff4-f3ae-4432-8e31-e3d0d8601780:004:C".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(92),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(215, resC.first)
        Assertions.assertEquals(
            listOf(
                "433e7ff4-f3ae-4432-8e31-e3d0d8601780:006:C",
                "543dc027-19f8-48e4-9e82-3c0413b91d90:007:C",
            ),
            resC.second
        )

        val resD = file.use {
            it.binarySearch(
                searchLine = "00000000-1111-2222-3333-444444444444:XXX:D".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(94),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 46,
            )
        }
        Assertions.assertEquals(301, resD.first)
        Assertions.assertEquals(
            listOf(
                "433e7ff4-f3ae-4432-8e31-e3d0d8601780:008:D",
            ),
            resD.second
        )

        val resE = file.use {
            it.binarySearch(
                searchLine = ":E".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(92),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(344, resE.first)
        Assertions.assertEquals(
            listOf(
                "d7411949-bc08-443e-b8fa-997187d6f73e:009:E",
            ),
            resE.second
        )

        val resF = file.use {
            it.binarySearch(
                searchLine = ":F".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(92),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(387, resF.first)
        Assertions.assertEquals(
            listOf(
                "3466935b-cb0d-4586-81c6-cd5b82c8922c:010:F",
                "3466935b-cb0d-4586-81c6-cd5b82c8922c:011:F",
            ),
            resF.second
        )

        val resG = file.use {
            it.binarySearch(
                searchLine = ":G".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(100),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 51,
                maxLineLengthInBytes = 49
            )
        }
        Assertions.assertEquals(473, resG.first)
        Assertions.assertEquals(
            listOf(
                "f937a264-abef-4d3e-ad86-90c0a0d85e7a:012:G",
            ),
            resG.second
        )
    }

    @Test
    fun `test binary-search small file #2`(@TempDir dir: Path) {
        val b =
            "#_000347b5-eec1-4a65-9f4b-bbe4289bf51c|5e7e3338-1d34-4472-848a-cf44727afafd".toByteArray(Charsets.UTF_8).size
        println(b)
        println("\n".toByteArray(Charsets.UTF_8).size)
        val content = """
            #_000347b5-eec1-4a65-9f4b-bbe4289bf51c|Q
            #_001e9f01-ed5d-44e2-ae55-4193e64de640|W
            #_00225668-5bfe-480a-b09a-8afec80f59a4|E
            #_02f6f151-064f-4337-9171-a6f14d50ddab|R
        """.trimIndent()

        val source = Files.createTempFile("xxx-binary-search-", ".xxx")
        source.writeText(content)

        val fileChannel = Files.newByteChannel(source, StandardOpenOption.READ)
        fileChannel.use { channel ->
            content.split("\n").forEach { line ->
                val key = line.split("|")[0]
                val (_, lines) = channel.binarySearch(
                    searchLine = key.toByteArray(Charsets.UTF_8),
                    delimiter = "\n".toByteArray(Charsets.UTF_8),
                    comparator = byteArrayPrefixSimpleComparator("|"),
                    maxLineLengthInBytes = 42,
                    buffer = ByteBuffer.allocateDirect(100),
                )
                Assertions.assertEquals(listOf(line), lines.map { it.toString(Charsets.UTF_8) })
            }
        }
    }

    @Test
    fun `test binary-search large file`(@TempDir dir: Path) {
        val charset = Charsets.UTF_8
        val content =
            MergeSortTest::class.java.getResourceAsStream("/sorted.csv")!!.bufferedReader(charset).readText()

        val source = Files.createTempFile(dir, "xxx-binary-search-", ".xxx")
        source.writeText(content, charset)

        val comparator = Comparator<String> { left, right ->
            val a = left.substringBefore(":")
            val b = right.substringBefore(":")
            a.compareTo(b)
        }

        source.useLines { lines ->
            lines.forEach { line ->
                val searchString = line.substringBefore(":")
                val expectedBlockSize = line.substringAfter(":").substringBefore(":").toInt()
                val found = binarySearch(
                    source = source,
                    searchLine = searchString,
                    comparator = comparator,
                )
                Assertions.assertEquals(expectedBlockSize, found.second.size)
                found.second.forEach {
                    Assertions.assertTrue(it.length > searchString.length)
                    Assertions.assertTrue(it.startsWith("$searchString:"))
                }
            }
        }
    }

    @Test
    fun `test binary-search sorted small file with empty lines at the end`(@TempDir dir: Path) {
        val charset = Charsets.UTF_8
        val content = """
            #_ffdf27f2-9acf-4a39-9c9d-66aa77b37ba3|A
            #_ffdf5659-17d3-4de2-9397-7fbef06ed785|B
            #_ffe4adb7-3e1c-435c-ab1f-11d58b78e227|C
            #_ffe4d7f7-fe0f-4788-931e-99c09a588a5c|D
            #_fffa83e1-6dd7-41c7-8878-415dd02068ba|E
            
            
        """.trimIndent()

        val source = Files.createTempFile(dir, "xxx-binary-search-", ".xxx")
        source.writeText(content, charset)

        val fileChannel = Files.newByteChannel(source, StandardOpenOption.READ)
        fileChannel.use { channel ->

            val (n1, lines1) = channel.binarySearch(
                searchLine = "#_ffdf5659-17d3-4de2-9397-7fbef06ed785".toByteArray(Charsets.UTF_8),
                delimiter = "\n".toByteArray(Charsets.UTF_8),
                comparator = Comparator<String> { leftLine, rightLine ->
                    leftLine.substringBefore("|").compareTo(rightLine.substringBefore("|"))
                }.toByteArrayComparator(),
            )

            println("$n1::$lines1")
            Assertions.assertEquals(41, n1)
            Assertions.assertEquals(
                listOf("#_ffdf5659-17d3-4de2-9397-7fbef06ed785|B"),
                lines1.map { it.toString(charset) }
            )

            val (n2, lines2) =
                channel.binarySearch(
                    searchLine = "".toByteArray(Charsets.UTF_8),
                    delimiter = "\n".toByteArray(Charsets.UTF_8),
                    comparator = Comparator<String> { leftLine, rightLine ->
                        leftLine.substringBefore("|").compareTo(rightLine.substringBefore("|"))
                    }.toByteArrayComparator(),
                )
            Assertions.assertEquals(1, n2)
            Assertions.assertTrue(lines2.isEmpty())
        }
    }

    @Test
    fun `test read-left-lines`(@TempDir dir: Path) {
        // ...  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68, ...
        // ... 119, 124,  73,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124,  52, 124, ...
        // ...   w,   |,   I,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   4,   |, ...
        val txt = "|r|s|**|+|'|f|q|5|@Ko|HS|s|2|L|n|f|**|**|w|I*|**|**|**|**|**|**|**|4|2|k|L" // size = 74

        val file = Files.createTempFile("xxx-binary-search-", ".xxx")
        file.writeText(txt, Charsets.UTF_8)

        val delimiter = byteArrayOf(124)
        val readPosition = 64L
        val line = byteArrayOf(42, 42)
        val source = ByteBuffer.wrap(byteArrayOf(42, 124, 42, 42, 124, 52))
        val res = mutableListOf<ByteArray>()
        val index = file.use {
            it.position(readPosition)
            it.readLeftLines(
                readPosition = readPosition,
                buffer = source,
                searchLine = line,
                delimiter = delimiter,
                comparator = byteArrayCachedStringComparator(),
                maxOfLines = 42,
                maxLineLengthInBytes = 1024,
                res = res,
            )
        }
        Assertions.assertEquals(
            setOf("**", "**", "**", "**", "**", "**"),
            res.map { it.toString(Charsets.UTF_8) }.toSet()
        )
        Assertions.assertEquals(46, index)
    }

    @Test
    fun `test read-right-lines`(@TempDir dir: Path) {
        //  ...  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40, ...
        //  ... 124, 120,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124,  42,  42, 124, 120, ...
        //  ...   |,   x,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   *,   *,   |,   x, ...
        val txt = "x|x|x|x|**|**|x|x*|**|**|**|**|**|**|**|x|x|x|x|x" // size = 49
        val delimiter = byteArrayOf(124)
        val line = byteArrayOf(42, 42)

        val readPosition = 18L
        val source = ByteBuffer.wrap(byteArrayOf(-1, -1, -1, 42, 124))

        val file = Files.createTempFile("xxx-binary-search-", ".xxx")
        file.writeText(txt, Charsets.UTF_8)

        val res = mutableListOf<ByteArray>()
        file.use {
            it.position(readPosition)
            it.readRightLines(
                readPosition = readPosition,
                buffer = source,
                searchLine = line,
                delimiter = delimiter,
                comparator = byteArrayCachedStringComparator(),
                maxOfLines = 42,
                maxLineLengthInBytes = 1024,
                res = res,
            )
        }
        Assertions.assertEquals(
            listOf("**", "**", "**", "**", "**", "**", "**"),
            res.map { it.toString(Charsets.UTF_8) })
    }

    @Test
    fun `test binary-search not found`(@TempDir dir: Path) {
        val content = """
            #_000347b5-eec1-4a65-9f4b-bbe4289bf51c|xxx
            #_000a0e22-5e84-40c8-9fb2-ceec4fd1f187|vvv
            #_0019785d-df4a-4e21-a4c5-1569b9f41cb7|bbb
            #_001e9f01-ed5d-44e2-ae55-4193e64de640|mmm
            #_00225668-5bfe-480a-b09a-8afec80f59a4|aaa
        """.trimIndent()

        val source = Files.createTempFile("xxx-binary-search-", ".xxx")
        source.writeText(content)

        val fileChannel = Files.newByteChannel(source, StandardOpenOption.READ)
        val searchLine = "XXX"
        fileChannel.use { channel ->
            val (n, lines) = channel.binarySearch(
                searchLine = searchLine.toByteArray(Charsets.UTF_8),
                delimiter = "\n".toByteArray(Charsets.UTF_8),
                comparator = byteArrayPrefixSimpleComparator("|"),
            )
            Assertions.assertTrue(lines.isEmpty())
            Assertions.assertEquals(215, n)
        }
    }

    @Test
    fun `test binary-search exceed max length line error`(@TempDir dir: Path) {
        val b =
            "#_000347b5-eec1-4a65-9f4b-bbe4289bf51c|5e7e3338-1d34-4472-848a-cf44727afafd".toByteArray(Charsets.UTF_8).size
        println(b)
        println("\n".toByteArray(Charsets.UTF_8).size)
        val content = """
            #_000347b5-eec1-4a65-9f4b-bbe4289bf51c|5e7e3338-1d34-4472-848a-cf44727afafd
            #_001e9f01-ed5d-44e2-ae55-4193e64de640|b8248fce-c79c-4aea-96cb-2880e13b36e9
            #_00225668-5bfe-480a-b09a-8afec80f59a4|ce41ce78-e643-42b8-ab82-ce04c8ce4108
            #_02f6f151-064f-4337-9171-a6f14d50ddab|d74e98d0-8b51-4fda-b1a4-af7d80a06b07
        """.trimIndent()
        val key = "#_00225668-5bfe-480a-b09a-8afec80f59a4"

        val source = Files.createTempFile("xxx-binary-search-", ".xxx")
        source.writeText(content)

        val fileChannel = Files.newByteChannel(source, StandardOpenOption.READ)
        fileChannel.use { channel ->
            // buffer size  100 must be >= 2 * (max-line-length + delimiter-length) = 102
            Assertions.assertThrows(IllegalArgumentException::class.java) {
                channel.binarySearch(
                    searchLine = key.toByteArray(Charsets.UTF_8),
                    delimiter = "\n".toByteArray(Charsets.UTF_8),
                    comparator = byteArrayPrefixSimpleComparator("|"),
                    maxLineLengthInBytes = 50,
                    buffer = ByteBuffer.allocateDirect(100),
                )
            }
        }
    }

}