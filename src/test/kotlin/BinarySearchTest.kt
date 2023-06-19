package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.writeText

internal class BinarySearchTest {

    @Test
    fun `test binary-search small file`(@TempDir dir: Path) {
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
        val delimiter = "\n".toByteArray(charset)
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
                buffer = ByteBuffer.allocate(91),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(0, resA.first)
        Assertions.assertEquals(listOf(
            "433e7ff4-f3ae-4432-8e31-e3d0d8601780:001:A",
            "b6b65cc6-1584-41c3-af01-42eacf18623d:002:A",
        ), resA.second.map { it.toString(charset) })

        val resB = file.use {
            it.binarySearch(
                searchLine = "x:B".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(91),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(86, resB.first)
        Assertions.assertEquals(listOf(
            "b6b65cc6-1584-41c3-af01-42eacf18623d:003:B",
            "433e7ff4-f3ae-4432-8e31-e3d0d8601780:004:B",
            "3466935b-cb0d-4586-81c6-cd5b82c8922c:005:B",
        ), resB.second.map { it.toString(charset) })

        val resC = file.use {
            it.binarySearch(
                searchLine = "433e7ff4-f3ae-4432-8e31-e3d0d8601780:004:C".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(91),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(215, resC.first)
        Assertions.assertEquals(listOf(
            "433e7ff4-f3ae-4432-8e31-e3d0d8601780:006:C",
            "543dc027-19f8-48e4-9e82-3c0413b91d90:007:C",
        ), resC.second.map { it.toString(charset) })

        val resD = file.use {
            it.binarySearch(
                searchLine = "00000000-1111-2222-3333-444444444444:XXX:D".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(92),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 46,
            )
        }
        Assertions.assertEquals(301, resD.first)
        Assertions.assertEquals(listOf(
            "433e7ff4-f3ae-4432-8e31-e3d0d8601780:008:D",
        ), resD.second.map { it.toString(charset) })

        val resE = file.use {
            it.binarySearch(
                searchLine = ":E".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(91),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(344, resE.first)
        Assertions.assertEquals(listOf(
            "d7411949-bc08-443e-b8fa-997187d6f73e:009:E",
        ), resE.second.map { it.toString(charset) })

        val resF = file.use {
            it.binarySearch(
                searchLine = ":F".toByteArray(charset),
                startAreaInclusive = 0,
                endAreaExclusive = it.size(),
                buffer = ByteBuffer.allocate(91),
                charset = charset,
                delimiter = delimiter,
                comparator = comparator,
                maxOfLinesPerBlock = 5,
                maxLineLengthInBytes = 45
            )
        }
        Assertions.assertEquals(387, resF.first)
        Assertions.assertEquals(listOf(
            "3466935b-cb0d-4586-81c6-cd5b82c8922c:010:F",
            "3466935b-cb0d-4586-81c6-cd5b82c8922c:011:F",
        ), resF.second.map { it.toString(charset) })

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
        Assertions.assertEquals(listOf(
            "f937a264-abef-4d3e-ad86-90c0a0d85e7a:012:G",
        ), resG.second.map { it.toString(charset) })
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
                comparator = defaultByteArrayComparator(),
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
                comparator = defaultByteArrayComparator(),
                maxOfLines = 42,
                maxLineLengthInBytes = 1024,
                res = res,
            )
        }
        Assertions.assertEquals(
            listOf("**", "**", "**", "**", "**", "**", "**"),
            res.map { it.toString(Charsets.UTF_8) })
    }
}