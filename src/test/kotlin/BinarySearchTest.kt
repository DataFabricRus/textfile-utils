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
        Assertions.assertEquals(setOf("**", "**", "**", "**", "**", "**"), res.map { it.toString(Charsets.UTF_8) }.toSet())
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
        Assertions.assertEquals(listOf("**", "**", "**", "**", "**", "**", "**"), res.map { it.toString(Charsets.UTF_8) })
    }
}