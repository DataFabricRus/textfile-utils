package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

internal class ArrayUtilsTest {

    @Test
    fun `test find-line-near-position functionality, short string`() {
        val txt1 = ";jjj;xxx;c;42;qqqq;www;ii,424242,mmm;;vvvv"
        val source = txt1.toByteArray(Charsets.UTF_8)
        val delimiter = ";".toByteArray(Charsets.UTF_8)

        val res1 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals("www", res1?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(19, res1?.second)

        val res2 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = 0,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals("", res2?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(0, res2?.second)

        val res3 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size - 1,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals("vvvv", res3?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(38, res3?.second)

        val res4 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = source.size / 2,
            sourceEndExclusive = source.size,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = true,
        )
        Assertions.assertEquals("ii,424242,mmm", res4?.first?.toString(Charsets.UTF_8))

        val res5 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = source.size / 2,
            sourceEndExclusive = source.size / 2 + 1,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertNull(res5)

        val res6 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = source.size / 3,
            sourceEndExclusive = source.size / 3 + 1,
            position = source.size / 3,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(0, res6?.first?.size)
        Assertions.assertEquals(source.size / 3, res6?.second)

        val res7 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size / 3 + 1,
            position = 0,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals("jjj", res7?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(1, res7?.second)

        val res8 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size / 3,
            delimiter = "|||".toByteArray(Charsets.UTF_8),
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertArrayEquals(source, res8?.first)
        Assertions.assertEquals(0, res8?.second)

        val res9 = findLineNearPosition(
            source = ByteBuffer.wrap("xxx qqq".toByteArray(Charsets.UTF_8)),
            sourceStartInclusive = 0,
            sourceEndExclusive = 5,
            position = 2,
            delimiter = " ".toByteArray(Charsets.UTF_8),
            includeLeftBound = true,
            includeRightBound = false,
        )
        Assertions.assertEquals("xxx", res9?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(0, res9?.second)
    }

    @Test
    fun `test find-line-near-position functionality, big string`() {
        val charset = Charsets.UTF_16
        val source = TEST_DATA_1.toByteArray(charset)
        val delimiter = ";".bytes(charset)

        val res1 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = source.size / 2,
            sourceEndExclusive = source.size,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals("сынақ", res1?.first?.toString(charset))
        Assertions.assertEquals(370, res1?.second)

        val res2 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = source.size / 2,
            sourceEndExclusive = source.size,
            position = 716,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals("", res2?.first?.toString(charset))
        Assertions.assertEquals(716, res2?.second)

        mapOf(42 to "tɛst", 424 to "चाचणी", 521 to "އިމްތިޙާން").forEach { (p, t) ->
            val res3 = findLineNearPosition(
                source = ByteBuffer.wrap(source),
                sourceStartInclusive = 0,
                sourceEndExclusive = source.size,
                position = p,
                delimiter = delimiter,
                includeLeftBound = false,
                includeRightBound = false,
            )
            Assertions.assertEquals(t, res3?.first?.toString(charset))
        }
    }
}