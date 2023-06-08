package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

internal class ArrayUtilsTest {

    @Test
    fun `test find-line-near-position #1-a`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9]
        // [ 32,  38,  38,  32, 105, 105,  32,  38,  38,  32]
        // [   ,   &,   &,    ,   i,   i,    ,   &,   &,    ]
        val txt = " && ii && "
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = " && ".toByteArray(Charsets.UTF_8)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size / 2,
            delimiter = delimiter,
        )
        Assertions.assertEquals(4, res?.second)
        Assertions.assertEquals("ii", res?.first?.toString(Charsets.UTF_8))
    }

    @Test
    fun `test find-line-near-position #1-b`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9]
        // [ 32,  38,  38,  32, 105, 105,  32,  38,  38,  32]
        // [   ,   &,   &,    ,   i,   i,    ,   &,   &,    ]
        val charset = Charsets.ISO_8859_1
        val txt = " && ii && "
        val source = txt.toByteArray(charset)
        val delimiter = "&&".toByteArray(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals(" ii ", res?.first?.toString(charset))
        Assertions.assertEquals(3, res?.second)
    }

    @Test
    fun `test find-line-near-position #1-c`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9]
        // [ 32,  38,  38,  32, 105, 105,  32,  38,  38,  32]
        // [   ,   &,   &,    ,   i,   i,    ,   &,   &,    ]
        val charset = Charsets.US_ASCII
        val txt = " && ii && "
        val source = txt.toByteArray(charset)
        val delimiter = " ++++ ".toByteArray(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(" && ii && ", res?.first?.toString(charset))
        Assertions.assertEquals(0, res?.second)
    }

    @Test
    fun `test find-line-near-position #1-d`() {
        val charset = Charsets.UTF_32LE
        val txt = " && ii && "
        val source = txt.toByteArray(charset)
        val delimiter = " ++++ ".toByteArray(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertNull(res)
    }

    @Test
    fun `test find-line-near-position #2-a`() {
        val txt = "xxx qqq"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = " ".toByteArray(Charsets.UTF_8)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 5,
            position = 2,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = false,
        )
        Assertions.assertEquals("xxx", res?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(0, res?.second)
    }

    @Test
    fun `test find-line-near-position #3-a`() {
        val txt = ";jjj;xxx;c;42;qqqq;www;ii,424242,mmm;;vvvv"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = ";".toByteArray(Charsets.UTF_8)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals("www", res?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(19, res?.second)
    }

    @Test
    fun `test find-line-near-position #3-b`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  ...
        // [ 59, 106, 106, 106,  59, 120, 120, 120,  59,  99,  59,  52,  50,  59, 113, 113, 113, 113,  59,  ...
        // [  ;,   j,   j,   j,   ;,   x,   x,   x,   ;,   c,   ;,   4,   2,   ;,   q,   q,   q,   q,   ;,  ...
        val txt = ";jjj;xxx;c;42;qqqq;www;ii,424242,mmm;;vvvv"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = ";".toByteArray(Charsets.UTF_8)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = 0,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals("jjj", res?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(1, res?.second)
    }

    @Test
    fun `test find-line-near-position #3-c`() {
        val txt = ";jjj;xxx;c;42;qqqq;www;ii,424242,mmm;;vvvv"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = ";".toByteArray(Charsets.UTF_8)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size - 1,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals("vvvv", res?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(38, res?.second)
    }

    @Test
    fun `test find-line-near-position #3-d`() {
        //                      p
        // ...  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41]
        // ...  59, 119, 119, 119,  59, 105, 105,  44,  52,  50,  52,  50,  52,  50,  44, 109, 109, 109,  59,  59, 118, 118, 118, 118]
        // ...   ;,   w,   w,   w,   ;,   i,   i,   ,,   4,   2,   4,   2,   4,   2,   ,,   m,   m,   m,   ;,   ;,   v,   v,   v,   v]
        val txt = ";jjj;xxx;c;42;qqqq;www;ii,424242,mmm;;vvvv"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = ";".toByteArray(Charsets.UTF_8)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = source.size / 2,
            sourceEndExclusive = source.size,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = true,
        )
        Assertions.assertEquals("ii,424242,mmm", res?.first?.toString(Charsets.UTF_8))
    }

    @Test
    fun `test find-line-near-position #3-e`() {
        val txt = ";jjj;xxx;c;42;qqqq;www;ii,424242,mmm;;vvvv"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = ";".toByteArray(Charsets.UTF_8)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = source.size / 2,
            sourceEndExclusive = source.size / 2 + 1,
            position = source.size / 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertNull(res)
    }

    @Test
    fun `test find-line-near-position #3-f`() {
        // ...   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22, ...
        // ...  59,  99,  59,  52,  50,  59, 113, 113, 113, 113,  59, 119, 119, 119,  59, ...
        // ...   ;,   c,   ;,   4,   2,   ;,   q,   q,   q,   q,   ;,   w,   w,   w,   ;, ...
        val txt = ";jjj;xxx;c;42;qqqq;www;ii,424242,mmm;;vvvv"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = ";".toByteArray(Charsets.UTF_8)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 14,
            sourceEndExclusive = 15,
            position = 14,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals("q", res?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(14, res?.second)
    }

    @Test
    fun `test find-line-near-position #3-g`() {
        val txt = ";jjj;xxx;c;42;qqqq;www;ii,424242,mmm;;vvvv"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = ";".toByteArray(Charsets.UTF_8)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size / 3 + 1,
            position = 0,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals("jjj", res?.first?.toString(Charsets.UTF_8))
        Assertions.assertEquals(1, res?.second)
    }

    @Test
    fun `test find-line-near-position #3-h`() {
        val txt = ";jjj;xxx;c;42;qqqq;www;ii,424242,mmm;;vvvv"
        val source = txt.toByteArray(Charsets.UTF_8)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = source.size / 3,
            delimiter = "|||".toByteArray(Charsets.UTF_8),
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertArrayEquals(source, res?.first)
        Assertions.assertEquals(0, res?.second)
    }

    @Test
    fun `test find-line-near-position #4-a`() {
        val charset = Charsets.UTF_16
        val source = TEST_DATA_1.toByteArray(charset)
        val delimiter = ";".bytes(charset)

        // ;ٽيسٽpróf;сынақ;ಪರೀಕ್ಷೆ;
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
    }

    @Test
    fun `test find-line-near-position #4-b`() {
        val charset = Charsets.UTF_16
        val source = TEST_DATA_1.toByteArray(charset)
        val delimiter = ";".bytes(charset)

        // end: ;sɔhwɛ;dodokpɔ;;
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = source.size / 2,
            sourceEndExclusive = source.size,
            position = 716,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals("", res?.first?.toString(charset))
        Assertions.assertEquals(716, res?.second)
    }

    @Test
    fun `test find-line-near-position #4-c`() {
        val charset = Charsets.UTF_16
        val source = TEST_DATA_1.toByteArray(charset)
        val delimiter = ";".bytes(charset)

        // 42: ;测试;चाचणी;تاقیکردنەوە;
        val res3 = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = 42,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        val actualString = res3?.first?.toString(charset)
        val actualPosition = res3?.second
        Assertions.assertEquals("tɛst", actualString) { "Wrong line near 42: $actualString" }
        Assertions.assertEquals(38, actualPosition)
    }

    @Test
    fun `test find-line-near-position #4-d`() {
        val charset = Charsets.UTF_16
        val source = TEST_DATA_1.toByteArray(charset)
        val delimiter = ";".bytes(charset)

        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = 424,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        val actualString = res?.first?.toString(charset)
        val actualPosition = res?.second
        Assertions.assertEquals("تاقیکردنەوە", actualString) { "Wrong line near 424: $actualString" }
        Assertions.assertEquals(422, actualPosition)
    }

    @Test
    fun `test find-line-near-position #4-e`() {
        val charset = Charsets.UTF_8
        val source = TEST_DATA_1.toByteArray(charset)
        val delimiter = ";".bytes(charset)

        // 542: ;പരീക്ഷ;އިމްތިޙާން;;
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            position = 542,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        val actualString = res?.first?.toString(charset)
        val actualPosition = res?.second
        Assertions.assertEquals("އިމްތިޙާން", actualString) { "Wrong line near 542: $actualString" }
        Assertions.assertEquals(537, actualPosition)
    }

    @Test
    fun `test find-line-near-position #5-a`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 95,  95,  84,  69,  83,  84,  95,  95,  68,  65,  84,  65,  95,  88,  95,  95]
        // [  _,   _,   T,   E,   S,   T,   _,   _,   D,   A,   T,   A,   _,   X,   _,   _]
        val charset = Charsets.UTF_8
        val txt = "__TEST__DATA_X__"
        val source = txt.toByteArray(charset)
        val delimiter = "_".bytes(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 16,
            position = 14,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        val actualString = res?.first?.toString(charset)
        val actualPosition = res?.second
        Assertions.assertEquals("X", actualString) { "Wrong line near 14: $actualString" }
        Assertions.assertEquals(13, actualPosition)
    }

    @Test
    fun `test find-line-near-position #5-b`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 95,  95,  84,  69,  83,  84,  95,  95,  68,  65,  84,  65,  95,  88,  95,  95]
        // [  _,   _,   T,   E,   S,   T,   _,   _,   D,   A,   T,   A,   _,   X,   _,   _]
        val charset = Charsets.UTF_8
        val txt = "__TEST__DATA_X__"
        val source = txt.toByteArray(charset)
        val delimiter = "_".bytes(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 13,
            position = 12,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        val actualString = res?.first?.toString(charset)
        val actualPosition = res?.second
        Assertions.assertEquals("DATA", actualString) { "Wrong line near 12: $actualString" }
        Assertions.assertEquals(8, actualPosition)
    }

    @Test
    fun `test find-line-near-position #5-c`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 95,  95,  84,  69,  83,  84,  95,  95,  68,  65,  84,  65,  95,  88,  95,  95]
        // [  _,   _,   T,   E,   S,   T,   _,   _,   D,   A,   T,   A,   _,   X,   _,   _]
        val charset = Charsets.UTF_8
        val txt = "__TEST__DATA_X__"
        val source = txt.toByteArray(charset)
        val delimiter = "_".bytes(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 16,
            position = 7,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        val actualString = res?.first?.toString(charset)
        val actualPosition = res?.second
        Assertions.assertEquals("", actualString) { "Wrong line near 7: $actualString" }
        Assertions.assertEquals(7, actualPosition)
    }

    @Test
    fun `test find-line-near-position #5-d`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 95,  95,  84,  69,  83,  84,  95,  95,  68,  65,  84,  65,  95,  88,  95,  95]
        // [  _,   _,   T,   E,   S,   T,   _,   _,   D,   A,   T,   A,   _,   X,   _,   _]
        val charset = Charsets.UTF_8
        val txt = "__TEST__DATA_X__"
        val source = txt.toByteArray(charset)
        val delimiter = "_".bytes(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 16,
            position = 15,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        val actualString = res?.first?.toString(charset)
        val actualPosition = res?.second
        Assertions.assertEquals("", actualString) { "Wrong line near 15: $actualString" }
        Assertions.assertEquals(15, actualPosition)
    }

    @Test
    fun `test find-line-near-position #6-a`() {
        // [  0,   1,   2,   3,   4,   5,   6]
        // [ 95,  88,  95,  84,  69,  83,  84]
        // [  _,   X,   _,   T,   E,   S,   T]
        val charset = Charsets.UTF_8
        val txt = "_X_TEST"
        val source = txt.toByteArray(charset)
        val delimiter = "_".bytes(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 7,
            position = 0,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )
        val actualString = res?.first?.toString(charset)
        val actualPosition = res?.second
        Assertions.assertEquals("X", actualString) { "Wrong line near 0: $actualString" }
        Assertions.assertEquals(1, actualPosition)
    }

    @Test
    fun `test find-line-near-position #7-a`() {
        // [  0,   1]
        // [124, 124]
        // [  |,   |]
        val txt = "||"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = "|".toByteArray(Charsets.UTF_8)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 1,
            position = 1,
            sourceEndExclusive = 2,
            delimiter = delimiter,
        )
        Assertions.assertNull(res) { "found: ${res?.second} :: '${res?.first?.toString(Charsets.UTF_8)}'" }
    }

    @Test
    fun `test find-line-near-position #7-b`() {
        // [  0,   1]
        // [124, 124]
        // [  |,   |]
        val txt = "||"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = "|".toByteArray(Charsets.UTF_8)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            position = 0,
            sourceEndExclusive = 2,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        val actualString = res?.first?.toString(Charsets.UTF_8)
        val actualPosition = res?.second
        Assertions.assertEquals("", actualString) { "Wrong line near 1: $actualString" }
        Assertions.assertEquals(1, actualPosition)
    }

    @Test
    fun `test find-line-near-position #8-a`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16]
        // [124, 124, 107, 124, 114, 124, 124, 114, 124, 114, 124, 113, 124, 113, 124, 124, 124]
        // [  |,   |,   k,   |,   r,   |,   |,   r,   |,   r,   |,   q,   |,   q,   |,   |,   |]
        val txt = "||k|r||r|r|q|q|||"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = "|".toByteArray(Charsets.UTF_8)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 1,
            position = 1,
            sourceEndExclusive = 2,
            delimiter = delimiter,
        )
        Assertions.assertNull(res)
    }

    @Test
    fun `test find-lines-block #8-b`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16]
        // [124, 124, 107, 124, 114, 124, 124, 114, 124, 114, 124, 113, 124, 113, 124, 124, 124]
        // [  |,   |,   k,   |,   r,   |,   |,   r,   |,   r,   |,   q,   |,   q,   |,   |,   |]
        val txt = "||k|r||r|r|q|q|||"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = "|".toByteArray(Charsets.UTF_8)
        val line = "".toByteArray(Charsets.UTF_8) to 0
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 17,
            delimiter = delimiter,
        )

        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf(""), actualLines)
        Assertions.assertEquals(1, actualStartPosition)
        Assertions.assertEquals(2, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #8-c`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16]
        // [124, 124, 107, 124, 114, 124, 124, 114, 124, 114, 124, 113, 124, 113, 124, 124, 124]
        // [  |,   |,   k,   |,   r,   |,   |,   r,   |,   r,   |,   q,   |,   q,   |,   |,   |]
        val txt = "||k|r||r|r|q|q|||"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = "|".toByteArray(Charsets.UTF_8)
        val line = "k".toByteArray(Charsets.UTF_8) to 2
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 17,
            delimiter = delimiter,
        )

        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("k"), actualLines)
        Assertions.assertEquals(2, actualStartPosition)
        Assertions.assertEquals(3, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #1-a`() {
        // ... 57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81, ...
        // ... 38,  32, 105,  32,  38,  38,  32, 105, 105,  32,  38,  38,  32, 105, 105,  32,  38,  38,  32, 105, 105,  32,  38,  38,  32, 107, ...
        // ...  &,    ,   i,    ,   &,   &,    ,   i,   i,    ,   &,   &,    ,   i,   i,    ,   &,   &,    ,   i,   i,    ,   &,   &,    ,   k, ...
        val charset = Charsets.UTF_8
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        sequenceOf(64, 70, 76).forEach {
            val line = "ii".toByteArray(charset) to it
            val res = findLineBlock(
                line = line,
                source = ByteBuffer.wrap(source),
                sourceStartInclusive = 0,
                sourceEndExclusive = source.size,
                delimiter = delimiter,
            )
            val actualLines = res.lines(charset)
            val actualStartPosition = res.startInclusive
            val actualEndPosition = res.endExclusive
            Assertions.assertEquals(listOf("ii", "ii", "ii"), actualLines) { "Wrong result for position $it"}
            Assertions.assertEquals(64, actualStartPosition)
            Assertions.assertEquals(78, actualEndPosition)
        }
    }

    @Test
    fun `test find-lines-block #1-b`() {
        // [   0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21, ...
        // [   0,  97,   0,  32,   0,  38,   0,  38,   0,  32,   0,  97,   0,  97,   0,  32,   0,  38,   0,  38,   0,  32, ...
        // [        a,         ,        &,        &,         ,        a,        a,         ,        &,        &,         , ...

        val charset = Charsets.UTF_16
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val source = txt.bytes(charset)
        val delimiter = " && ".bytes(charset)

        val line = "aa".bytes(charset) to 10
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
        )
        val actualLines = res.lines(charset)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("aa"), actualLines)
        Assertions.assertEquals(10, actualStartPosition)
        Assertions.assertEquals(14, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #1-c`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  ...
        // [ 97,  32,  38,  38,  32,  97,  97,  32,  38,  38,  32,  ...
        // [  a,    ,   &,   &,    ,   a,   a,    ,   &,   &,    ,  ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"
        val charset = Charsets.UTF_8

        val source = txt.toByteArray(charset)
        val delimiter = " ;; ".toByteArray(charset)
        val line = "aa".toByteArray(charset) to 5
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
        )
        Assertions.assertEquals(Lines.NULL, res)
    }

    @Test
    fun `test find-lines-block #1-d`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  ...
        // [ 97,  32,  38,  38,  32,  97,  97,  32,  38,  38,  32,  ...
        // [  a,    ,   &,   &,    ,   a,   a,    ,   &,   &,    ,  ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"
        val charset = Charsets.US_ASCII

        val source = txt.toByteArray(charset)
        val delimiter = " ".toByteArray(charset)
        val line = "AA".toByteArray(charset) to 5
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals(Lines.NULL, res)
    }

    @Test
    fun `test find-lines-block #1-g`() {
        //  ... 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147]
        //  ... 119,  32,  38,  38,  32, 120,  32,  38,  38,  32, 120, 120,  32,  38,  38,  32, 120, 120]
        //  ..... w,    ,   &,   &,    ,   x,    ,   &,   &,    ,   x,   x,    ,   &,   &,    ,   x,   x]
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = " && ".toByteArray(Charsets.UTF_8)
        val line = "xx".toByteArray(Charsets.UTF_8) to 146
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 142,
            sourceEndExclusive = 148,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = true,
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("xx"), actualLines)
        Assertions.assertEquals(146, actualStartPosition)
        Assertions.assertEquals(148, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #1-h`() {
        //  ... 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147]
        //  ... 119,  32,  38,  38,  32, 120,  32,  38,  38,  32, 120, 120,  32,  38,  38,  32, 120, 120]
        //  ..... w,    ,   &,   &,    ,   x,    ,   &,   &,    ,   x,   x,    ,   &,   &,    ,   x,   x]
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = " && ".toByteArray(Charsets.UTF_8)
        val line = "xx".toByteArray(Charsets.UTF_8) to 146
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 133,
            sourceEndExclusive = 148,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals(Lines.NULL, res)
    }

    @Test
    fun `test find-lines-block #1-k`() {
        //  ... 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147]
        //  ... 119,  32,  38,  38,  32, 120,  32,  38,  38,  32, 120, 120,  32,  38,  38,  32, 120, 120]
        //  ..... w,    ,   &,   &,    ,   x,    ,   &,   &,    ,   x,   x,    ,   &,   &,    ,   x,   x]
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = " && ".toByteArray(Charsets.UTF_8)
        val line = "xx".toByteArray(Charsets.UTF_8) to 146
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 133,
            sourceEndExclusive = 148,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = true,
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("xx", "xx"), actualLines)
        Assertions.assertEquals(140, actualStartPosition)
        Assertions.assertEquals(148, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #1-l`() {
        // ...   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31, ...
        // ...  97,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  99, ...
        // ...   a,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   c, ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"
        val source = txt.toByteArray(Charsets.UTF_8)
        val delimiter = " && ".toByteArray(Charsets.UTF_8)
        val line = "b".toByteArray(Charsets.UTF_8) to 11
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 26,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = true,
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("b", "b", "b"), actualLines)
        Assertions.assertEquals(11, actualStartPosition)
        Assertions.assertEquals(22, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #2-a`() {
        val source = TEST_DATA_1.toByteArray(Charsets.UTF_8)
        val delimiter = ";".toByteArray(Charsets.UTF_8)
        val line = "ტესტი".toByteArray(Charsets.UTF_8) to 265
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 133,
            sourceEndExclusive = 342,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("ტესტი"), actualLines)
        Assertions.assertEquals(265, actualStartPosition)
        Assertions.assertEquals(280, actualEndPosition)
    }
}