package com.gitlab.sszuev.textfiles

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

internal class ByteArrayUtilsTest {

    @Test
    fun `test left-index-of`() {
        val source = ByteBuffer.wrap(byteArrayOf(42, 2, 4, 24, 2, 4, 42))
        Assertions.assertEquals(
            4,
            source.lastIndexOf(1, 6, byteArrayOf(2, 4)),
        )
        Assertions.assertEquals(
            4,
            source.lastIndexOf(3, 7, byteArrayOf(2, 4)),
        )
        Assertions.assertEquals(
            1,
            source.lastIndexOf(1, 4, byteArrayOf(2, 4)),
        )
        Assertions.assertEquals(
            5,
            source.lastIndexOf(1, 7, byteArrayOf(4, 42)),
        )
        Assertions.assertEquals(
            0,
            source.lastIndexOf(0, 7, byteArrayOf(42, 2)),
        )
        Assertions.assertEquals(
            -1,
            source.lastIndexOf(2, 7, byteArrayOf(42, 2)),
        )
        Assertions.assertEquals(
            4,
            source.lastIndexOf(1, 7, byteArrayOf(2, 4, 42)),
        )
        Assertions.assertEquals(
            -1,
            source.lastIndexOf(0, 6, byteArrayOf(2, 4, 42)),
        )
        Assertions.assertEquals(
            -1,
            source.lastIndexOf(1, 5, byteArrayOf(2, 4, 42)),
        )
        Assertions.assertEquals(
            1,
            source.lastIndexOf(1, 5, byteArrayOf(2, 4, 24)),
        )
        Assertions.assertEquals(
            5,
            source.lastIndexOf(0, 6, byteArrayOf(4)),
        )
        Assertions.assertEquals(
            2,
            source.lastIndexOf(0, 4, byteArrayOf(4)),
        )
        Assertions.assertEquals(
            -1,
            source.lastIndexOf(0, 4, byteArrayOf(22)),
        )
        Assertions.assertEquals(
            -1,
            source.lastIndexOf(1, 7, byteArrayOf(2, 4, 42, 42)),
        )
    }

    @Test
    fun `test right-index-of`() {
        val source = ByteBuffer.wrap(byteArrayOf(4, 42, 2, 4, 42, 4))
        Assertions.assertEquals(
            0,
            source.firstIndexOf(0, 5, byteArrayOf(4)),
        )
        Assertions.assertEquals(
            3,
            source.firstIndexOf(2, 4, byteArrayOf(4)),
        )
        Assertions.assertEquals(
            -1,
            source.firstIndexOf(1, 5, byteArrayOf(4, 43, 4)),
        )
        Assertions.assertEquals(
            -1,
            source.firstIndexOf(1, 6, byteArrayOf(4, 43, 4)),
        )
        Assertions.assertEquals(
            -1,
            source.firstIndexOf(1, 5, byteArrayOf(4, 42, 4)),
        )
        Assertions.assertEquals(
            3,
            source.firstIndexOf(3, 6, byteArrayOf(4, 42, 4)),
        )
        Assertions.assertEquals(
            3,
            source.firstIndexOf(0, 6, byteArrayOf(4, 42, 4)),
        )
        Assertions.assertEquals(
            4,
            source.firstIndexOf(3, 6, byteArrayOf(42)),
        )
        Assertions.assertEquals(
            1,
            source.firstIndexOf(0, 3, byteArrayOf(42)),
        )
    }

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
            sourceEndExclusive = 10,
            position = 5,
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
            includeLeftBound = false,
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
            includeLeftBound = false,
            includeRightBound = false,
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
        val actualString = res?.first?.toString(Charsets.UTF_8)
        val actualPosition = res?.second
        Assertions.assertEquals("", actualString) { "Wrong line near 1: $actualString" }
        Assertions.assertEquals(1, actualPosition)
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
    fun `test find-line-near-position #7-c`() {
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
            includeLeftBound = false,
            includeRightBound = true,
        )
        Assertions.assertNull(res) { "found: ${res?.second} :: '${res?.first?.toString(Charsets.UTF_8)}'" }
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
        val actualString = res?.first?.toString(Charsets.UTF_8)
        val actualPosition = res?.second
        Assertions.assertEquals("", actualString) { "Wrong line near 1: $actualString" }
        Assertions.assertEquals(1, actualPosition)
    }

    @Test
    fun `test find-line-near-position #8-b`() {
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
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertNull(res)
    }

    @Test
    fun `test find-line-near-position #9-a`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20]
        // [ 97,  32,  38,  38,  32,  97,  97,  32,  38,  38,  32,  98,  98,  98,  32,  38,  38,  32, 120, 120, 120]
        // [  a,    ,   &,   &,    ,   a,   a,    ,   &,   &,    ,   b,   b,   b,    ,   &,   &,    ,   x,   x,   x]
        val txt = "a && aa && bbb && xxx"

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            position = 2,
            sourceEndExclusive = 5,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = false,
        )

        val actualString = res?.first?.toString(Charsets.UTF_8)
        val actualPosition = res?.second
        Assertions.assertEquals("a", actualString) { "Wrong line near 0: $actualString" }
        Assertions.assertEquals(0, actualPosition)
    }

    @Test
    fun `test find-line-near-position #10-a`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,    9,   10,  11,  12,  13,  14,   15,  16,  17,  18]
        // [ 96, -32, -80, -86, -32, -80, -80, -32, -79, -128, -32, -80, -107, -32, -79, -115, -32, -80, -73]
        // [  `,             ప,            ర,              ీ,               క,              ్,             ష]

        val txt = "`పరీక్ష"
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "`".toByteArray(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            position = 0,
            sourceEndExclusive = 19,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )

        val actualString = res?.first?.toString(Charsets.UTF_8)
        val actualPosition = res?.second
        Assertions.assertEquals("", actualString) { "Wrong line near 0: $actualString" }
        Assertions.assertEquals(0, actualPosition)
    }

    @Test
    fun `test find-line-near-position #11-a`() {
        // ...   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31, ...
        // ...  97,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  99, ...
        // ...   a,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   c, ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val res = findLineNearPosition(
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            position = 15,
            sourceEndExclusive = 32,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = true,
        )

        val actualString = res?.first?.toString(Charsets.UTF_8)
        val actualPosition = res?.second
        Assertions.assertEquals("b", actualString) { "Wrong line near 15: $actualString" }
        Assertions.assertEquals(11, actualPosition)
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
                comparator = defaultByteArrayComparator(),
            )
            val actualLines = res.lines(charset)
            val actualStartPosition = res.startInclusive
            val actualEndPosition = res.endExclusive
            Assertions.assertEquals(listOf("ii", "ii", "ii"), actualLines) { "Wrong result for position $it" }
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
            comparator = defaultByteArrayComparator(),
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
            comparator = defaultByteArrayComparator(),
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
            comparator = defaultByteArrayComparator(),
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
            comparator = defaultByteArrayComparator(),
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
            comparator = defaultByteArrayComparator(),
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

        val line = "xx".toByteArray(Charsets.UTF_8) to 140
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 133,
            sourceEndExclusive = 148,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = true,
            comparator = defaultByteArrayComparator(),
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
            comparator = defaultByteArrayComparator(),
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("xx", "xx"), actualLines)
        Assertions.assertEquals(140, actualStartPosition)
        Assertions.assertEquals(148, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #1-m`() {
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
            comparator = defaultByteArrayComparator(),
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("b", "b", "b"), actualLines)
        Assertions.assertEquals(11, actualStartPosition)
        Assertions.assertEquals(22, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #1-n`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  ...
        // [ 97,  32,  38,  38,  32,  97,  97,  32,  38,  38,  32,  ...
        // [  a,    ,   &,   &,    ,   a,   a,    ,   &,   &,    ,  ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"
        val charset = Charsets.UTF_8

        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "a".toByteArray(charset) to 0
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = false,
            comparator = defaultByteArrayComparator(),
        )
        val actualLines = res.lines(charset)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("a"), actualLines)
        Assertions.assertEquals(0, actualStartPosition)
        Assertions.assertEquals(1, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #1-o`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  ...
        // [ 97,  32,  38,  38,  32,  97,  97,  32,  38,  38,  32,  ...
        // [  a,    ,   &,   &,    ,   a,   a,    ,   &,   &,    ,  ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"
        val charset = Charsets.UTF_8

        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "a".toByteArray(charset) to 0
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 5,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = false,
            comparator = defaultByteArrayComparator(),
        )
        Assertions.assertEquals(Lines.NULL, res)
    }

    @Test
    fun `test find-lines-block #1-p`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  ...
        // [ 97,  32,  38,  38,  32,  97,  97,  32,  38,  38,  32,  ...
        // [  a,    ,   &,   &,    ,   a,   a,    ,   &,   &,    ,  ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "a".toByteArray(charset) to 0
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 42,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = false,
            comparator = defaultByteArrayComparator(),
        )

        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("a"), actualLines)
        Assertions.assertEquals(0, actualStartPosition)
        Assertions.assertEquals(1, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #1-q`() {
        // ...   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31, ...
        // ...  97,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  99, ...
        // ...   a,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   c, ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "b".toByteArray(charset) to 11
        val res = findLineBlock(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 148,
            delimiter = delimiter,
            includeLeftBound = true,
            includeRightBound = false,
            comparator = defaultByteArrayComparator(),
        )

        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("b", "b", "b", "b"), actualLines)
        Assertions.assertEquals(11, actualStartPosition)
        Assertions.assertEquals(27, actualEndPosition)
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
            comparator = defaultByteArrayComparator(),
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("ტესტი"), actualLines)
        Assertions.assertEquals(265, actualStartPosition)
        Assertions.assertEquals(280, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #3-a`() {
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
            comparator = defaultByteArrayComparator(),
        )

        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("", ""), actualLines)
        Assertions.assertEquals(0, actualStartPosition)
        Assertions.assertEquals(1, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #3-b`() {
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
            comparator = defaultByteArrayComparator(),
        )

        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("k"), actualLines)
        Assertions.assertEquals(2, actualStartPosition)
        Assertions.assertEquals(3, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #4-a`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19]
        // [ 49,  37,  50,  37,  51,  37,  52,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   2,   %,   3,   %,   4,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).map { it }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        val res = findLineBlock(
            line = "10".toByteArray(charset) to 18,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 20,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = true,
            comparator = defaultByteArrayComparator(),
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("10"), actualLines)
        Assertions.assertEquals(18, actualStartPosition)
        Assertions.assertEquals(20, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #5-a`() {
        // [  0,   1,   2,   3,   4,   5,    6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20]
        // [ 96,  96,  96,  96,  96, -61, -106, 108, -61, -89, 101, 107,  96,  96,  96,  96,  96,  96,  96,  96,  96]
        // [  `,   `,   `,   `,   `,         Ö,    l,       ç,   e,   k,   `,   `,   `,   `,   `,   `,   `,   `,   `]
        val txt = "`````Ölçek`````````"
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "``".toByteArray(charset)
        val res = findLineBlock(
            line = "".toByteArray(charset) to 4,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 12,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = true,
            comparator = defaultByteArrayComparator(),
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("", ""), actualLines)
        Assertions.assertEquals(2, actualStartPosition)
        Assertions.assertEquals(4, actualEndPosition)
    }

    @Test
    fun `test find-lines-block #5-b`() {
        // [  0,   1,   2,   3,   4,   5,    6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20]
        // [ 96,  96,  96,  96,  96, -61, -106, 108, -61, -89, 101, 107,  96,  96,  96,  96,  96,  96,  96,  96,  96]
        // [  `,   `,   `,   `,   `,         Ö,    l,       ç,   e,   k,   `,   `,   `,   `,   `,   `,   `,   `,   `]
        val txt = "`````Ölçek`````````"
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "``".toByteArray(charset)
        val res = findLineBlock(
            line = "".toByteArray(charset) to 14,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 4,
            sourceEndExclusive = 21,
            delimiter = delimiter,
            includeLeftBound = false,
            includeRightBound = true,
            comparator = defaultByteArrayComparator(),
        )
        val actualLines = res.lines(Charsets.UTF_8)
        val actualStartPosition = res.startInclusive
        val actualEndPosition = res.endExclusive
        Assertions.assertEquals(listOf("", "", ""), actualLines)
        Assertions.assertEquals(14, actualStartPosition)
        Assertions.assertEquals(18, actualEndPosition)
    }

    @Test
    fun `test binary search #1-a`() {
        // ...   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31, ...
        // ...  97,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  98,  32,  38,  38,  32,  99, ...
        // ...   a,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   b,    ,   &,   &,    ,   c, ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "b".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
        )
        Assertions.assertEquals(11, res.startInclusive)
        Assertions.assertEquals(27, res.endExclusive)
        Assertions.assertEquals(listOf("b", "b", "b", "b"), res.lines(charset))
    }

    @Test
    fun `test binary search #1-b`() {
        // ...  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112 ...
        // ... 107,  32,  38,  38,  32, 107, 107, 107,  32,  38,  38,  32, 108,  32,  38,  38,  32, 111, 111,  32,  38,  38,  32, 112...
        // ...   k,    ,   &,   &,    ,   k,   k,   k,    ,   &,   &,    ,   l,    ,   &,   &,    ,   o,   o,    ,   &,   &,    ,   p...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "l".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
        )
        Assertions.assertEquals(101, res.startInclusive)
        Assertions.assertEquals(102, res.endExclusive)
        Assertions.assertEquals(listOf("l"), res.lines(charset))
    }

    @Test
    fun `test binary search #1-c`() {
        // ...  89,  90,  91,  92,  93,  94,  95,  96,  97,  98,  99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, ...
        // ... 107,  32,  38,  38,  32, 107, 107, 107,  32,  38,  38,  32, 108,  32,  38,  38,  32, 111, 111,  32,  38,  38,  32, ...
        // ...   k,    ,   &,   &,    ,   k,   k,   k,    ,   &,   &,    ,   l,    ,   &,   &,    ,   o,   o,    ,   &,   &,    , ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "l".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
        )
        Assertions.assertEquals(101, res.startInclusive)
        Assertions.assertEquals(102, res.endExclusive)
        Assertions.assertEquals(listOf("l"), res.lines(charset))
    }

    @Test
    fun `test binary search #1-d`() {
        // ... 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147]
        // ... 119,  32,  38,  38,  32, 120,  32,  38,  38,  32, 120, 120,  32,  38,  38,  32, 120, 120]
        // ...   w,    ,   &,   &,    ,   x,    ,   &,   &,    ,   x,   x,    ,   &,   &,    ,   x,   x]
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "xx".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
        )
        Assertions.assertEquals(140, res.startInclusive)
        Assertions.assertEquals(148, res.endExclusive)
        Assertions.assertEquals(listOf("xx", "xx"), res.lines(charset))
    }

    @Test
    fun `test binary search #1-e`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  ...
        // [ 97,  32,  38,  38,  32,  97,  97,  32,  38,  38,  32,  ...
        // [  a,    ,   &,   &,    ,   a,   a,    ,   &,   &,    ,  ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "a".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 42,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(0, res.startInclusive)
        Assertions.assertEquals(1, res.endExclusive)
        Assertions.assertEquals(listOf("a"), res.lines(charset))
    }

    @Test
    fun `test binary search #1-f`() {
        // ... 25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48, ...
        // ... 32,  98,  32,  38,  38,  32,  99,  99, 100,  32,  38,  38,  32,  99,  99, 100,  32,  38,  38,  32, 100, 102, 103,  32, ...
        // ...   ,   b,    ,   &,   &,    ,   c,   c,   d,    ,   &,   &,    ,   c,   c,   d,    ,   &,   &,    ,   d,   f,   g,    , ...
        val txt = "a && aa && b && b && b && b && ccd && ccd && dfg && ghf && " +
                "i && ii && ii && ii && kk && kk && kkk && l && oo && pw && q && ww && ww && x && xx && xx"

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " && ".toByteArray(charset)
        val line = "ccc".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 42,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(31, res.startInclusive)
        Assertions.assertEquals(31, res.endExclusive)
        Assertions.assertEquals(emptyList<String>(), res.lines(charset))
    }


    @Test
    fun `test binary search #2-a`() {
        // [  0,   1,   4,   5,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19]
        // [ 49,  37,  51,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   3,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).filterNot { it != 2 && it != 4 }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        data.forEach { word ->
            val line = word.toByteArray(charset)
            val res = binarySearch(
                line = line,
                source = ByteBuffer.wrap(source),
                sourceStartInclusive = 0,
                sourceEndExclusive = source.size,
                delimiter = delimiter,
                comparator = byteArrayComparator(charset) { it.toInt() },
                includeLeftBound = true,
                includeRightBound = true,
            )
            Assertions.assertEquals(listOf(word), res.lines(charset))
        }
    }

    @Test
    fun `test binary search #2-b`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 49,  37,  51,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   3,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).filter { it != 2 && it != 4 }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        val line = "2".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = byteArrayComparator(charset) { it.toInt() },
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(2, res.startInclusive)
        Assertions.assertEquals(2, res.endExclusive)
        Assertions.assertEquals(emptyList<String>(), res.lines(charset))
    }

    @Test
    fun `test binary search #2-c`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 49,  37,  51,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   3,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).filter { it != 2 && it != 4 }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        val line = "4".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = byteArrayComparator(charset) { it.toInt() },
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(4, res.startInclusive)
        Assertions.assertEquals(4, res.endExclusive)
        Assertions.assertEquals(emptyList<String>(), res.lines(charset))
    }

    @Test
    fun `test binary search #2-d`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 49,  37,  51,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   3,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).filter { it != 2 && it != 4 }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        val line = "1".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = byteArrayComparator(charset) { it.toInt() },
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(0, res.startInclusive)
        Assertions.assertEquals(1, res.endExclusive)
        Assertions.assertEquals(listOf("1"), res.lines(charset))
    }

    @Test
    fun `test binary search #2-e`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 49,  37,  51,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   3,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).filter { it != 2 && it != 4 }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        val line = "10".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = byteArrayComparator(charset) { it.toInt() },
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(14, res.startInclusive)
        Assertions.assertEquals(16, res.endExclusive)
        Assertions.assertEquals(listOf("10"), res.lines(charset))
    }

    @Test
    fun `test binary search #2-f`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 49,  37,  51,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   3,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).filter { it != 2 && it != 4 }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        val line = "-42".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = byteArrayComparator(charset) { it.toInt() },
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(-1, res.startInclusive)
        Assertions.assertEquals(0, res.endExclusive)
        Assertions.assertEquals(emptyList<String>(), res.lines(charset))
    }

    @Test
    fun `test binary search #2-g`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 49,  37,  51,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   3,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).filter { it != 2 && it != 4 }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        val line = "42".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = byteArrayComparator(charset) { it.toInt() },
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(16, res.startInclusive)
        Assertions.assertEquals(-1, res.endExclusive)
        Assertions.assertEquals(emptyList<String>(), res.lines(charset))
    }

    @Test
    fun `test binary search #2-k`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 49,  37,  51,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   3,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).filter { it != 2 && it != 4 }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        val line = "3".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 2,
            sourceEndExclusive = 13,
            delimiter = delimiter,
            comparator = byteArrayComparator(charset) { it.toInt() },
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals(-1, res.startInclusive)
        Assertions.assertEquals(3, res.endExclusive)
        Assertions.assertEquals(emptyList<String>(), res.lines(charset))
    }

    @Test
    fun `test binary search #2-l`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15]
        // [ 49,  37,  51,  37,  53,  37,  54,  37,  55,  37,  56,  37,  57,  37,  49,  48]
        // [  1,   %,   3,   %,   5,   %,   6,   %,   7,   %,   8,   %,   9,   %,   1,   0]
        val data = (1..10).filter { it != 2 && it != 4 }.distinct().sorted().map { it.toString() }
        val txt = data.joinToString("%")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)
        val line = "42".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 9,
            sourceEndExclusive = 13,
            delimiter = delimiter,
            comparator = byteArrayComparator(charset) { it.toInt() },
            includeLeftBound = false,
            includeRightBound = false,
        )
        Assertions.assertEquals(11, res.startInclusive)
        Assertions.assertEquals(-1, res.endExclusive)
        Assertions.assertEquals(emptyList<String>(), res.lines(charset))
    }

    @Test
    fun `test binary search #3-a`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20, ...
        // [ 97, 100,  32,  97, 100, 105, 112, 105, 115,  99, 105, 110, 103,  32,  97, 108, 105, 113, 117,  97,  32, ...
        // [  a,   d,    ,   a,   d,   i,   p,   i,   s,   c,   i,   n,   g,    ,   a,   l,   i,   q,   u,   a,    , ...
        val txt = TEST_DATA_2
            .replace(".", " ")
            .replace(",", " ")
            .split("\\s+".toRegex())
            .filterNot { it.isBlank() }
            .map { it.lowercase() }
            .sorted()
            .joinToString(" ")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " ".toByteArray(charset)

        mapOf("ad" to (0 to 2), "adipiscing" to (3 to 13), "aliqua" to (14 to 20)).forEach { (w, i) ->
            val line = w.toByteArray(charset)
            val res = binarySearch(
                line = line,
                source = ByteBuffer.wrap(source),
                sourceStartInclusive = 0,
                sourceEndExclusive = 42,
                delimiter = delimiter,
                comparator = byteArrayComparator(charset) { it },
                includeLeftBound = true,
                includeRightBound = false,
            )
            Assertions.assertEquals(i.first.toLong(), res.startInclusive) { "word='$w', expected indexes=$i" }
            Assertions.assertEquals(i.second.toLong(), res.endExclusive) { "word='$w', expected indexes=$i" }
            Assertions.assertEquals(listOf(w), res.lines(charset)) { "word='$w', expected indexes=$i" }
        }
    }

    @Test
    fun `test binary search #3-b`() {
        // ... 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436]
        // ... 117, 116,  32, 117, 116,  32, 117, 116,  32, 118, 101, 108, 105, 116,  32, 118, 101, 110, 105,  97, 109,  32, 118, 111, 108, 117, 112, 116,  97, 116, 101]
        // ...   u,   t,    ,   u,   t,    ,   u,   t,    ,   v,   e,   l,   i,   t,    ,   v,   e,   n,   i,   a,   m,    ,   v,   o,   l,   u,   p,   t,   a,   t,   e]
        val txt = TEST_DATA_2
            .replace(".", " ")
            .replace(",", " ")
            .split("\\s+".toRegex())
            .filterNot { it.isBlank() }
            .map { it.lowercase() }
            .sorted()
            .joinToString(" ")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " ".toByteArray(charset)

        mapOf(
            ("ut" to 3) to (406 to 414),
            ("velit" to 1) to (415 to 420),
            ("veniam" to 1) to (421 to 427),
            ("voluptate" to 1) to (428 to 437),
        ).forEach { (w, i) ->
            val line = w.first.toByteArray(charset)
            val res = binarySearch(
                line = line,
                source = ByteBuffer.wrap(source),
                sourceStartInclusive = 42,
                sourceEndExclusive = 437,
                delimiter = delimiter,
                comparator = byteArrayComparator(charset) { it },
            )
            val expected = (0 until w.second).map { w.first }
            Assertions.assertEquals(expected, res.lines(charset)) { "word='$w', expected indexes=$i" }
            Assertions.assertEquals(i.first.toLong(), res.startInclusive) { "word='$w', expected indexes=$i" }
            Assertions.assertEquals(i.second.toLong(), res.endExclusive) { "word='$w', expected indexes=$i" }
        }
    }

    @Test
    fun `test binary search #3-c`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20, ...
        // [ 97, 100,  32,  97, 100, 105, 112, 105, 115,  99, 105, 110, 103,  32,  97, 108, 105, 113, 117,  97,  32, ...
        // [  a,   d,    ,   a,   d,   i,   p,   i,   s,   c,   i,   n,   g,    ,   a,   l,   i,   q,   u,   a,    , ...
        val txt = TEST_DATA_2
            .replace(".", " ")
            .replace(",", " ")
            .split("\\s+".toRegex())
            .filterNot { it.isBlank() }
            .map { it.lowercase() }
            .sorted()
            .joinToString(" ")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " ".toByteArray(charset)

        (3..12).forEach {
            val line = "adipiscing".toByteArray(charset)
            val res = binarySearch(
                line = line,
                source = ByteBuffer.wrap(source),
                sourceStartInclusive = it,
                sourceEndExclusive = 42,
                delimiter = delimiter,
                comparator = defaultByteArrayComparator(charset),
                includeLeftBound = false,
                includeRightBound = false,
            )
            Assertions.assertEquals(-1, res.startInclusive)
            Assertions.assertEquals(13, res.endExclusive)
            Assertions.assertEquals(emptyList<String>(), res.lines(charset))
        }
    }

    @Test
    fun `test binary search #3-d`() {
        // ...  415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436]
        // ...  118, 101, 108, 105, 116,  32, 118, 101, 110, 105,  97, 109,  32, 118, 111, 108, 117, 112, 116,  97, 116, 101]
        // ...    v,   e,   l,   i,   t,    ,   v,   e,   n,   i,   a,   m,    ,   v,   o,   l,   u,   p,   t,   a,   t,   e]
        val txt = TEST_DATA_2
            .replace(".", " ")
            .replace(",", " ")
            .split("\\s+".toRegex())
            .filterNot { it.isBlank() }
            .map { it.lowercase() }
            .sorted()
            .joinToString(" ")

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = " ".toByteArray(charset)

        (428..436).forEach {
            val line = "voluptate".toByteArray(charset)
            val res = binarySearch(
                line = line,
                source = ByteBuffer.wrap(source),
                sourceStartInclusive = 221,
                sourceEndExclusive = it,
                delimiter = delimiter,
                comparator = defaultByteArrayComparator(charset),
                includeLeftBound = false,
                includeRightBound = false,
            )
            Assertions.assertEquals(427, res.startInclusive)
            Assertions.assertEquals(-1, res.endExclusive)
            Assertions.assertEquals(emptyList<String>(), res.lines(charset))
        }
    }


    @Test
    fun `test binary search #4-a`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,   11,  12,  13,   14,  15,  16,   17,  18,  19,  20]
        // [ 96,  96,  96, -32, -80, -86, -32, -80, -80, -32, -79, -128, -32, -80, -107, -32, -79, -115, -32, -80, -73]
        // [  `,   `,   `,             ప,            ర,              ీ,               క,              ్,             ష]
        val txt = "```పరీక్ష"

        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "`".toByteArray(charset)
        val res = binarySearch(
            line = byteArrayOf(), // empty string
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
            includeLeftBound = true,
            includeRightBound = false,
        )
        Assertions.assertEquals(listOf("", "", ""), res.lines(charset))
    }

    @Test
    fun `test binary search #5-a`() {
        // [  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20]
        // [ 96,  96,  96,  96,  96,  96,  96,  96,  96,  96,  96,  96,  96,  96, -61, -106, 108, -61, -89, 101, 107]
        // [  `,   `,   `,   `,   `,   `,   `,   `,   `,   `,   `,   `,   `,   `,         Ö,   l,        ç,   e,   k]
        val txt = "``````````````Ölçek"
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "``".toByteArray(charset)
        val res = binarySearch(
            line = byteArrayOf(), // empty string
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(listOf("", "", "", "", "", "", ""), res.lines(charset))
    }

    @Test
    fun `test binary search #6-a`() {
        val txt = TEST_DATA_1.split(";").sorted().joinToString("\n")
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "\n".toByteArray(charset)
        val line = "ການທົດສອບ".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
        )
        Assertions.assertEquals(listOf("ການທົດສອບ"), res.lines(charset))
    }

    @Test
    fun `test binary search #6-b`() {
        val txt = TEST_DATA_1.split(";").sorted().joinToString("\n")
        val charset = Charsets.UTF_16LE
        val source = txt.toByteArray(charset)
        val delimiter = "\n".toByteArray(charset)
        val line = "ፈተና".toByteArray(charset)
        val res = binarySearch(
            line = line,
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = source.size,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
        )
        Assertions.assertEquals(listOf("ፈተና", "ፈተና"), res.lines(charset))
    }

    @Test
    fun `test binary search #6-c`() {
        val data = TEST_DATA_1.split(";").distinct().sorted()
        val txt = data.joinToString("%")
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = "%".toByteArray(charset)

        data.forEach { word ->
            val line = word.toByteArray(charset)
            val res = binarySearch(
                line = line,
                source = ByteBuffer.wrap(source),
                sourceStartInclusive = 0,
                sourceEndExclusive = source.size,
                delimiter = delimiter,
                comparator = defaultByteArrayComparator(charset),
            )
            Assertions.assertEquals(listOf(word), res.lines(charset)) { "Can't find word `$word`" }
        }
    }

    @Test
    fun `test binary search #7-a`() {
        val txt = "42,42"
        val charset = Charsets.UTF_8
        val source = txt.toByteArray(charset)
        val delimiter = ",".toByteArray(charset)
        val res1 = binarySearch(
            line = "42".toByteArray(charset),
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 1,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(1, res1.startInclusive)
        Assertions.assertEquals(-1, res1.endExclusive)
        Assertions.assertTrue(res1.lines.isEmpty())
        val res2 = binarySearch(
            line = "".toByteArray(charset),
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 1,
            delimiter = delimiter,
            comparator = defaultByteArrayComparator(charset),
            includeLeftBound = true,
            includeRightBound = true,
        )
        Assertions.assertEquals(-1, res2.startInclusive)
        Assertions.assertEquals(0, res2.endExclusive)
        Assertions.assertTrue(res2.lines.isEmpty())
    }

    @Test
    fun `test binary search #8-a`() {
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
        val source = txt.toByteArray(charset)
        val delimiter = "\n".toByteArray(charset)
        val comparator = Comparator<String> { left, right ->
            val a = left.substringAfterLast(":")
            val b = right.substringAfterLast(":")
            a.compareTo(b)
        }.toByteComparator(charset)

        val res1 = binarySearch(
            line = "xxx:A".toByteArray(charset),
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 515,
            delimiter = delimiter,
            comparator = comparator,
        )
        Assertions.assertEquals(0, res1.startInclusive)
        Assertions.assertEquals(85, res1.endExclusive)
        Assertions.assertEquals(
            listOf(
                "433e7ff4-f3ae-4432-8e31-e3d0d8601780:001:A",
                "b6b65cc6-1584-41c3-af01-42eacf18623d:002:A",
            ), res1.lines(charset)
        )

        val res2 = binarySearch(
            line = ":B".toByteArray(charset),
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 515,
            delimiter = delimiter,
            comparator = comparator,
        )
        println(res2)
        println(res2.lines(charset))

        Assertions.assertEquals(86, res2.startInclusive)
        Assertions.assertEquals(214, res2.endExclusive)
        Assertions.assertEquals(
            listOf(
                "b6b65cc6-1584-41c3-af01-42eacf18623d:003:B",
                "433e7ff4-f3ae-4432-8e31-e3d0d8601780:004:B",
                "3466935b-cb0d-4586-81c6-cd5b82c8922c:005:B",
            ), res2.lines(charset)
        )

        val res3 = binarySearch(
            line = "433e7ff4-f3ae-4432-8e31-e3d0d8601780:008:D".toByteArray(charset),
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 515,
            delimiter = delimiter,
            comparator = comparator,
        )
        println(res3)
        println(res3.lines(charset))

        Assertions.assertEquals(301, res3.startInclusive)
        Assertions.assertEquals(343, res3.endExclusive)
        Assertions.assertEquals(
            listOf(
                "433e7ff4-f3ae-4432-8e31-e3d0d8601780:008:D",
            ), res3.lines(charset)
        )

        val res4 = binarySearch(
            line = "433e7ff4-f3ae-4432-8e31-e3d0d8601780:008:G".toByteArray(charset),
            source = ByteBuffer.wrap(source),
            sourceStartInclusive = 0,
            sourceEndExclusive = 515,
            delimiter = delimiter,
            comparator = comparator,
        )
        println(res4)
        println(res4.lines(charset))

        Assertions.assertEquals(473, res4.startInclusive)
        Assertions.assertEquals(515, res4.endExclusive)
        Assertions.assertEquals(
            listOf(
                "f937a264-abef-4d3e-ad86-90c0a0d85e7a:012:G",
            ), res4.lines(charset)
        )
    }
}