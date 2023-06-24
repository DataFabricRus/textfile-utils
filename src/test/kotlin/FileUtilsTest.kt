package cc.datafabric.textfileutils

import cc.datafabric.textfileutils.files.calcChunkSize
import cc.datafabric.textfileutils.files.insert
import cc.datafabric.textfileutils.files.insertLines
import cc.datafabric.textfileutils.files.invert
import cc.datafabric.textfileutils.files.isSorted
import cc.datafabric.textfileutils.files.sameFilePaths
import cc.datafabric.textfileutils.files.use
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.random.Random

internal class FileUtilsTest {

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
            it.insert(
                data = insertContent.joinToString("\n", "", "\n").toByteArray(),
                beforePosition = 0,
                buffer = ByteBuffer.allocateDirect(42),
            )
        }
        val expectedContent = insertContent + originalContent
        val actualContent = file.readText().split("\n").map { it.toInt() }
        Assertions.assertEquals(expectedContent, actualContent)
    }

    @Test
    fun `test insert to the end of file (big file, small buffer)`(@TempDir dir: Path) {
        val originalContent = (0..4242).map { Random.Default.nextInt() }
        val file = Files.createTempFile(dir, "test-insert-", ".xxx")
        file.writeText(originalContent.joinToString("\n"))

        val insertContent = (0..42).map { Random.Default.nextInt() }
        file.use {
            it.insert(
                data = insertContent.joinToString("\n", "\n", "").toByteArray(),
                beforePosition = file.fileSize(),
                buffer = ByteBuffer.allocateDirect(42),
            )
        }
        val expectedContent = originalContent + insertContent
        val actualContent = file.readText().split("\n").map { it.toInt() }
        Assertions.assertEquals(expectedContent, actualContent)
    }

    @Test
    fun `test insert to the beginning of file (big file, small buffer)`(@TempDir dir: Path) {
        testInsertAtRandomPosition(
            dir = dir,
            insertPositionInLines = 39,
            originalContent = (0..4242).map { Random.Default.nextInt() },
            insertContent = (0..42).map { Random.Default.nextInt() },
            bufferSize = 42,
            charset = Charsets.UTF_8,
        )
    }

    @Test
    fun `test insert to the middle of file (big file, small buffer)`(@TempDir dir: Path) {
        testInsertAtRandomPosition(
            dir = dir,
            insertPositionInLines = 210,
            originalContent = (0..420).map { Random.Default.nextInt() },
            insertContent = (0..7).map { Random.Default.nextInt() },
            bufferSize = 4,
            charset = Charsets.UTF_16,
        )
    }

    @Test
    fun `test insert near the end of file (big file, small buffer)`(@TempDir dir: Path) {
        testInsertAtRandomPosition(
            dir = dir,
            insertPositionInLines = 415,
            originalContent = (0..420).map { Random.Default.nextInt() },
            insertContent = (0..7).map { Random.Default.nextInt() },
            bufferSize = 4,
            charset = Charsets.UTF_8,
        )
    }

    @Test
    fun `test insert to the middle of file (small file, big buffer)`(@TempDir dir: Path) {
        testInsertAtRandomPosition(
            dir = dir,
            insertPositionInLines = 210,
            originalContent = (0..420).map { Random.Default.nextInt() },
            insertContent = (0..7).map { Random.Default.nextInt() },
            bufferSize = 424242,
            charset = Charset.defaultCharset(),
        )
    }

    @Test
    fun `test invert file content`(@TempDir dir: Path) {
        val source = Files.createTempFile(dir, "test-insert-source-", ".xxx")
        val target = Files.createTempFile(dir, "test-insert-target-", ".xxx")
        val sourceContent = (1..42).map { Random.nextDouble() }
        source.writeText(sourceContent.joinToString(";"), Charsets.UTF_8)

        invert(source = source, target = target, delimiter = ";", deleteSourceFiles = true, charset = Charsets.UTF_8)

        val targetContent = target.readText(Charsets.UTF_8).split(";").map { it.toDouble() }
        Assertions.assertEquals(sourceContent.reversed(), targetContent)
        Assertions.assertFalse(source.exists())
    }

    @Test
    fun `test is sorted`(@TempDir dir: Path) {
        val f1 = Files.createTempFile(dir, "test-is-sorted-1-", ".xxx")
        f1.writeText((1..42).map { Random.nextDouble() }.sorted().joinToString(","), Charsets.UTF_8)
        Assertions.assertTrue(
            isSorted(
                file = f1,
                delimiter = ",",
                charset = Charsets.UTF_8,
                comparator = { a, b -> a.toDouble().compareTo(b.toDouble()) }
            )
        )

        val f2 = Files.createTempFile(dir, "test-is-sorted-2-", ".xxx")
        f2.writeText((1..42).map { Random.nextLong() }.reversed().joinToString("\n"), Charsets.UTF_8)
        Assertions.assertFalse(
            isSorted(
                file = f2,
                delimiter = "\n",
                charset = Charsets.UTF_8,
                comparator = { a, b -> a.toDouble().compareTo(b.toDouble()) }
            )
        )

        val f3 = Files.createTempFile(dir, "test-is-sorted-2-", ".xxx")
        f3.writeText(sequenceOf(989, 2, 333, 55454, -1).joinToString(";"), Charsets.UTF_32)
        Assertions.assertFalse(
            isSorted(
                file = f3,
                delimiter = ";",
                charset = Charsets.UTF_32,
                comparator = { a, b -> a.toInt().compareTo(b.toInt()) }
            )
        )

        val f4 = Files.createTempFile(dir, "test-is-sorted-2-", ".xxx")
        f4.writeText(sequenceOf(-999.9, 2, 3, 3, 3, 3, 42).joinToString("\n"), Charsets.ISO_8859_1)
        Assertions.assertTrue(
            isSorted(
                file = f4,
                charset = Charsets.ISO_8859_1,
                comparator = { a, b -> a.toFloat().compareTo(b.toFloat()) }
            )
        )
    }

    @Test
    fun `test sameFilePaths(Path, Path)`() {
        val root1 = Paths.get(".").toRealPath()
        val root2 = Paths.get(root1.toString().lowercase())
        val root3 = Paths.get(root1.toString().replaceFirst("\\", "\\.\\"))
        val root4 = Paths.get(root1.toString().replace("\\", "\\.\\"))
        Assertions.assertTrue(
            sameFilePaths(Paths.get("/tmp/.gitignore"), Paths.get("/tmp/.gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get(".gitignore"), Paths.get(".gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root1\\.gitignore"), Paths.get("$root1\\.gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root1\\.gitignore"), Paths.get(".gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root2\\.gitignore"), Paths.get("$root1\\.gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root2\\.gitignore"), Paths.get("$root3\\.gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root4\\.gitignore"), Paths.get(".gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(Paths.get("$root1\\..\\${root1.fileName}\\.gitignore"), Paths.get(".gitignore"))
        )
        Assertions.assertTrue(
            sameFilePaths(
                Paths.get("W:\\tmp\\.\\text-file-sorting\\.\\.gitignore"),
                Paths.get("W:\\tmp\\..\\tmp\\.\\text-file-sorting\\.gitignore")
            )
        )

        Assertions.assertFalse(
            sameFilePaths(
                Paths.get("W:\\xxx${Random.Default.nextInt()}\\text-file-sorting\\.\\.gitignore"),
                Paths.get(".gitignore")
            )
        )
        Assertions.assertFalse(
            sameFilePaths(
                Paths.get("W:\\tmp\\text-file-sorting\\.\\.gitignore"),
                Paths.get("Q:\\tmp\\text-file-sorting\\.gitignore")
            )
        )
        Assertions.assertFalse(
            sameFilePaths(
                Paths.get("W:\\tmp\\uuu\\text-file-sorting\\.\\.gitignore"),
                Paths.get("W:\\tmp\\text-file-sorting\\.gitignore")
            )
        )
        Assertions.assertFalse(
            sameFilePaths(
                Paths.get("W:\\tmp\\text-file-sorting\\.\\.gitignore-1"),
                Paths.get("W:\\tmp\\text-file-sorting\\.gitignore-2")
            )
        )
        Assertions.assertFalse(
            sameFilePaths(Paths.get("/xxx/.gitignore"), Paths.get("/qqq/.gitignore"))
        )
    }

    @Test
    fun `test calc chunk size`() {
        testCalcChunkSize(16349910L, 8912)
        testCalcChunkSize(4657, 102)
        testCalcChunkSize(4657, 91)
        testCalcChunkSize(4657, 4656)
        testCalcChunkSize(42, 21)
        testCalcChunkSize(42, 31)
        testCalcChunkSize(217, 215)
        testCalcChunkSize(17, 15)
        testCalcChunkSize(333, 222)
        testCalcChunkSize(4242424242424242424, 8912)
        testCalcChunkSize(4242424242424242424, 424242424)
        testCalcChunkSize(2, 1)
    }

    @ParameterizedTest
    @MethodSource("testInsertLinesData")
    fun `test insert lines`(insertData: List<String>, position: Long, index: Int, bufferSize: Int, @TempDir dir: Path) {
        val originLines = listOf(
            "433e7ff4-f3ae-4432-8e31-e3d0d8601780:001:A",
            "b6b65cc6-1584-41c3-af01-42eacf18623d:002:A",
            "b6b65cc6-1584-41c3-af01-42eacf18623d:003:B",
            "433e7ff4-f3ae-4432-8e31-e3d0d8601780:004:B",
            "3466935b-cb0d-4586-81c6-cd5b82c8922c:005:B",
            "433e7ff4-f3ae-4432-8e31-e3d0d8601780:006:C",
            "543dc027-19f8-48e4-9e82-3c0413b91d90:007:C",
            "433e7ff4-f3ae-4432-8e31-e3d0d8601780:008:D",
            "d7411949-bc08-443e-b8fa-997187d6f73e:009:E",
            "3466935b-cb0d-4586-81c6-cd5b82c8922c:010:F",
            "3466935b-cb0d-4586-81c6-cd5b82c8922c:011:F",
            "f937a264-abef-4d3e-ad86-90c0a0d85e7a:012:G",
        )
        val dataFile = Files.createTempFile(dir, "test-insert-", ".xxx")
        dataFile.writeText(originLines.joinToString("\r\n"), Charsets.UTF_8)
        insertLines(
            target = dataFile,
            lines = insertData,
            delimiter = "\r\n",
            charset = Charsets.UTF_8,
            position = position,
            buffer = ByteBuffer.allocate(bufferSize)
        )

        val expectedData = originLines.toMutableList()
        insertData.forEachIndexed { i, line ->
            expectedData.add(index + i, line)
        }

        val actual = dataFile.readText(Charsets.UTF_8)

        val expected = expectedData.joinToString("\r\n")

        Assertions.assertEquals(expected, actual)
    }

    companion object {

        @JvmStatic
        private fun testInsertLinesData(): List<Arguments> {
            return listOf(
                Arguments.arguments(
                    listOf("bfb4f76b-96cf-45c0-81c2-7ec832da7fd7::I", "4608f812-5a70-4e73-9616-365ce91ba4a1::I"),
                    0L, 0, 91,
                ),
                Arguments.arguments(
                    listOf("52ce3eb8-f432-432e-9771-66800c8a422c::I"),
                    0L, 0, 42,
                ),
                Arguments.arguments(
                    listOf("c8968af2-afb4-4239-817f-7a85685a997b::I"),
                    88L, 2, 42,
                ),
                Arguments.arguments(
                    listOf(
                        "2251dcdd-0f25-410b-96ab-1550ff162ea8::I",
                        "e55aa883-2503-45cf-8791-75c633181663::I",
                        "7279264d-7df7-40c0-b35c-b8f10012c351::I",
                        "a795815c-9832-4ff8-b8c2-29ddf67f4f68::I",
                        "ba9f2c8d-1f4c-418d-b0c8-e0d19cd23028::I",
                        "0ecd0e5f-3ac0-4cee-96f7-3a8bc6f2fece::I",
                        "69135b71-35dc-460c-85f8-8f205a60b8a6::I",
                        "847282e0-8497-4a18-bc75-0985deda75fe::I",
                        "04bfa28d-71bb-4dc3-8c58-e27e378652d7::I",
                        "baa9fd6a-1054-4ef4-848b-2c12f4984e98::I",
                        "c900e7d4-00e5-4de6-be94-8796cd639725::I",
                        "d232787a-278c-42f2-bd68-85839139e22e::I",
                        "aa775db5-16ba-4999-8a1c-6aff5f3f98a4::I",
                        "771fbbfe-38db-425a-bb94-74d3d7c6f346::I",
                        "64176e88-5df9-43a3-8df3-10355499c448::I",
                        "53c9ab38-23a7-4e8d-b13c-4b578dc2250a::I",
                        "b2e7293e-f5b1-4c11-8def-4272094f911e::I",
                        "2fcb3780-1b51-41f4-a776-ae4ef1f4ac45::I",
                        "15794169-8b1a-4f93-a2a2-b6391af9a91b::I",
                        "e5f60463-d280-448a-a06e-4d12df835261::I",
                        "eb70b4a9-e649-46b6-bf58-a35aa7cc6fd7::I",
                    ),
                    220L, 5, 42,
                ),
                Arguments.arguments(
                    listOf(
                        "312fb2dc-9b82-4ab7-b681-96a7552393c2::I",
                        "7ce2c887-e89b-40f6-9015-0422afd12db0::I",
                        "35b7b090-6d9f-46b9-a1e4-f7acbaac0ade::I",
                        "c998d75f-0374-4a13-a6f7-147915f3bdd9::I",
                    ),
                    308L, 7, 42000,
                ),
                Arguments.arguments(
                    listOf(
                        "1c6eb354-6d3f-4290-ae98-2c6a6a7bf3db::I",
                        "2e373479-ea20-48da-8401-0898130249a8::I",
                        "d841d47a-7775-42bb-978f-15bfc4517a72::I",
                    ),
                    484L, 11, 42,
                ),
                Arguments.arguments(
                    listOf(
                        "d5f6ba4e-90d4-4e00-b66e-2910c4aa3cc3::I",
                        "b43ee595-bb83-4073-bc3c-7eab20f53e57::I",
                    ),
                    526L, 12, 42,
                ),
            )
        }

        private fun testInsertAtTheBeginningOfNonEmptyFile(dir: Path, bufferSize: Int) {
            val txtBefore = """
            Sed ut perspiciatis, unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, 
            totam rem aperiam eaque ipsa, quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt, 
            explicabo.
        """.trimIndent()
            val file = Files.createTempFile(dir, "test-insert-", ".xxx")
            file.writeText(TEST_DATA_2)
            file.use {
                it.insert(
                    data = txtBefore.toByteArray(),
                    beforePosition = 0,
                    buffer = ByteBuffer.allocateDirect(bufferSize)
                )
            }
            val actualText = file.readText()
            Assertions.assertEquals(txtBefore + TEST_DATA_2, actualText)
        }

        private fun testInsertAtTheBeginningOfEmptyFile(dir: Path, bufferSize: Int) {
            val file = Files.createTempFile(dir, "test-insert-", ".xxx")
            file.use {
                it.insert(
                    data = TEST_DATA_2.toByteArray(),
                    beforePosition = 0,
                    buffer = ByteBuffer.allocateDirect(bufferSize)
                )
            }
            val actualText = file.readText()
            Assertions.assertEquals(TEST_DATA_2, actualText)
        }

        private fun <X> testInsertAtRandomPosition(
            dir: Path,
            insertPositionInLines:
            Int,
            originalContent: List<X>,
            insertContent: List<X>,
            bufferSize: Int,
            charset: Charset,
        ) {
            val file = Files.createTempFile(dir, "test-insert-", ".xxx")

            val originalContentAsString = originalContent.joinToString("\n")
            val originalContentAsByteArray = originalContentAsString.toByteArray(charset)
            val insertContentAsString = insertContent.joinToString(separator = "\n", prefix = "", postfix = "\n")
            val insertContentAsByteArray = insertContentAsString.toByteArray(charset)
            val insertPositionInBytes = originalContent.take(insertPositionInLines)
                .joinToString(separator = "\n", prefix = "", postfix = "\n")
                .toByteArray(charset).size.toLong()
            val expectedContentAsString =
                (originalContentAsByteArray.take(insertPositionInBytes.toInt()).toByteArray() +
                        insertContentAsByteArray +
                        originalContentAsByteArray.drop(insertPositionInBytes.toInt()).toByteArray()
                        ).toString(charset)

            file.writeText(originalContentAsString, charset)

            file.use {
                it.insert(
                    data = insertContentAsByteArray,
                    beforePosition = insertPositionInBytes,
                    buffer = ByteBuffer.allocateDirect(bufferSize),
                )
            }
            val actualContentAsString = file.readText(charset)
            Assertions.assertEquals(expectedContentAsString, actualContentAsString)
        }

        private fun testCalcChunkSize(totalSize: Long, maxChunkSize: Int) {
            val chunk = calcChunkSize(totalSize, maxChunkSize)
            val rest = totalSize - chunk * (totalSize / chunk)
            Assertions.assertTrue(rest <= chunk) {
                "total=$totalSize, max=$maxChunkSize, chunk=$chunk, rest=$rest"
            }
        }
    }
}