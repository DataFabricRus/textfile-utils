package cc.datafabric.textfileutils

import cc.datafabric.textfileutils.iterators.byteArrayPrefixSimpleComparator
import cc.datafabric.textfileutils.iterators.byteArraySimpleComparator
import cc.datafabric.textfileutils.iterators.toStringComparator
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource


internal class ComparatorsTest {

    @Test
    fun `test byte-array-simple comparator`() {
        val data = """
            1e3ed44a-36ab-4733-b3a0-cf04af5d1337:3:1373
            d308ecfb-d846-40a7-90ce-8bcff197c8a9:2:9187
            73ba239f-acca-4f8f-b812-6795132377f9:4:5099
            dd9dcf52-afb9-499b-80b8-5255657c74e0:4:9688
            29ea1e7b-f491-46ee-ae4c-3302052483fb:3:1874
            d84ad1dd-8707-4937-b552-c91b4bff299e:1:9409
            5e832c9d-93e4-448d-ac8b-3a4eb0258a29:2:4198
            3941a78f-10b0-4ff1-8d4b-4374ce991ab9:4:2544
            4f77c013-2182-4ba2-8586-f3d37694b740:4:3524
            e0957fd2-5830-4748-961b-120495eaa2bd:4:9813
            185266b2-514a-437c-a983-828d392c0c5b:4:1076
            5771f49b-ce24-4615-aecd-4816ac9aa2e3:4:3887
            dbab47f8-2bef-41ea-9116-c8410a73008a:1:9587
            df90e041-d0e3-4785-bab8-ba27d59bb739:4:9774
        """.trimIndent().split("\n")

        val res = data.map { it.toByteArray(Charsets.UTF_8) }
            .sortedWith(byteArraySimpleComparator())
            .map { it.toString(Charsets.UTF_8) }

        Assertions.assertEquals(data.sorted(), res)
    }

    @Test
    fun `test byte-array-prefixed-simple comparator #1`() {
        val data = """
            
            #_ffdf27f2-9acf-4a39-9c9d-66aa77b37ba3|A
            #_ffe4d7f7-fe0f-4788-931e
            #_ffdf5659-17d3-4de2-9397-7fbef06ed785|B
            #_ffe4adb7-3e1c-435c-ab1f-11d58b78e227|C
            #_ffdf5659-17d3-4de2-9397-7fbef06ed785
            #_ffe4d7f7-fe0f-4788-931e-99c09a588a5c|DX
            #_fffa83e1-6dd7-41c7-8878-415dd02068ba|E
            
            #_ffe4adb7-3e1c-435c-ab1f-11d58b78e227|
            
            #_ffe4d7f7-fe0f-4788
            
        """.trimIndent().split("\n")

        val actual = data.map { it.toByteArray(Charsets.UTF_8) }
            .sortedWith(
                byteArrayPrefixSimpleComparator("|".toByteArray(Charsets.UTF_8))
            )
            .map { it.toString(Charsets.UTF_8) }

        val expected = data.sortedWith { a, b ->
            a.substringBefore("|").compareTo(b.substringBefore("|"))
        }
        Assertions.assertEquals(expected, actual)
    }

    @ParameterizedTest
    @ValueSource(strings = ["", "|", "||", "|||", "||||", "!", "a", "b", "c", "der", " "])
    fun `test byte-array-prefixed-simple comparator #2`(delimiter: String) {
        val data = listOf(
            "aa||A||xx",
            "AA||A||xx",
            "aa",
            "aa||||||||AA",
            "ab|cd",
            "||abcd|ef",
            "||xxx",
            "||xxx",
            "AA||A|xx",
            "|der",
        )

        val actual = data.map { it.toByteArray(Charsets.UTF_8) }
            .sortedWith(
                byteArrayPrefixSimpleComparator(delimiter.toByteArray(Charsets.UTF_8))
            )
            .map { it.toString(Charsets.UTF_8) }

        val expected = data.sortedWith { a, b ->
            a.substringBefore(delimiter).compareTo(b.substringBefore(delimiter))
        }
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `test byte-array-prefixed-simple-string comparator`() {
        val data = listOf("e", "b", "c", "d", "e", "f", "g", "h", "i", "a")

        val actual = data.sortedWith(byteArraySimpleComparator().toStringComparator(Charsets.UTF_8))

        val expected = data.sorted()
        Assertions.assertEquals(expected, actual)
    }
}