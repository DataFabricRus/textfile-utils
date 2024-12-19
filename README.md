## TextFile Utils

[![](https://jitpack.io/v/DataFabricRus/textfile-utils.svg)](https://jitpack.io/#DataFabricRus/textfile-utils)

A simple JVM library for working with text files of any size.
The library is based on Java NIO (i.e. `java.nio.channels.SeekableByteChannel`) and Kotlin Coroutines.

The library allows sorting an arbitrary file that can be divided into byte blocks by some delimiter.
After sorting, these blocks can be found using a binary search algorithm.
The library takes care of memory consumption, performance and diskspace, so it is suitable for environments with limited resources.
Large CSV-files are one example where this library could be used.
The library is lightweight, so it can be used if there is no possibility to use heavy frameworks or databases.

Contains the following utils:

- insertion at an arbitrary position in the file
- reading text lines from the end or start of the file
- files merging
- determining if a file is sorted
- invert file content
- sorting large text files with memory O(1) and no additional diskspace (optionally)
- binary search in sorted file

#### MergeSort:
```kotlin
fun sort(
    source: Path,                       // existing regular file
    target: Path,                       // result file, must not exist
    comparator: Comparator<String>,     // to compare lines, by default lexicographically
    delimiter: String,                  // default: `\n`
    allocatedMemorySizeInBytes: Int,    // the approximate allowed memory consumption
    controlDiskspace: Boolean,          // if `true` source file will be truncated while process
    charset: Charset,                   // default: UTF8
    coroutineContext: CoroutineContext, // default: Dispatchers.IO
)
```
#### BinarySearch:
```kotlin
fun binarySearch(
    source: Path,                       // existing regular file
    searchLine: String,                 // pattern to search
    buffer: ByteBuffer,                 // to be used while reading data from file
    charset: Charset,                   // default: UTF8   
    delimiter: String,                  // default: `\n`
    comparator: Comparator<String>,     // to compare lines, by default lexicographically
    maxOfLinesPerBlock: Int,            // maximum number of lines in a paragraph 
    maxLineLengthInBytes: Int,          // maximum length of line
): Pair<Long, List<String>>
```

#### Available via maven-central:
```kotlin
dependencies {
    implementation("io.github.datafabricrus:text-file-utils:{{latest_version}}")
}
```

### Apache License Version 2.0