## TextFile Utils

A simple JVM library for working with text files of any size.
The library is based on Java NIO (i.e. `java.nio.channels.SeekableByteChannel`) and Kotlin Coroutines.

Contains following utils:

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
    source: Path,
    target: Path,
    comparator: Comparator<String>,
    delimiter: String,
    allocatedMemorySizeInBytes: Int,
    controlDiskspace: Boolean,
    charset: Charset,
    coroutineContext: CoroutineContext,
)
```
#### BinarySearch:
```kotlin
fun binarySearch(
    source: Path,
    searchLine: String,
    buffer: ByteBuffer,
    charset: Charset,
    delimiter: String,
    comparator: Comparator<String>,
    maxOfLinesPerBlock: Int,
    maxLineLengthInBytes: Int,
): Pair<Long, List<String>>
```
#### Usage:

Available via [jitpack](https://jitpack.io/#DataFabricRus/textfile-utils):
```kotlin
allprojects {
    repositories {
        ...
        maven { url 'https://jitpack.io' }
    }
}

dependencies {
    implementation 'com.github.DataFabricRus:textfile-utils:1.0-SNAPSHOT'
}
```

### Apache License Version 2.0