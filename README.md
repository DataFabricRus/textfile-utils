## TextFile Utils

A simple JVM library for working with text files of any size.
The library is based on Java NIO (i.e. `java.nio.channels.SeekableByteChannel`) and Kotlin Coroutines.

Contains following utils:

- inserting at the beginning of file
- reading text lines from the end or start of the file
- files merging
  TODO:
- sorting large text files with memory O(1) and no additional diskspace
- binary search in sorted file