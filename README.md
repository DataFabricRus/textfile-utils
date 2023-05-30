## TextFile Utils

A simple JVM library for working with text files of any size.
The library is based on Java NIO (i.e. `java.nio.channels.SeekableByteChannel`) and Kotlin Coroutines.

Contains following utils:

- insert at an arbitrary position in the file
- reading text lines from the end or start of the file
- files merging
- a method to determine if a file is sorted
- invert file content
  TODO:
- sorting large text files with memory O(1) and no additional diskspace
- binary search in sorted file