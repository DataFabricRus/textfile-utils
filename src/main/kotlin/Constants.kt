package com.gitlab.sszuev.textfiles

const val DEFAULT_BUFFER_SIZE_IN_BYTES = 8192

const val MERGE_FILES_MIN_WRITE_BUFFER_SIZE_IN_BYTES = 8192
const val MERGE_FILES_MIN_READ_BUFFER_SIZE_IN_BYTES = 1024

const val LINE_READER_MAX_LENGTH_IN_BYTES = 8912
const val LINE_READER_SINGLE_OPERATION_TIMEOUT_IN_MS = 60 * 1000L
const val LINE_READER_INTERNAL_QUEUE_SIZE = 1024

const val SORT_FILE_MIN_MEMORY_ALLOCATION_IN_BYTES = 64
const val SORT_FILE_DEFAULT_MEMORY_ALLOCATION_IN_BYTES = 8912
const val SORT_FILE_WRITE_OPERATION_TIMEOUT_IN_MS = 5 * 60 * 1000L
const val SORT_FILE_CHUNK_GAP = 0.2
