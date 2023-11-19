package org.rnakra.merger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Summary: Race Condition Handling in Key-Value Store Merge Process
 * Context:
 * Implementing a merge process for a key-value store with data files capped at 10 MB each.
 * Files are named with timestamps to indicate their creation time.
 * Concerned about race conditions where new writes could occur during the merging process.
 * Solution Approach:
 *
 * Timestamp-Based File Naming:
 *
 * Data files are named with timestamps, providing a chronological record of their creation.
 * Merge Process:
 *
 * Merge involves consolidating data from two files into one, choosing the file with the latest data based on the timestamp, and then deleting the other file.
 * Index Update Strategy:
 *
 * When updating the main index after merging, the process checks the timestamp in the current index entry against the timestamp of the file from which the data is being merged.
 * The index is updated only if it points to a file with a timestamp equal to or older than the merged file's timestamp.
 * If the index points to a newer file (indicating a write occurred after the merge started), the index is not updated for that key, preserving the latest data..
 */

public class CompactAndMerge {
    public static void merge(ConcurrentHashMap<String, Long> memoryIndex, String file1, String file2) {

    }
}
