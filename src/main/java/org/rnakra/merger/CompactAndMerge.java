package org.rnakra.merger;

import org.rnakra.core.IndexLocation;
import org.rnakra.io.DataFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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
    public static void merge(ConcurrentHashMap<String, IndexLocation> memoryIndex, String file1Path, String file2Path, DataFile dataFile) {
        try {
            File file1 = new File(file1Path);
            File file2 = new File(file2Path);
            List<DataFile.Entry> entries1 = dataFile.readEntries(file1);
            List<DataFile.Entry> entries2 = dataFile.readEntries(file2);
            File fileToKeepName = file1Path.compareTo(file2Path) > 0 ? file1 : file2;
            File fileToDeleteName = file1Path.compareTo(file2Path) > 0 ? file2 : file1;
            // write to a temporary file first , which will be renamed afterward, to avoid data loss in case process crashes
            ConcurrentHashMap<String, IndexLocation> tempMemoryIndex = new ConcurrentHashMap<String, IndexLocation>();
            File tempFile = new File(fileToKeepName.getAbsolutePath() + ".tmp");
            tempFile.createNewFile();
            DataFile tempDataFile = new DataFile(tempFile.getName());
            for(DataFile.Entry entry: entries1) {
                if(memoryIndex.get(entry.key).getFileName().equals(file1.getName()) && memoryIndex.get(entry.key).getOffset() == entry.offset) {
                    IndexLocation indexLocation = tempDataFile.appendEntryWhileMerging(entry.key, entry.value);
                    tempMemoryIndex.put(entry.key, new IndexLocation(fileToKeepName.getName(), indexLocation.getOffset()));
                }
            }
            for(DataFile.Entry entry: entries2) {
                if(memoryIndex.get(entry.key).getFileName().equals(file2.getName()) && memoryIndex.get(entry.key).getOffset() == entry.offset) {
                    IndexLocation indexLocation = tempDataFile.appendEntryWhileMerging(entry.key, entry.value);
                    tempMemoryIndex.put(entry.key, new IndexLocation(fileToKeepName.getName(), indexLocation.getOffset()));
                }
            }

            // rename the temporary file to the file to keep
            tempFile.renameTo(fileToKeepName);
            // delete the file to delete
            fileToDeleteName.delete();
            // update the main index
            for(Map.Entry<String,IndexLocation> entry: tempMemoryIndex.entrySet()) {
                // timestamp check
                if(memoryIndex.get(entry.getKey()).getFileName().compareTo(entry.getValue().getFileName()) <= 0) {
                    System.out.println("Updating index for key: " + entry.getKey() + " to file: " + entry.getValue().getFileName());
                    memoryIndex.put(entry.getKey(), entry.getValue());
                }
            }


        } catch (IOException e) {
            System.out.println("CompactAndMerge Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
