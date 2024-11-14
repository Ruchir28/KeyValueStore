package org.rnakra.merger;

import org.rnakra.core.DataFilesManager;
import org.rnakra.core.IndexLocation;
import org.rnakra.io.DataFile;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Summary: Race Condition Handling in Key-Value Store Merge Process
 * 
 * Context:
 * Implementing a merge process for a key-value store with data files capped at 10 MB each.
 * Files are named with timestamps to indicate their creation time.
 * Concerned about race conditions where new writes could occur during the merging process.
 * 
 * Solution Approach:
 * 1. Timestamp-Based File Naming:
 *    - Data files are named with format: timestamp.version.db
 *    - Timestamp provides chronological record of creation
 *    - Version numbers ensure safe concurrent access during merges
 * 
 * 2. Merge Process:
 *    - Consolidates data from two files into a new versioned file
 *    - Chooses latest data based on timestamp
 *    - Maintains both old and new files until index updates complete
 *    - Soft deletes old files only after safe transition
 * 
 * 3. Index Update Strategy:
 *    - New merged file is made available before index updates begin
 *    - Updates index only if it points to same or older file (timestamp comparison)
 *    - If index points to newer file (indicating concurrent write), preserves that entry
 *    - Ensures no data loss during concurrent operations
 * 
 * File Naming Example:
 * Original files: 1234567890.1.db, 1234567890.2.db
 * Merged file:    1234567890.3.db
 * 
 * Safety Mechanisms:
 * 1. Version numbers prevent file content corruption during merges
 * 2. Soft deletion ensures ongoing reads complete successfully
 * 3. Atomic index updates preserve consistency
 * 4. Timestamp comparisons prevent data loss from concurrent writes
 */

public class CompactAndMerge {
    /**
     * Performs thread-safe merge of two data files while handling concurrent reads and writes.
     * 
     * @param memoryIndex The concurrent hash map storing key to file location mappings
     * @param dataFile1 First file to merge
     * @param dataFile2 Second file to merge
     * @param dataFilesManager Manager handling active data files for reads/writes
     */
    public static synchronized void merge(ConcurrentHashMap<String, IndexLocation> memoryIndex, DataFile dataFile1, DataFile dataFile2, DataFilesManager dataFilesManager) {
        try {
            List<DataFile.Entry> entries1 = dataFile1.readEntries();
            List<DataFile.Entry> entries2 = dataFile2.readEntries();
            DataFile fileToKeepName = dataFile1.getFileName().compareTo(dataFile2.getFileName()) > 0 ? dataFile1 : dataFile2;
            DataFile fileToDeleteName = dataFile1.getFileName().compareTo(dataFile2.getFileName()) > 0 ? dataFile2 : dataFile1;
            // write to a temporary file first , which will be renamed afterward, to avoid data loss in case process crashes
            ConcurrentHashMap<String, IndexLocation> tempMemoryIndex = new ConcurrentHashMap<String, IndexLocation>();

            // extract version number from the file to keep, if file does not have a integer version number, set it to 0

            int version_number = 0;
            try {
                version_number = Integer.parseInt(fileToKeepName.getFileName().split("\\.")[1]);
            } catch (NumberFormatException e) {
                // If the version number is not an integer, set it to 0
            }

            String newfileName = Long.toString(Long.parseLong(fileToKeepName.getFileName().split("\\.")[0])) + "."+ ((version_number + 1)) +".db";

            // System.out.println("Creating new file: " + newfileName);
            File tempFile = new File("data",newfileName);

            if(!tempFile.createNewFile()) {
                System.out.println("Failed to create File " + tempFile.getName());
                return;
            } else {
                // System.out.println("Created file" + tempFile.getName());
            }
            DataFile tempDataFile = new DataFile(tempFile);
            for(DataFile.Entry entry: entries1) {
                if(memoryIndex.get(entry.key).getFileName().equals(dataFile1.getFileName()) && memoryIndex.get(entry.key).getOffset() == entry.offset) {
                    IndexLocation indexLocation = tempDataFile.appendEntryWhileMerging(entry.key, entry.value);
                    tempMemoryIndex.put(entry.key, new IndexLocation(tempDataFile.getFileName(), indexLocation.getOffset()));
                }
            }
            for(DataFile.Entry entry: entries2) {
                if(memoryIndex.get(entry.key).getFileName().equals(dataFile2.getFileName()) && memoryIndex.get(entry.key).getOffset() == entry.offset) {
                    IndexLocation indexLocation = tempDataFile.appendEntryWhileMerging(entry.key, entry.value);
                    tempMemoryIndex.put(entry.key, new IndexLocation(tempDataFile.getFileName(), indexLocation.getOffset()));
                }
            }

            // add the merged file to the data file manager, so that it is ready to be used
            // and then only update the index, we have both the merged files and the unmerged file available
            // tile the index update is done, in case we get read request in b/w and index is not updated yet it can be
            // read from the old files too
            dataFilesManager.addDataFile(tempDataFile);


            // update the main index
            for(Map.Entry<String,IndexLocation> entry: tempMemoryIndex.entrySet()) {

                if(memoryIndex.get(entry.getKey()).getFileName().compareTo(entry.getValue().getFileName()) <= 0) {
                    memoryIndex.put(entry.getKey(), entry.getValue());
                }
            }

            // Can't immediately delete the files because there might be read requests in between

            fileToKeepName.softdeleteFile(); // Mark the file as deleted
            fileToDeleteName.softdeleteFile(); // Mark the file as deleted

            // System.out.println("MERGE AND COMPACT SUCCESSFUL");
        } catch (IOException | NoSuchAlgorithmException e) {
            System.out.println("CompactAndMerge Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
