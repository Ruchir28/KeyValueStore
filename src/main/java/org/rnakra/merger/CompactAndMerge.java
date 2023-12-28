package org.rnakra.merger;

import org.rnakra.core.DataFilesManager;
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
    public static synchronized void merge(ConcurrentHashMap<String, IndexLocation> memoryIndex, DataFile dataFile1, DataFile dataFile2, DataFilesManager dataFilesManager) {
        try {
            List<DataFile.Entry> entries1 = dataFile1.readEntries();
            List<DataFile.Entry> entries2 = dataFile2.readEntries();
            DataFile fileToKeepName = dataFile1.getFileName().compareTo(dataFile2.getFileName()) > 0 ? dataFile1 : dataFile2;
            DataFile fileToDeleteName = dataFile1.getFileName().compareTo(dataFile2.getFileName()) > 0 ? dataFile2 : dataFile1;
            // write to a temporary file first , which will be renamed afterward, to avoid data loss in case process crashes
            ConcurrentHashMap<String, IndexLocation> tempMemoryIndex = new ConcurrentHashMap<String, IndexLocation>();

            String newfileName = Long.toString(Long.parseLong(fileToKeepName.getFileName().split("\\.")[0])  + 1) + ".db";
            System.out.println("Creating new file: " + newfileName);
            File tempFile = new File("data",newfileName);
            if(!tempFile.createNewFile()) {
                System.out.println("Failed to create File" + tempFile.getName());
                return;
            } else {
                System.out.println("Created file" + tempFile.getName());
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
            // update the main index
            for(Map.Entry<String,IndexLocation> entry: tempMemoryIndex.entrySet()) {
                // timestamp check

                if(memoryIndex.get(entry.getKey()).getFileName().compareTo(entry.getValue().getFileName()) <= 0) {
//                    System.out.println("Updating index for key: " + entry.getKey() + " to file: " + entry.getValue().getFileName());
                    memoryIndex.put(entry.getKey(), entry.getValue());
                }
            }
            // rename the temporary file to the file to keep
            // tempDataFile.renameFile(fileToKeepName.getFileName());
            // delete the file to delete
//            tempFile.renameTo(new File("data/" + fileToKeepName.getFileName()));
            dataFilesManager.addDataFile(tempDataFile);
            dataFilesManager.removeDataFile(fileToDeleteName);
            dataFilesManager.removeDataFile(fileToKeepName);
            // update the data file manager
            fileToKeepName.deleteFile();
            fileToDeleteName.deleteFile();
        } catch (IOException e) {
            System.out.println("CompactAndMerge Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
