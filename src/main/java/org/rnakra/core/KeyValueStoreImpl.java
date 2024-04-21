package org.rnakra.core;

import org.rnakra.io.DataFile;
import org.rnakra.io.DataFile.Pair;
import org.rnakra.merger.CompactAndMerge;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStoreImpl implements KeyValueStore {
    private final DataFilesManager dataFileManager;

    private enum COMPACT_AND_MERGE_STATE {
        IDLE,
        IN_PROGRESS
    }

    COMPACT_AND_MERGE_STATE compactAndMergeState = COMPACT_AND_MERGE_STATE.IDLE;
    // In memory index to keep track of the location of the key in the data file.
    private final ConcurrentHashMap<String, IndexLocation> memoryIndex;

    public KeyValueStoreImpl() throws FileNotFoundException {
        this.dataFileManager = new DataFilesManager();
        this.memoryIndex = new ConcurrentHashMap<String, IndexLocation>();
        loadIndexes();
    }

    public void put(String key, String value) throws IOException, NoSuchAlgorithmException {
//        System.out.println("Putting key: " + key + " value: " + value);
        IndexLocation location = dataFileManager.getCurrentDataFile().appendEntry(key, value);
        memoryIndex.put(key, location);
    }

    private void loadIndexes() {
        try {
            List<DataFile> files = dataFileManager.getDataFiles();
            for(DataFile file: files) {
                List<DataFile.Entry> entries = file.readEntries();
                for(DataFile.Entry entry: entries) {
                    memoryIndex.put(entry.key, new IndexLocation(file.getFileName(), entry.offset));
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String get(String key) throws IOException {
//        System.out.println("Getting key: " + key);

        IndexLocation location = memoryIndex.get(key);
        if (location == null) {
            return null;
        }
        DataFile dataFile = dataFileManager.getDataFile(location.getFileName());
        return dataFile.readEntry(location);
    }

    public void compactAndMerge() {
        synchronized (this) {
            if(compactAndMergeState == COMPACT_AND_MERGE_STATE.IN_PROGRESS) {
//                System.out.println("Compaction and merge already in progress");
                return;
            }
            compactAndMergeState = COMPACT_AND_MERGE_STATE.IN_PROGRESS;
        }
        List<DataFile> files = dataFileManager.getFilesForMerging();
        if(files.size() > 1) {
            System.out.println("Merging files " + files.get(0).getFileName() + " and "
                    + files.get(1).getFileName() + " into " + files.get(1).getFileName() + " and deleting "
                    + " and " + files.get(0).getFileName() + " from disk");
            DataFile file1 = files.get(0);
            DataFile file2 = files.get(1);
            CompactAndMerge.merge(memoryIndex, file1, file2, dataFileManager);
        } else {
//            System.out.println("No files to merge");
        }
        synchronized (this) {
            compactAndMergeState = COMPACT_AND_MERGE_STATE.IDLE;
        }
    }
}
//saalsmlasmalsmdlasmdlasmdlkasmdlkasmlkdmaldmalskmdlasmlaksmlkasmlkamclkmsclkamlckamlcmalkmlkasmclkamcladmclasllaksmalcmlamclamclakmclksamclkmalsklasnlcnalcnaknsclaskncalsncalkscnalcnalsnc