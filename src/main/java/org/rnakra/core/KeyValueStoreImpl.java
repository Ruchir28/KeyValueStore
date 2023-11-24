package org.rnakra.core;

import org.rnakra.io.DataFile;
import org.rnakra.io.DataFile.Pair;
import org.rnakra.merger.CompactAndMerge;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStoreImpl implements KeyValueStore {
    private final DataFilesManager dataFileManager;
    // In memory index to keep track of the location of the key in the data file.
    private final ConcurrentHashMap<String, IndexLocation> memoryIndex;

    public KeyValueStoreImpl() throws FileNotFoundException {
        this.dataFileManager = new DataFilesManager();
        this.memoryIndex = new ConcurrentHashMap<String, IndexLocation>();
        loadIndexes();
    }

    public void put(String key, String value) throws IOException {
        System.out.println("Putting key: " + key + " value: " + value);
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
        System.out.println("Getting key: " + key);
        IndexLocation location = memoryIndex.get(key);
        if (location == null) {
            return null;
        }
        DataFile dataFile = dataFileManager.getDataFile(location.getFileName());
        return dataFile.readEntry(location);
    }

    public void compactAndMerge(String file1Path, String file2Path) {
//        CompactAndMerge.merge(memoryIndex, file1Path, file2Path, da);
    }
}
//saalsmlasmalsmdlasmdlasmdlkasmdlkasmlkdmaldmalskmdlasmlaksmlkasmlkamclkmsclkamlckamlcmalkmlkasmclkamcladmclasllaksmalcmlamclamclakmclksamclkmalsklasnlcnalcnaknsclaskncalsncalkscnalcnalsnc