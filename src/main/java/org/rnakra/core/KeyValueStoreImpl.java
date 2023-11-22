package org.rnakra.core;

import org.rnakra.io.DataFile;
import org.rnakra.io.DataFile.Pair;
import org.rnakra.merger.CompactAndMerge;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStoreImpl implements KeyValueStore {
    private final DataFile dataFile;
    // In memory index to keep track of the location of the key in the data file.
    private final ConcurrentHashMap<String, IndexLocation> memoryIndex;

    public KeyValueStoreImpl() throws FileNotFoundException {
        this.dataFile = new DataFile();
        this.memoryIndex = new ConcurrentHashMap<String, IndexLocation>();
        loadIndexes();
    }

    public void put(String key, String value) throws IOException {
        System.out.println("Putting key: " + key + " value: " + value);
        IndexLocation location = dataFile.appendEntry(key, value);
        memoryIndex.put(key, location);

    }

    private void loadIndexes() {
        try {
            File[] files = dataFile.getFiles();
            for(File file: files) {
                long offset = 0;
                while(true) {
                    Pair pair = dataFile.readKeyAndNextOffset(file,offset);
                    if(pair == null || pair.key == null || pair.offset == null) {
                        break;
                    }
                    memoryIndex.put(pair.key, new IndexLocation(file.getName(), offset));
                    offset = pair.offset;
                }
            }
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }

    public String get(String key) throws IOException {
        System.out.println("Getting key: " + key);
        IndexLocation location = memoryIndex.get(key);
        if (location == null) {
            return null;
        }
        return dataFile.readEntry(location);
    }

    public void compactAndMerge(String file1Path, String file2Path) {
        CompactAndMerge.merge(memoryIndex, file1Path, file2Path, dataFile);
    }
}
//saalsmlasmalsmdlasmdlasmdlkasmdlkasmlkdmaldmalskmdlasmlaksmlkasmlkamclkmsclkamlckamlcmalkmlkasmclkamcladmclasllaksmalcmlamclamclakmclksamclkmalsklasnlcnalcnaknsclaskncalsncalkscnalcnalsnc