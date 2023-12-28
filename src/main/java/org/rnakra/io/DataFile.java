package org.rnakra.io;
import org.rnakra.core.IndexLocation;
import org.rnakra.listener.DataFileSizeListener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

// TODO: Maintain instances of ReadFile and WriteFile or some optimization around em
public class DataFile {
//    public static final int MAX_FILE_SIZE = 1000000; // 1 MB

    public static final int MAX_FILE_SIZE = 1000; // 1 MB

    public static class Pair {
        public String key;
        public Long offset;
        Pair(String key, Long offset) {
            this.key = key;
            this.offset = offset;
        }
    }

    public static class Entry {
        public String key;
        public String value;
        public long offset;
        Entry(String key, String value, long offset) {
            this.key = key;
            this.value = value;
            this.offset = offset;
        }
    }

    private File file;
    private RandomAccessFile storeFile;

    private List<DataFileSizeListener> dataFileSizeListeners;

    public DataFile(String fileName) throws FileNotFoundException {
        this.file = new File(fileName); // Default file path
        this.storeFile = new RandomAccessFile(this.file, "rw");
        this.dataFileSizeListeners = new ArrayList<>();
    }

    public DataFile(File file) throws FileNotFoundException {
        this.file = file;
        this.storeFile = new RandomAccessFile(this.file,"rw");
        this.dataFileSizeListeners = new ArrayList<>();
    }

    public void refresh() throws FileNotFoundException {
        try {
            this.storeFile.close();
        } catch (IOException e) {
            System.err.println("Error in closing file: " + this.file.getName());
        }
        this.storeFile = new RandomAccessFile(this.file,"rw");
    }

    public void addDataFileSizeListener(DataFileSizeListener dataFileSizeListener) {
        this.dataFileSizeListeners.add(dataFileSizeListener);
    }

    public void removeDataFileSizeListener(DataFileSizeListener dataFileSizeListener) {
        this.dataFileSizeListeners.remove(dataFileSizeListener);
    }

    public void notifyDataFileSizeListeners() {
        for(DataFileSizeListener dataFileSizeListener: this.dataFileSizeListeners) {
            dataFileSizeListener.onFileSizeExceeded(this);
        }
    }

    public String getFileName() {
        return this.file.getName();
    }
    public synchronized IndexLocation appendEntry(String key, String value) throws IOException {
        storeFile.seek(storeFile.length()); // Move to the end of the file

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

        dataOutputStream.writeInt(keyBytes.length); // Key size
        dataOutputStream.writeInt(valueBytes.length); // Value size
        dataOutputStream.write(keyBytes); // Key data
        dataOutputStream.write(valueBytes); // Value data

        byte[] entryBytes = outputStream.toByteArray();
        storeFile.write(entryBytes); // Write the whole entry

        // check if size is greater than 1MB start writing to a new file

        long offset =  storeFile.length() - entryBytes.length; // Return the start offset of this entry
        IndexLocation indexLocation = new IndexLocation(this.file.getName(), offset);

        // if file size exceeds the threshold fire the event

        if(storeFile.length() > MAX_FILE_SIZE) {
            notifyDataFileSizeListeners();
        }

        return indexLocation;
    }


    public synchronized boolean deleteFile() throws IOException {
        return this.file.delete();
    }

    public synchronized boolean renameFile(String newName) throws IOException {
        return this.file.renameTo(new File(newName));
    }

    public File getFile() {
        return this.file;
    }

    public synchronized IndexLocation appendEntryWhileMerging(String key, String value) throws IOException {
        storeFile.seek(storeFile.length()); // Move to the end of the file

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

        dataOutputStream.writeInt(keyBytes.length); // Key size
        dataOutputStream.writeInt(valueBytes.length); // Value size
        dataOutputStream.write(keyBytes); // Key data
        dataOutputStream.write(valueBytes); // Value data

        byte[] entryBytes = outputStream.toByteArray();
        storeFile.write(entryBytes); // Write the whole entry

        long offset =  storeFile.length() - entryBytes.length; // Return the start offset of this entry
        return new IndexLocation(this.file.getName(), offset);
    }

    public synchronized void updateFile(String newFileName) throws IOException {
        this.file = new File(newFileName);
        this.storeFile = new RandomAccessFile(this.file,"rw");
    }

    public synchronized String readEntry(IndexLocation indexLocation) throws IOException {
        storeFile.seek(indexLocation.getOffset()); // Move to the start of the entry

        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(storeFile.getFD()));

        int keySize = dataInputStream.readInt();
        int valueSize = dataInputStream.readInt();

        byte[] keyBytes = new byte[keySize];
        byte[] valueBytes = new byte[valueSize];

        dataInputStream.read(keyBytes);
        dataInputStream.read(valueBytes);

        return new String(valueBytes, StandardCharsets.UTF_8);
    }


    public synchronized List<Entry> readEntries() throws IOException {
        storeFile.seek(0); // Move to the start of the entry
        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(storeFile.getFD()));
        List<Entry> entries = new ArrayList<>();
        if(file.length() == 0) {
            return entries;
        }
        long offset = 0;
        while(true) {
            int keySize = dataInputStream.readInt();
            int valueSize = dataInputStream.readInt();

            byte[] keyBytes = new byte[keySize];
            byte[] valueBytes = new byte[valueSize];

            dataInputStream.read(keyBytes);
            dataInputStream.read(valueBytes);
            entries.add(new Entry(new String(keyBytes, StandardCharsets.UTF_8), new String(valueBytes, StandardCharsets.UTF_8), offset));
            offset = offset + 8 + keySize + valueSize;
            if(offset >= storeFile.length()) {
                break;
            }
        }
        return entries;
    }

    public synchronized String readKey(IndexLocation indexLocation) throws IOException {
        storeFile.seek(indexLocation.getOffset()); // Move to the start of the entry

        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(storeFile.getFD()));

        int keySize = dataInputStream.readInt();
        int valueSize = dataInputStream.readInt();

        byte[] keyBytes = new byte[keySize];
        byte[] valueBytes = new byte[valueSize];

        dataInputStream.read(keyBytes);
        dataInputStream.read(valueBytes);

        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    private void createDirectoryIfNotExists(String directoryPath) {
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

}

