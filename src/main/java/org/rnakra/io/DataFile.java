package org.rnakra.io;
import org.rnakra.core.IndexLocation;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.time.Instant;

public class DataFile {

    public class Pair {
        public String key;
        public Long offset;
        Pair(String key, Long offset) {
            this.key = key;
            this.offset = offset;
        }
    }

    private final String defaultDirectory = "data"; // Default directory
    private File file;
    private RandomAccessFile storeFile;

    public DataFile() throws FileNotFoundException {
        this.file = new File(defaultDirectory, Instant.now().toEpochMilli() + ".db"); // Default file path
        createDirectoryIfNotExists(defaultDirectory);
        this.storeFile = new RandomAccessFile(this.file, "rw");
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
        if(storeFile.length() > 100) {
            this.file = new File(defaultDirectory, Instant.now().toEpochMilli() + ".db"); // Default file path
            createDirectoryIfNotExists(defaultDirectory);
            this.storeFile = new RandomAccessFile(this.file, "rw");
        }

        return indexLocation;
    }

    public synchronized String readEntry(IndexLocation indexLocation) throws IOException {
        RandomAccessFile file = new RandomAccessFile(new File(defaultDirectory, indexLocation.getFileName()), "r");
        file.seek(indexLocation.getOffset()); // Move to the start of the entry

        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file.getFD()));

        int keySize = dataInputStream.readInt();
        int valueSize = dataInputStream.readInt();

        byte[] keyBytes = new byte[keySize];
        byte[] valueBytes = new byte[valueSize];

        dataInputStream.read(keyBytes);
        dataInputStream.read(valueBytes);

        return new String(valueBytes, StandardCharsets.UTF_8);
    }

    public synchronized String readKey(IndexLocation indexLocation) throws IOException {
        RandomAccessFile file = new RandomAccessFile(new File(defaultDirectory, indexLocation.getFileName()), "r");
        file.seek(indexLocation.getOffset()); // Move to the start of the entry

        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file.getFD()));

        int keySize = dataInputStream.readInt();
        int valueSize = dataInputStream.readInt();

        byte[] keyBytes = new byte[keySize];
        byte[] valueBytes = new byte[valueSize];

        dataInputStream.read(keyBytes);
        dataInputStream.read(valueBytes);

        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    public synchronized Pair readKeyAndNextOffset(File file,long offSet) throws IOException {
        RandomAccessFile toReadFile = new RandomAccessFile(file, "r");
        if(offSet >= toReadFile.length()) {
            return null;
        }
        toReadFile.seek(offSet); // Move to the start of the entry

        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(toReadFile.getFD()));

        int keySize = dataInputStream.readInt();
        int valueSize = dataInputStream.readInt();

        byte[] keyBytes = new byte[keySize];
        byte[] valueBytes = new byte[valueSize];

        dataInputStream.read(keyBytes);
        dataInputStream.read(valueBytes);

        return new Pair(new String(keyBytes, StandardCharsets.UTF_8), offSet + 8 + keySize + valueSize);
    }

    public File[] getFiles() {
        File folder = new File(defaultDirectory);
        File[] listOfFiles = folder.listFiles();
        return Arrays.stream(listOfFiles).filter(File::isFile).toArray(File[]::new);
    }

    private void createDirectoryIfNotExists(String directoryPath) {
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

}

