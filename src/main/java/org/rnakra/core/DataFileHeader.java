package org.rnakra.core;

import org.rnakra.io.DataFile;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * DataFileHeader is the header of the data file, which contains the metadata of the file.
 * The header is written at the beginning of the file.
 * Caution: Methods in this class are not thread safe, and should be called only when the file is locked.
 */
abstract public class DataFileHeader {

    /**
     * HEADER STRUCTURE
     * 0 - 3: Magic number
     * 4: File state
     * 5 - 20: Checksum (MD5)
     */
    protected static final int HEADER_SIZE = 21; // Magic number + File state + Checksum

    /**
     * File state: 0 - Normal, 1 - Deleted
     */
    private byte fileState = (byte)(0); // 1 byte
    private byte[] checksum = new byte[16]; // MD5 checksum
    private DataFile file;

    /**
     * Write the header to the file, This method is not synchronized hence should be only called when,
     * lock on a file is acquired, or the file is not being accessed by any other thread.
     * @param file File to write the header to
     * @throws IOException In case of any IO error
     * @throws NoSuchAlgorithmException In case of any error in calculating checksum
     */
    public void writeHeader(RandomAccessFile file) throws IOException, NoSuchAlgorithmException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.putInt(0x1234ABCD); // Magic number
        this.fileState = 0;
        this.checksum = calculateChecksum(file);
        buffer.put((byte)0); // File state: 0
        buffer.put(calculateChecksum(file)); // Calculate and store checksum
        file.seek(0); // Start at the beginning of the file
        file.write(buffer.array());
    }

    public byte getFileState() {
        return fileState;
    }

    public void updateFileState(RandomAccessFile file,byte fileState) throws IOException {
        this.fileState = fileState;
        file.seek(4);
        file.writeByte(fileState);
    }

    private byte[] calculateChecksum(RandomAccessFile file) throws IOException, NoSuchAlgorithmException {

        MessageDigest md = MessageDigest.getInstance("MD5");
        file.seek(HEADER_SIZE);
        byte[] dataBytes = new byte[1024];
        int nread = 0;
        while ((nread = file.read(dataBytes)) != -1) {
            md.update(dataBytes, 0, nread);
        }
        return md.digest();

    }

    public void readHeader(RandomAccessFile file) throws IOException {
        file.seek(0);
        byte[] headerBytes = new byte[HEADER_SIZE];
        file.readFully(headerBytes);
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        int magicNumber = buffer.getInt();
        fileState = buffer.get();
        buffer.get(checksum);
    }
}
