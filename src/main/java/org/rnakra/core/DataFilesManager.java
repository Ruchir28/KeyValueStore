package org.rnakra.core;

import org.rnakra.io.DataFile;
import org.rnakra.listener.DataFileSizeListener;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DataFilesManager implements DataFileSizeListener {

    ReadWriteLock readWriteLock;
    private final String defaultDirectory = "data"; // Default directory
    private List<DataFile> dataFiles;

    private DataFile currentDataFile;

    public DataFilesManager() throws FileNotFoundException {
        try {
            File directory = new File(defaultDirectory);
            if(directory.exists() && directory.isDirectory() && directory.listFiles().length > 0) {
                List<File> files = Arrays.stream(directory.listFiles()).filter(File::isFile).filter(file -> file.getName().endsWith(".db")).collect(Collectors.toList());
                this.dataFiles = files.stream().map(file -> {
                    try {
                        return new DataFile(file);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).filter(Objects::nonNull).collect(Collectors.toList());
            }
            if(this.dataFiles == null || this.dataFiles.isEmpty()) {
                File file = new File(defaultDirectory, Instant.now().toEpochMilli() + ".db");
                file.createNewFile();
                DataFile dataFile = new DataFile(file);
                this.currentDataFile = dataFile;
                this.dataFiles.add(dataFile);
            }
            this.currentDataFile = this.dataFiles.get(this.dataFiles.size() - 1);
            // adding the listener
            this.currentDataFile.addDataFileSizeListener(this);

            this.readWriteLock = new ReentrantReadWriteLock();
        } catch (Exception e) {
            System.out.println("DataFiles Manager Initilization Exception" + e.getMessage());
        }
    }

    public DataFile getCurrentDataFile() {
        return currentDataFile;
    }

    @Override
    public void onFileSizeExceeded(DataFile dataFile) {
        try {
            // Only for current file
            if(dataFile != this.currentDataFile) {
                return;
            }
            File file = new File(defaultDirectory, Instant.now().toEpochMilli() + ".db");
            DataFile newDataFile = new DataFile(file);
            this.readWriteLock.writeLock().lock();
            this.dataFiles.add(newDataFile);
            this.readWriteLock.writeLock().unlock();
            this.currentDataFile = newDataFile;
            // adding the listener
            this.currentDataFile.addDataFileSizeListener(this);
        } catch (FileNotFoundException e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }

    public List<DataFile> getDataFiles() {
        return dataFiles;
    }

    public DataFile getDataFile(String fileName) {
        this.readWriteLock.readLock().lock();
        DataFile file = null;
        for(DataFile dataFile: this.dataFiles) {
            if(dataFile.getFileName().equals(fileName)) {
                file = dataFile;
                break;
            }
        }
        this.readWriteLock.readLock().unlock();
        return file;
    }
}
