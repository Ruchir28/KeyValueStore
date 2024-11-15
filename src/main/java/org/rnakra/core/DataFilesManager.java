package org.rnakra.core;

import org.rnakra.io.DataFile;
import org.rnakra.listener.DataFileSizeListener;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DataFilesManager implements DataFileSizeListener {

    ReadWriteLock readWriteLock;
    private final String defaultDirectory = "data"; // Default directory
    private List<DataFile> dataFiles;

    private DataFile currentDataFile;

    private ScheduledExecutorService scheduler;


    public DataFilesManager() throws FileNotFoundException {
        try {
            File directory = new File(defaultDirectory);

            if(directory.exists() && directory.isDirectory() && directory.listFiles().length > 0) {
                List<File> files = Arrays.stream(directory.listFiles()).filter(File::isFile).filter(file -> file.getName().endsWith(".db")).collect(Collectors.toList());
                this.dataFiles = files.stream().map(file -> {
                    try {
                        return new DataFile(file);
                    } catch (Exception e) {
                        System.err.println("Error occurred in Creating File");
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

            this.scheduler = Executors.newScheduledThreadPool(1);

            scheduler.scheduleAtFixedRate(
                    this::cleanupSoftDeletedFiles,
                    5000, // 5s for now
                    5000, // 5s for now
                    java.util.concurrent.TimeUnit.MILLISECONDS
            );

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
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public List<DataFile> getDataFiles() {
        return dataFiles;
    }

    public void addDataFile(DataFile dataFile) {
        this.readWriteLock.writeLock().lock();
        this.dataFiles.add(dataFile);
        // sorted is ascending order by name
        this.dataFiles.sort(Comparator.comparing(DataFile::getFileName));
        this.readWriteLock.writeLock().unlock();
    }

    public void removeDataFile(DataFile dataFile) {
        this.readWriteLock.writeLock().lock();
        this.dataFiles.removeIf(file -> file.getFileName().equals(dataFile.getFileName()));
        // sorted is ascending order by name
        this.readWriteLock.writeLock().unlock();
    }

    public List<DataFile> getFilesForMerging() {
       readWriteLock.readLock().lock();
       List<DataFile> files = this.dataFiles.stream().filter(dataFile -> dataFile.getFileState() == 0).collect(Collectors.toList());
       readWriteLock.readLock().unlock();
       return files;
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
        if(file == null) {
            System.out.println("File not found: " + fileName);
        }
        this.readWriteLock.readLock().unlock();
        return file;
    }

    public void cleanupSoftDeletedFiles() {
        this.readWriteLock.readLock().lock();
        long currentTime = System.currentTimeMillis();

        for (DataFile file : dataFiles) {
            // If file is safe to delete and beyond the grace period
            if (file.getFile().exists() && (file.getFileState() == 1) && System.currentTimeMillis() - file.getFile().lastModified() > 2000) {
                file.getFile().delete();
                // System.out.println("Deleted file: " + file.getFile().getName());
            }

        }

        this.readWriteLock.readLock().unlock();
    }

}
