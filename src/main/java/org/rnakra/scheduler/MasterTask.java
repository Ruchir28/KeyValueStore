package org.rnakra.scheduler;

import org.rnakra.core.DataFilesManager;
import org.rnakra.core.KeyValueStore;
import org.rnakra.core.KeyValueStoreImpl;
import org.rnakra.merger.CompactAndMerge;

import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterTask {
    private final int NUM_READ_THREADS = 5;
    private Queue<ReadTask> readQueue = new ConcurrentLinkedQueue<>();
    private final Queue<WriteTask> writeQueue = new LinkedList<>(); // Guarded by a lock
    private ExecutorService readExecutor = Executors.newFixedThreadPool(NUM_READ_THREADS);
    private ExecutorService writeExecutor = Executors.newSingleThreadExecutor();

    private final KeyValueStore keyValueStore;

    public MasterTask() throws FileNotFoundException {
        keyValueStore = new KeyValueStoreImpl();
    }

    public CompletableFuture<String> submitReadTask(String key) {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        readQueue.add(new ReadTask(key,keyValueStore,completableFuture));
        readExecutor.submit(this::processReadTask);
        return completableFuture;
    }

    public CompletableFuture<Void> submitWriteTask(String key, String value) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        writeQueue.add(new WriteTask(key, value, keyValueStore, completableFuture));
        writeExecutor.submit(this::processWriteTask);
        int random_choice = (int)(Math.random() * 1000.0);
        if(random_choice < 10) {
            submitMergeTask();
        }
        return completableFuture;
    }

    private void processWriteTask() {
        synchronized (writeQueue) {
            WriteTask writeTask = writeQueue.poll();
            if(writeTask != null) {
                writeTask.run();
                writeTask.getCompletableFuture().complete(null);
            }
        }
    }

    private void processReadTask()  {
        ReadTask readTask = readQueue.poll();
        try {
            String value = null;
            if(readTask != null) {
                value = readTask.call();
                readTask.getCompletableFuture().complete(value);
            }
        } catch (Exception e) {
            readTask.getCompletableFuture().completeExceptionally(e);
        }
    }

    public void submitMergeTask() {
        keyValueStore.compactAndMerge();
    }



}
