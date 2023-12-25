package org.rnakra.scheduler;

import org.rnakra.core.KeyValueStore;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public class ReadTask implements Callable<String> {

    private String key;
    private final KeyValueStore keyValueStore;

    private CompletableFuture<String> completableFuture;

    public ReadTask(String key, KeyValueStore keyValueStore, CompletableFuture<String> completableFuture) {
        this.key = key;
        this.keyValueStore = keyValueStore;
        this.completableFuture = completableFuture;
    }

    public CompletableFuture<String> getCompletableFuture() {
        return completableFuture;
    }

    @Override
    public String call() throws Exception {
        try {
            return keyValueStore.get(key);
        } catch (IOException e) {
            System.err.println("Exception in reading key:" + key+" "+e.getMessage());
            throw new RuntimeException(e);
        }
    }
}

