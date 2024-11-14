package org.rnakra.scheduler;

import org.rnakra.core.KeyValueStore;

import java.util.concurrent.CompletableFuture;

public class WriteTask implements Runnable {

    private final String key;
    private final String value;
    private final KeyValueStore keyValueStore;
    private final CompletableFuture<Void> completableFuture;

    public WriteTask(String key, String value, KeyValueStore keyValueStore, CompletableFuture<Void> completableFuture) {
        this.key = key;
        this.value = value;
        this.keyValueStore = keyValueStore;
        this.completableFuture = completableFuture;
    }

    @Override
    public void run() {
        try {
            keyValueStore.put(key, value);
        } catch (Exception e) {
            completableFuture.completeExceptionally(e);
        }
    }

    public CompletableFuture<Void> getCompletableFuture() {
        return completableFuture;
    }
}
