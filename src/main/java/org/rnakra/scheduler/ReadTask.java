package org.rnakra.scheduler;

import org.rnakra.core.KeyValueStore;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public class ReadTask implements Callable<String> {

    private String key;
    private final KeyValueStore keyValueStore;

    public ReadTask(String key, KeyValueStore keyValueStore) {
        this.key = key;
        this.keyValueStore = keyValueStore;
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

