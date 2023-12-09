package org.rnakra.scheduler;

import org.rnakra.core.KeyValueStore;

import java.io.IOException;

public class WriteTask implements Runnable {

    private final String key;
    private final String value;
    private final KeyValueStore keyValueStore;

    public WriteTask(String key, String value, KeyValueStore keyValueStore) {
        this.key = key;
        this.value = value;
        this.keyValueStore = keyValueStore;
    }


    @Override
    public void run() {
        try {
            keyValueStore.put(key,value);
        } catch (IOException e) {
            System.err.println("Error in writing Key: " + key+" "+ e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
