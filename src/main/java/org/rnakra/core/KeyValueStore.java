package org.rnakra.core;

import java.io.IOException;

public interface KeyValueStore {
    public void put(String key, String value) throws IOException;
    public String get(String key) throws IOException;
}
