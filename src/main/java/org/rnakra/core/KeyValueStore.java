package org.rnakra.core;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public interface KeyValueStore {
    public void put(String key, String value) throws IOException, NoSuchAlgorithmException;
    public String get(String key) throws IOException;
    public void compactAndMerge();
}
