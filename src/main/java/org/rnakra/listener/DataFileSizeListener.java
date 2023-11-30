package org.rnakra.listener;

import org.rnakra.io.DataFile;

public interface DataFileSizeListener {
    public void onFileSizeExceeded(DataFile dataFile);
}
