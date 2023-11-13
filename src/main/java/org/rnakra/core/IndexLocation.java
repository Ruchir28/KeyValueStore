package org.rnakra.core;

public class IndexLocation  {
    private final String fileName;
    private final long offset;

    public IndexLocation(String fileName, long offset) {
        this.fileName = fileName;
        this.offset = offset;
    }

    public String getFileName() {
        return fileName;
    }

    public long getOffset() {
        return offset;
    }
}
