package com.clickzetta.tools.replay;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class SQLOutput {
    private final FileOutputStream outputStream;

    public SQLOutput(Config config) throws FileNotFoundException {
        outputStream = new FileOutputStream(config.getOutputFile());
    }

    public synchronized void write(String sql) throws Exception {
        outputStream.write(sql.getBytes());
        outputStream.flush();
    }

    public void close() throws Exception {
        outputStream.close();
    }
}
