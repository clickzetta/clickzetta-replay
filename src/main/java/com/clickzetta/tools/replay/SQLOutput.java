package com.clickzetta.tools.replay;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class SQLOutput {
    private final FileOutputStream outputStream;

    public SQLOutput(Config config) throws IOException {
        outputStream = new FileOutputStream(config.getOutputFile());
        outputStream.write("slide_id,id,job_id,cz_start_time,cz,holo_start_time,holo,rs_cnt\n".getBytes());
    }

    public synchronized void write(String sql) throws Exception {
        outputStream.write(sql.getBytes());
        outputStream.flush();
    }

    public void close() throws Exception {
        outputStream.close();
    }
}
