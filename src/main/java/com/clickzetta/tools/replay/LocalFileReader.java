package com.clickzetta.tools.replay;

import java.io.*;

public class LocalFileReader {
    private static InputStream inputStream;
    private static BufferedReader reader;

    public LocalFileReader(String filePath) throws FileNotFoundException {
        File file = new File(filePath);
        if (file.exists()) {
            inputStream = new FileInputStream(file);
            reader = new BufferedReader(new InputStreamReader(inputStream));
        } else {
            throw new RuntimeException("sql file not found");
        }
    }

    public String getLine() throws IOException {
        return reader.readLine();
    }

    public void close() throws IOException {
        inputStream.close();
        reader.close();
    }
}
