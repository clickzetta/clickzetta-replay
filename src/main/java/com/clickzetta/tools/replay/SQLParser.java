package com.clickzetta.tools.replay;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLParser {

    private static InputStream inputStream;
    private static BufferedReader reader;

    public SQLParser(String filePath) throws FileNotFoundException {
        File file = new File(filePath);
        if (file.exists()) {
            inputStream = new FileInputStream(file);
            reader = new BufferedReader(new InputStreamReader(inputStream));
        } else {
            throw new RuntimeException("sql file not found");
        }
    }

    public SQLProperty getSQL() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        }
        if (line.trim().equals("") || line.equals("\n")) {
            return getSQL();
        }

        Pattern pattern = Pattern.compile("view_id:(.*) id:(.*) start_time:(.*) elapsed_time:(.*) sql:(.*)");
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
            SQLProperty sqlProperty = new SQLProperty();
            sqlProperty.setViewId(matcher.group(1));
            sqlProperty.setSqlId(matcher.group(2));
            sqlProperty.setStartTime(Long.parseLong(matcher.group(3)));
            sqlProperty.setElapsedTime(Long.parseLong(matcher.group(4)));
            sqlProperty.setSql(matcher.group(5));
            return sqlProperty;
        } else {
            System.out.println("line:" + line);
            throw new RuntimeException("sql file format error");
        }
    }

    public void close() throws IOException {
        reader.close();
        inputStream.close();
    }
}
