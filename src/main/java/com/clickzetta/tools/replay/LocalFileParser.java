package com.clickzetta.tools.replay;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalFileParser implements SQLParser {
    private final LocalFileReader reader;

    public LocalFileParser(Config config) throws FileNotFoundException {
        reader = new LocalFileReader(config.getInputFile());
    }

    @Override
    public SQLProperty getSQL() throws IOException {
        String line = reader.getLine();
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
            sqlProperty.setOriginStartTime(Long.parseLong(matcher.group(3)));
            sqlProperty.setOriginElapsedTime(Long.parseLong(matcher.group(4)));
            sqlProperty.setSql(SQLConverter.convert(matcher.group(5)));
            return sqlProperty;
        } else {
            throw new RuntimeException("sql file format error");
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
