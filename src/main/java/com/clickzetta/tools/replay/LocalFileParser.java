package com.clickzetta.tools.replay;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalFileParser implements SQLParser {
    private final List<String> sqlList = new ArrayList<>();
    private int index;

    public LocalFileParser(Config config) throws IOException {
        LocalFileReader reader = new LocalFileReader(config.getInputFile());
        String sql;
        while ((sql = reader.getLine()) != null) {
            sqlList.add(sql);
        }
        index = 0;
    }

    @Override
    public SQLProperty getSQL() throws IOException {
        if (index >= sqlList.size()) {
            return null;
        }
        String line = sqlList.get(index++);
        if (line == null) {
            return null;
        }
        if (line.trim().equals("") || line.equals("\n")) {
            return getSQL();
        }

        Pattern pattern = Pattern.compile("category:(.*) id:(.*) start_time:(.*) elapsed_time:(.*) sql:(.*)");
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
            SQLProperty sqlProperty = new SQLProperty();
            sqlProperty.setViewId(matcher.group(1));
            sqlProperty.setSqlId(matcher.group(2));
            sqlProperty.setOriginStartTime(Long.parseLong(matcher.group(3)));
            sqlProperty.setOriginElapsedTime(Long.parseLong(matcher.group(4)));
            sqlProperty.setSql(matcher.group(5));
            return sqlProperty;
        } else {
            throw new RuntimeException("sql file format error");
        }
    }

    @Override
    public void close() throws IOException {
    }

    public long getCurrentSqlIndex() {
        return index;
    }

    public long getTotalCount() {
        return sqlList.size();
    }
}
