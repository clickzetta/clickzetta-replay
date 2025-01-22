package com.clickzetta.tools.replay;

import cz.shade.org.apache.commons.csv.CSVFormat;
import cz.shade.org.apache.commons.csv.CSVParser;
import cz.shade.org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalFileParser implements SQLParser {
    private final List<SQLProperty> sqlList = new ArrayList<>();
    private int index;

    public LocalFileParser(Config config) throws IOException {
        String inputFile = config.getInputFile();
        if (inputFile.endsWith(".csv")) {
            try (Reader reader = new FileReader(inputFile);
                 CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
                for (CSVRecord csvRecord : csvParser) {
                    SQLProperty sqlProperty = new SQLProperty();
                    sqlProperty.setCategory(csvRecord.get(0));
                    sqlProperty.setSqlId(csvRecord.get(1));
                    sqlProperty.setOriginStartTime((Long.parseLong(csvRecord.get(2))));
                    sqlProperty.setOriginElapsedTime(Long.parseLong(csvRecord.get(3)));
                    sqlProperty.setSql(csvRecord.get(4));
                    sqlList.add(sqlProperty);
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } else {
            LocalFileReader reader = new LocalFileReader(inputFile);
            String sql;
            while ((sql = reader.getLine()) != null) {
                if (sql.trim().isEmpty() || sql.equals("\n")) {
                    continue;
                }
                sqlList.add(getSqlProperty(sql));
            }
        }
        index = 0;
    }

    private static SQLProperty getSqlProperty(String sql) {
        Pattern pattern = Pattern.compile("category:(.*) id:(.*) start_time:(.*) elapsed_time:(.*) sql:(.*)");
        Matcher matcher = pattern.matcher(sql);
        SQLProperty sqlProperty = null;
        if (matcher.matches()) {
            sqlProperty = new SQLProperty();
            sqlProperty.setCategory(matcher.group(1));
            sqlProperty.setSqlId(matcher.group(2));
            sqlProperty.setOriginStartTime(Long.parseLong(matcher.group(3)));
            sqlProperty.setOriginElapsedTime(Long.parseLong(matcher.group(4)));
            sqlProperty.setSql(matcher.group(5));
        } else {
            throw new RuntimeException("sql file format error");
        }
        return sqlProperty;
    }

    @Override
    public SQLProperty getSQL() throws IOException {
        if (index >= sqlList.size()) {
            return null;
        }
        return sqlList.get(index++);
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
