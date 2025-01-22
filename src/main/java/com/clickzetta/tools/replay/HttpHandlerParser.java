package com.clickzetta.tools.replay;

import cz.shade.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class HttpHandlerParser implements SQLParser {

    private String paramStr;

    public HttpHandlerParser(String paramStr) {
        this.paramStr = paramStr;
    }
    @Override
    public SQLProperty getSQL() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        HttpSqlRecord sqlRecord = objectMapper.readValue(paramStr, HttpSqlRecord.class);
        SQLProperty sqlProperty = new SQLProperty();
        sqlProperty.setCategory(sqlRecord.getAction());
        sqlProperty.setSqlId(String.valueOf(sqlRecord.getSequenceId()));
        sqlProperty.setSql(sqlRecord.getSql());
        sqlProperty.setOriginStartTime(sqlRecord.getStartTime());
        sqlProperty.setOriginElapsedTime(sqlRecord.getElapsedTime());
        sqlProperty.setOriginResultCount(sqlRecord.getResultCount());
        return sqlProperty;
    }

    @Override
    public void close() throws IOException {

    }
}
