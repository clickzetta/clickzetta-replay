package com.clickzetta.tools.replay;

public class HttpSqlRecord {
    long sequenceId;
    long startTime;
    String username;
    String action;
    String dataBase;
    String sql;
    long elapsedTime;
    long resultCount;

    public long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getAction() {
        return action;
    }

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public long getResultCount() {
        return resultCount;
    }

    public void setResultCount(long resultCount) {
        this.resultCount = resultCount;
    }
}
