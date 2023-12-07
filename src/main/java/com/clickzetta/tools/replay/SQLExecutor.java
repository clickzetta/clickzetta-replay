package com.clickzetta.tools.replay;

import com.clickzetta.client.jdbc.core.CZRequestIdGenerator;
import com.clickzetta.client.jdbc.core.CZStatement;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SQLExecutor {
    protected final Config config;
    protected HikariDataSource dataSource;
    protected final ScheduledExecutorService executor;
    protected final SQLOutput sqlOutput;

    public SQLExecutor(Config config) throws FileNotFoundException {
        this.config = config;
        initDataSource(config);
        executor = new ScheduledThreadPoolExecutor(config.getThreadCount());
        sqlOutput = new SQLOutput(config);
    }

    private void initDataSource(Config config) {
        HikariConfig hc = new HikariConfig();
        hc.setDriverClassName(config.getDriverClass());
        hc.setJdbcUrl(config.getJdbcUrl());
        hc.setUsername(config.getUsername());
        hc.setPassword(config.getPassword());
        hc.setMaximumPoolSize(config.getThreadCount());
        hc.addDataSourceProperty("cachePrepStmts", "true");
        hc.addDataSourceProperty("prepStmtCacheSize", "100");
        dataSource = new HikariDataSource(hc);
    }

    void executeInternal(final SQLProperty sql, long delay) {
        executor.schedule(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                String jobId = CZRequestIdGenerator.getInstance().generate();
                Connection connection = dataSource.getConnection();
                long startTime = System.currentTimeMillis();
                sql.setStartTime(startTime);
                sql.setJobId(jobId);
                try {
                    System.out.println("sql:" + jobId);
                    String comment = "/* " + sql.getViewId() + "-" + sql.getSqlId() + " */";
                    Statement statement = connection.createStatement();
                    CZStatement czStatement = statement.unwrap(CZStatement.class);
                    ResultSet resultSet = czStatement.executeQuery(comment + sql.getSql(), jobId);
                    int count = 0;
                    while (resultSet.next()) {
                        count++;
                    }
                    sql.setElapsedTime(System.currentTimeMillis() - startTime);
                    sql.setResultCount(count);
                    czStatement.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    sql.setElapsedTime(System.currentTimeMillis() - startTime);
                } finally {
                    connection.close();
                }
                sqlOutput.write(sql.toString() + "\n");
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    void execute() throws Exception {}
}
