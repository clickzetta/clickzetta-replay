package com.clickzetta.tools.replay;

import com.clickzetta.client.jdbc.core.CZRequestIdGenerator;
import com.clickzetta.client.jdbc.core.CZStatement;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SQLExecutor {
    private final Config config;
    private HikariDataSource dataSource;
    private final ScheduledExecutorService executor;

    private long startTime = 0;

    private final SQLOutput sqlOutput;


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

    public void execute(final SQLProperty sql) {
        synchronized (this) {
            if (startTime == 0) {
                startTime = sql.getStartTime();
            } else {
                if (sql.getStartTime() < startTime) {
                    startTime = sql.getStartTime();
                }
            }
        }
        long delay = 0;
        if (!config.isWithoutDelay()) {
            delay = (sql.getStartTime() - startTime) / config.getReplayRate();
        }
        executor.schedule(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                String record;
                String jobId = CZRequestIdGenerator.getInstance().generate();
                Connection connection = dataSource.getConnection();
                try {
                    long startTime = System.currentTimeMillis();
                    System.out.println("sql:" + jobId);
                    String comment = "/* " + sql.getViewId() + "-" + sql.getSqlId() + " */";
                    Statement statement = connection.createStatement();
                    CZStatement czStatement = statement.unwrap(CZStatement.class);
                    ResultSet resultSet = czStatement.executeQuery(comment + sql.getSql(), jobId);
                    int count = 0;
                    while (resultSet.next()) {
                        count++;
                    }
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    record = sql.getViewId() + "," + sql.getSqlId() + "," + jobId + ","
                                + startTime + "," + elapsedTime + ","
                                + sql.getStartTime() + "," + sql.getElapsedTime() + "," + count + "\n";
                    czStatement.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    record = sql.getViewId() + "," + sql.getSqlId() + "," + jobId + ","
                            + startTime + "," + elapsedTime + ","
                            + sql.getStartTime() + "," + sql.getElapsedTime() + ",FAILED" + "\n";
                } finally {
                    connection.close();
                }
                sqlOutput.write(record);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }


    public void checkWait() throws InterruptedException {
        while (((ScheduledThreadPoolExecutor)executor).getQueue().size() >= ((ScheduledThreadPoolExecutor) executor).getPoolSize()) {
            try {
                System.out.println("wait for queue size: " + ((ScheduledThreadPoolExecutor)executor).getQueue().size());
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw e;
            }
        }
    }

    public void close() throws Exception {
        executor.shutdown();
        boolean isDown = false;
        while (!isDown) {
            try {
                isDown = executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        dataSource.close();
        sqlOutput.close();
    }
}
