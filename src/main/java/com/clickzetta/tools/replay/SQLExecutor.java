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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLExecutor {
    protected final Config config;
    protected HikariDataSource dataSource;
    protected final ScheduledExecutorService executor;
    protected final SQLOutput sqlOutput;

    protected final AtomicInteger activeTasks = new AtomicInteger(0);

    public SQLExecutor(Config config) throws IOException {
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

    void executeInternal(final SQLProperty sql, long delay, long currentIndex, long totalCount) {
        executor.schedule(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                activeTasks.incrementAndGet();
                String jobId = CZRequestIdGenerator.getInstance().generate();
                Connection connection = dataSource.getConnection();
                long startTime = System.currentTimeMillis();
                sql.setStartTime(startTime);
                sql.setJobId(jobId);
                try {
                    System.out.println("sql:" + jobId + "    " + currentIndex + "/" + totalCount);
                    String comment = "/* " + sql.getCategory() + "-" + sql.getSqlId() + " */";
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
                    if (e.getMessage().contains("resultSet is null")) {
                        sql.setResultCount(0);
                    } else {
                        e.printStackTrace();
                    }
                    sql.setElapsedTime(System.currentTimeMillis() - startTime);
                } finally {
                    connection.close();
                    activeTasks.decrementAndGet();
                }
                sqlOutput.write(sql.toString() + "\n");
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    void execute() throws Exception {}
}
