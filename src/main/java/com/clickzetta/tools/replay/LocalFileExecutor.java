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

public class LocalFileExecutor extends SQLExecutor {

    private long startTime = 0;

    private final LocalFileParser parser;

    public LocalFileExecutor(Config config) throws IOException {
        super(config);
        parser = new LocalFileParser(config);
    }

    @Override
    public void execute() throws Exception {
        SQLProperty sqlProperty;
        long lastStartTime = 0;
        long totalCount = parser.getTotalCount();
        while ((sqlProperty = parser.getSQL()) !=null) {
            long delay = 0;
            synchronized (this) {
                if (startTime == 0) {
                    startTime = sqlProperty.getOriginStartTime();
                } else {
                    if (sqlProperty.getOriginStartTime() < startTime) {
                        startTime = sqlProperty.getOriginStartTime();
                    }
                }
            }
            if (lastStartTime == 0) {
                lastStartTime = startTime;
            }
            if (config.isDynamicMode()) {
                long alreadySleep = 0;
                while (activeTasks.get() > 0) {
                    if (sqlProperty.getOriginStartTime() - lastStartTime <= alreadySleep) {
                        break;
                    }
                    Thread.sleep(config.getSleepInterval());
                    alreadySleep += config.getSleepInterval();
                }
            } else {
                if (!config.isWithoutDelay()) {
                    if (activeTasks.get() > 0) {
                        delay = (sqlProperty.getOriginStartTime() - startTime) / config.getReplayRate();
                    }
                }
            }
            executeInternal(sqlProperty, delay, parser.getCurrentSqlIndex(), totalCount);
            lastStartTime = sqlProperty.getOriginStartTime();
        }
        close();
    }

    public void checkWait() throws InterruptedException {
        while (((ScheduledThreadPoolExecutor)executor).getQueue().size() >= ((ScheduledThreadPoolExecutor) executor).getPoolSize()) {
            try {
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
        parser.close();
    }
}
