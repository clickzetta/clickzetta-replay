package com.clickzetta.tools.replay;

import com.clickzetta.client.jdbc.core.CZRequestIdGenerator;
import com.clickzetta.client.jdbc.core.CZStatement;
import com.sun.net.httpserver.HttpServer;
import lombok.SneakyThrows;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HttpHandlerExecutor extends SQLExecutor {
    private final HttpServer httpServer;
    protected Map<String, SQLProperty> sqlPropertyMap = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<SQLProperty> sqlQueue = new ConcurrentLinkedQueue<>();

    Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
            while (!thread.isInterrupted()) {
                SQLProperty sqlProperty = sqlQueue.poll();
                if (sqlProperty != null) {
                    if (sqlProperty.getOriginElapsedTime() != -1 || isTimeout(sqlProperty)) {
                        try {
                            sqlOutput.write(sqlProperty.toString() + "\n");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        sqlQueue.add(sqlProperty);
                    }
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    });


    public HttpHandlerExecutor(Config config) throws IOException {
        super(config);
        httpServer = HttpServer.create(new InetSocketAddress(config.getServerPort()), 0);
        httpServer.createContext("/submitSQL", new SubmitSQLHandler(this));
        httpServer.createContext("/updateSQL", new UpdateSQLHandler(this));
        httpServer.setExecutor(Executors.newFixedThreadPool(config.getThreadCount()));
    }

    @Override
    public void execute() throws IOException {
        httpServer.start();
        thread.start();
    }

    @Override
    void executeInternal(final SQLProperty sql, long delay, long currentIndex, long totalCount) {
        executor.schedule(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                if (sql.getSql().equals("")) {
                    return;
                }
                String jobId = CZRequestIdGenerator.getInstance().generate();
                Connection connection = dataSource.getConnection();
                long startTime = System.currentTimeMillis();
                sql.setStartTime(startTime);
                sql.setJobId(jobId);
                try {
                    System.out.println("job:" + jobId);
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
                    if (e.getClass().getName().equals("com.clickzetta.client.jdbc.core.CZNullResultException")) {
                        sql.setElapsedTime(System.currentTimeMillis() - startTime);
                        sql.setResultCount(0);
                    } else {
                        e.printStackTrace();
                        sql.setElapsedTime(System.currentTimeMillis() - startTime);
                    }
                } finally {
                    connection.close();
                }
                if (sql.getOriginElapsedTime() != -1) {
                    sqlOutput.write(sql.toString() + "\n");
                } else {
                    sqlQueue.add(sql);
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    public void close() throws Exception {
        httpServer.stop(0);
        thread.interrupt();
    }

    private boolean isTimeout(SQLProperty sqlProperty) {
        return System.currentTimeMillis() > sqlProperty.getStartTime() + sqlProperty.getElapsedTime() + config.getOutputTimeout();
    }
}
