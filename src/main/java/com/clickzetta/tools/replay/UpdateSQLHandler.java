package com.clickzetta.tools.replay;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class UpdateSQLHandler implements HttpHandler {
    SQLExecutor sqlExecutor;
    public UpdateSQLHandler(SQLExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String paramStr = getParam(exchange);
        HttpHandlerParser httpHandlerParser = new HttpHandlerParser(paramStr);
        SQLProperty sqlProperty = httpHandlerParser.getSQL();
        ((HttpHandlerExecutor) sqlExecutor).sqlPropertyMap.compute(sqlProperty.getUniqueKey(), (k, v) -> {
            if (v != null) {
                v.setOriginElapsedTime(sqlProperty.getOriginElapsedTime());
                v.setOriginResultCount(sqlProperty.getOriginResultCount());
                return null;
            }
            return null;
        });
        String resMsg = "SUCCESS!";
        exchange.sendResponseHeaders(200, resMsg.length());
        OutputStream outputStream=exchange.getResponseBody();
        outputStream.write(resMsg.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
    }

    private String getParam(HttpExchange exchange) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), "utf-8"));
        StringBuilder requestBodyContent = new StringBuilder();
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            requestBodyContent.append(line);
        }
        return requestBodyContent.toString();
    }
}
