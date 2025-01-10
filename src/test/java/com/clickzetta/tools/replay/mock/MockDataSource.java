package com.clickzetta.tools.replay.mock;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class MockDataSource extends HikariDataSource {
    @Override
    public Connection getConnection() throws SQLException {
        return new MockConnection();
    }
}
