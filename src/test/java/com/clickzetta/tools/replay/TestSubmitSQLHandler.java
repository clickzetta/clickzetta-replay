package com.clickzetta.tools.replay;

import com.clickzetta.tools.replay.mock.MockDataSource;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;

public class TestSubmitSQLHandler {
    @Test
    public void testSubmitSQLHandler() throws Exception {
        Config config = new Config();
        config.setJdbcUrl("jdbc:clickzetta://example.api.clickzetta.com/example?vcluster=example");
        config.setUsername("mock");
        config.setPassword("mock");
        config.setDriverClass("com.clickzetta.client.jdbc.ClickZettaDriver");
        config.setThreadCount(1);
        SQLExecutor executor = new HttpHandlerExecutor(config);
        executor.execute();
        Field field = SQLExecutor.class.getDeclaredField("dataSource");
        field.setAccessible(true);
        field.set(executor, new MockDataSource());

//        SQLExecutor executor = new HttpHandlerExecutor(new Config() {{
//            setThreadCount(1);
//        }});
    }
}
