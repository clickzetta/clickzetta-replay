package com.clickzetta.tools.replay;

public class SQLExecutorFactory {
    public static SQLExecutor getExecutor(Config config) throws Exception {
        if (config.getInputFile() != null && !config.getInputFile().equals("")) {
            return new LocalFileExecutor(config);
        }
        return new HttpHandlerExecutor(config);
    }
}
