package com.clickzetta.tools.replay;

import java.io.IOException;

public interface SQLParser {
    SQLProperty getSQL() throws IOException;

    void close() throws IOException;
}
