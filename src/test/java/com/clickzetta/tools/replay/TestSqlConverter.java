package com.clickzetta.tools.replay;

import org.junit.Test;

public class TestSqlConverter {
    String sql = "to_char(to_timestamp(1693365993712/1000) AT TIME ZONE 'Asia/Shanghai', '\"Q\"Q')\n" +
            "    yearofweek(convert_timezone('Asia/Shanghai',1693365993712 div 1000)),'-',weekofyear(convert_timezone('Asia/Shanghai',1693365993712 div 1000)));";

    @Test
    public void testConvert() {
        System.out.println(SQLConverter.convert(sql));
    }

}
