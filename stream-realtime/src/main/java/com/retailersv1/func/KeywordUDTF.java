package com.retailersv1.func;

import com.stream.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/22 19:15
 * @version: 1.8
 */
public class KeywordUDTF  extends TableFunction<Row> {
    public void eval(String test) {
        for (String keyword : KeywordUtil.analyze(test)) {
            collect(Row.of(keyword));
        }
    }
}