package com.retailersv1.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/15 14:38
 * @version: 1.8
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim implements Serializable {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族
    String sinkFamily;

    String sinkRowKey;

    String op;
}
