package com.stream.common;

import com.stream.common.utils.ConfigUtils;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/15 9:15
 * @version: 1.8
 */
public class CommonTest {
    public static void main(String[] args) {
        String kafka_err_log = ConfigUtils.getString("kafka.err.log");
        System.err.println(kafka_err_log);
    }
}
