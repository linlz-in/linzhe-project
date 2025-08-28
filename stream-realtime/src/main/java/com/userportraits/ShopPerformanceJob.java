package com.userportraits;

import com.userportraits.bean.Common;
import com.userportraits.ods.ShopPermanceSource;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/27 15:57
 * @version: 1.8
 */
public class ShopPerformanceJob {
    @SneakyThrows
    public static void main(String[] args) {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        DataStream<String> rawStream = ShopPermanceSource.buildSource(env);
        DataStream<Common.CleanedShopData> cleanedStream = ShopPerformanceETL.processETL(rawStream);
        DataStream<Common.ShopPerformanceResult> resultStream = ShopPerformanceADS.calculateIndicators(cleanedStream);
//        ShopPerformanceSink.writeResult(resultStream);

        // 输出结果
        ShopPerformanceSink.writeResult(resultStream);

        // 执行
        env.execute("Shop Performance Real-time ");
    }
}
