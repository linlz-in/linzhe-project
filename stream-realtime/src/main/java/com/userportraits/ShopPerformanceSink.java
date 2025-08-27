package com.userportraits;

import com.userportraits.bean.Common;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;
import java.util.Properties;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/27 15:23
 * @version: 1.8
 *
 * 指标计算结果输出
 */
public class ShopPerformanceSink {
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * 输出计算结果
     * @param resultStream 指标计算结果流
     */
    public static void writeResult(DataStream<Common.ShopPerformanceResult> resultStream)
    {
        // 1. 测试环境
        resultStream.print().name("console-sink").uid("console-sink-uid");

        // 2. 生产环境  -- kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cdh01:9092,cdh02:9092,cdh03:9092");

        resultStream.map(result ->{
            try {
                return mapper.writeValueAsString(result);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        })
                .filter(Objects::nonNull)
                .addSink(new FlinkKafkaProducer<>(
                        "shop-performance-result-topic",
                        new SimpleStringSchema(),
                        properties
                )).name("kafka-sink")
                .uid("kafka-sink-uid");

        // 3. mysql
        resultStream.addSink(JdbcSink.sink(
                "insert into shop_performance(product_id, dimension, time_window, consult_user_count) values(?,?,?,?)",
                (ps, result) -> {
                    ps.setString(1, result.getProductId());
                    ps.setString(2, result.getDimension());
                    ps.setString(3, result.getTimeWindow());
                    ps.setInt(4, result.getConsultUserCount());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://cdh01:3306/userportraits")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        )).name("mysql-sink").uid("mysql-sink-uid");


    }
}
