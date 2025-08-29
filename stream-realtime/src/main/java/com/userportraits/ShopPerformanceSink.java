package com.userportraits;

import com.alibaba.fastjson.JSON;
import com.userportraits.bean.Common;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(Common.DATETIME_FORMAT);

    /**
     * 输出计算结果
     * @param resultStream 指标计算结果流
     */
    public static void writeResult(DataStream<Common.ShopPerformanceResult> resultStream)
    {
        // 1. 实时控制台输出（带时间戳）
        resultStream.map(result ->
                String.format("[%s] 实时结果: %s", LocalDateTime.now().format(dtf), JSON.toJSONString(result))
        ).print().name("realtime-console-sink");

        // 2. Kafka实时写入（供下游实时dashboard消费）
//        Properties kafkaProps = new Properties();
//        kafkaProps.setProperty("bootstrap.servers", "kafka01:9092,kafka02:9092");
//
//        resultStream.map(JSON::toJSONString)
//                .addSink(new FlinkKafkaProducer<>(
//                        "shop-performance-realtime-result",
//                        new SimpleStringSchema(),
//                        kafkaProps
//                )).name("realtime-kafka-sink")
//                .uid("realtime-kafka-sink-uid");

        // 3. 实时写入MySQL（带批量优化）
        String insertSql = "replace into shop_performance_realtime " +
                "(product_id, dimension, time_window, consult_user_count, " +
                "inquire_user_count, same_day_inquire_user_count, inquire_conversion_rate, " +
                "same_day_inquire_conversion_rate, same_day_pay_user_count, same_day_pay_amount, " +
                "final_pay_user_count, final_pay_amount, final_pay_quantity, window_end_ts) " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        resultStream.addSink(JdbcSink.sink(
                        insertSql,
                        (ps, result) -> {
                            ps.setString(1, result.getProductId());
                            ps.setString(2, result.getDimension());
                            ps.setString(3, result.getTimeWindow());
                            ps.setInt(4, result.getConsultUserCount());
                            ps.setInt(5, result.getInquireUserCount());
                            ps.setInt(6, result.getSameDayInquireUserCount());
                            ps.setDouble(7, result.getInquireConversionRate());
                            ps.setDouble(8, result.getSameDayInquireConversionRate());
                            ps.setInt(9, result.getSameDayPayUserCount());
                            ps.setDouble(10, result.getSameDayPayAmount());
                            ps.setInt(11, result.getFinalPayUserCount());
                            ps.setDouble(12, result.getFinalPayAmount());
                            ps.setInt(13, result.getFinalPayQuantity());
                            ps.setLong(14, result.getWindowEndTs());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(500) // 批量写入大小
                                .withBatchIntervalMs(1000) // 批量写入间隔
                                .withMaxRetries(3) // 失败重试次数
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://db01:3306/realtime_db?useSSL=false&serverTimezone=UTC")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                )).name("realtime-mysql-sink")
                .uid("realtime-mysql-sink-uid");
    }
}
