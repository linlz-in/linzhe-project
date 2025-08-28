package com.userportraits.ods;

import com.userportraits.data.DataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/27 9:38
 * @version: 1.8
 *
 * Source: 数据源接入 -- 模拟数据/真实kafka数据
 */
public class ShopPermanceSource {
    /**
     * 构建数据流
     * @param env Flink执行环境
     * @return 原始JSON数据流
     */
    public static DataStream<String> buildSource(StreamExecutionEnvironment env) {
        // 示例：通过系统属性/配置判断环境，实际可从配置中心、枚举等灵活获取
        String envFlag = System.getProperty("env", "local");
        if ("local".equals(envFlag)) {
            // 本地测试：模拟数据
            return env.addSource(new DataGenerator())
                    .name("shop-performance-source")
                    .uid("shop-performance-sim-source-uid");
        } else {
            // 生产环境：Kafka 数据源
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh01:9092,cdh02:9092,cdh03:9092");
            kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "shop-performance-group");

            return env.addSource(new FlinkKafkaConsumer<>(
                            "shop-performance-topic",
                            new SimpleStringSchema(),
                            kafkaProps))
                    .name("shop-performance-kafka-source")
                    .uid("shop-performance-kafka-source-uid");
        }
    }
}
