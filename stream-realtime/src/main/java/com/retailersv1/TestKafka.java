package com.retailersv1;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/16 8:38
 * @version: 1.8
 */
public class TestKafka {
    public static void main(String[] args) throws Exception {
        // 仅执行消费消息测试
        testConsumeWithFlink();
    }

    // 测试从 Kafka 消费消息（使用 Flink KafkaSource）
    private static void testConsumeWithFlink() throws Exception {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1，避免多线程打印混乱
        env.setParallelism(1);

        // 从配置文件获取Kafka连接信息
        String bootServerList = ConfigUtils.getString("kafka.bootstrap.servers");
        String kafkaTopic = ConfigUtils.getString("kafka.page.topic");
        String consumerGroup = "id"; // 消费者组ID

        // 构建Kafka数据源

        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(
                bootServerList,
                kafkaTopic,
                consumerGroup,
                OffsetsInitializer.latest());

        // 创建数据流
        DataStreamSource<String> source = env.fromSource(
                kafkaSource,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "Kafka Consumer Source"
        );



        // 打印消费到的消息
        source.print("消费到的Kafka消息：");

        // 执行Flink作业
        env.execute("Kafka Consumer Test Job");
    }
}
