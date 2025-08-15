package com.pg;
import com.sun.corba.se.impl.util.Version;
import lombok.SneakyThrows;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/11 20:30
 * @version: 1.8
 */
public class TestPgCDC {
    @SneakyThrows
    public static void main(String[] args) {
        DebeziumDeserializationSchema<String> deserializer = new JsonDebeziumDeserializationSchema();
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("192.168.200.101")
                        .port(5432)
                        .database("spider_db")
                        .schemaList("public")
                        .tableList("public.test_cdc")
                        .username("etl_flink_cdc_pub_user")
                        .password("etl_flink_cdc_pub_user123,./")
                        .slotName("flink_cdc_slot_test_cdc")
                        .debeziumProperties(new Properties() {{
                            setProperty("plugin.name", "pgoutput");  // 这里的setProperty会生效
                            // 强制使用手动创建的发布（关键）
                            setProperty("publication.name", "flink_cdc_publication");
                            // 禁用自动创建发布
                            setProperty("debezium.connector.postgresql.auto-create-publication", "false");
                        }})
                        .deserializer(deserializer)
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加数据源到执行环境
        env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
//                .setParallelism(2)
                .print();
        // 简单打印数据，实际应用中可替换为其他处理逻辑

        // 执行任务
        env.execute();
    }
}
