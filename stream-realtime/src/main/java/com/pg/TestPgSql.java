package com.pg;
import com.utils.EnvironmentSettingsUtils;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/11 20:30
 * @version: 1.8
 */
public class TestPgSql {
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");
        System.setProperty("org.apache.flink.runtime.webmonitor.disable", "true");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingsUtils.defaultParameter(env);
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("10.160.60.14")
                        .port(5432)
                        .database("spider_db")
                        .schemaList("public")
                        .tableList("public.source_data_car_info_message_dtl")
                        .username("postgres")
                        .password("Zh1028,./")
                        .slotName("postgresIncrementalSource")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .decodingPluginName("pgoutput")
                        .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initial())
                        .build();
        // 添加数据源到执行环境
        env.fromSource(postgresIncrementalSource, WatermarkStrategy.noWatermarks(), "PostgreSQL CDC Source")
                .print(); // 简单打印数据，实际应用中可替换为其他处理逻辑

        // 执行任务
        env.execute("PostgreSQL CDC Job");
    }
}
