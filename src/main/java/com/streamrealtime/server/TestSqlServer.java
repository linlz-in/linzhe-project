package com.streamrealtime.server;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class TestSqlServer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode","schema_only");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl","true");
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("192.168.200.101")
                .port(1433)
                .username("sa")
                .password("lz0918./")
                .database("test")
                .tableList("dbo.test_v1")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "——transaction_log_source1");
        dataStreamSource.print().setParallelism(1);
        env.execute("sqlserver-cdc-test");
    }
    public static Properties getDebeziumProperties(){
        Properties properties = new Properties();
        properties.put("converter","sqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.type","SqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.database.type","sqlserver");
        properties.put("sqlserverDebeziumConverter.former.datetime","yyyy-MM-dd HH:mm:ss");
        properties.put("sqlserverDebeziumConverter.former.date","yyyy-MM-dd");
        properties.put("sqlserverDebeziumConverter.former.time","HH:mm:ss");
        return properties;
    }
}
