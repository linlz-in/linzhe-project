package com.retailersv1;

import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.utils.PaimonMinioUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/15 14:59
 * @version: 1.8
 */
public class FlkPaimonTest {
    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "NONE");

        PaimonMinioUtils.ExecCreateMinioCatalogAndDatabases(tenv,"minio_paimon_catalog","realtime_v2");


        tenv.executeSql("select * from realtime_v2.res_cart_info_tle where ds = '20250506' ").print();

    }
}
