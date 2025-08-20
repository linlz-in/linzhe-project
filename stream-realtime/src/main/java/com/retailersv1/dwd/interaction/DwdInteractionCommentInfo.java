package com.retailersv1.dwd.interaction;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/20 18:47
 * @version: 1.8
 * 评论事实表
 */
public class DwdInteractionCommentInfo {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关的设置
        //2.1 开启检查点

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置状态取消后,检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart( 3, Time.days(30), Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck/");


        //2.7 设置操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME","root");

        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表       -- kafka连接器
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                " `database` string, \n" +
                " `table` string, \n" +
                " `type` string, \n" +
                " `ts` bigint, \n" +
                " `data` MAP<string, string>, \n" +
                " `old` MAP<string, string>, \n" +
                "  proc_time as proctime()\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka', \n" +
                " 'topic' = 'cdc_db_topic', \n" +
                " 'properties.bootstrap.servers' = 'cdh01:9092' , \n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'scan.startup.mode' = 'latest-offset', \n" +
                " 'format' = 'json' \n" +
                ")");
        tableEnv.executeSql( "select * from topic_db") .print();
        //TODO 4.过滤出评论数据                                --where table = 'comment_info' type = 'insert'
        //TODO 5.从HBase中读取字典数据 创建动态表                 --hbase连接器
        //TODO 6.将评论表和字典表进行关联                        -- lookup join
        //TODO 7.将关联的结果写到kafka主题中                     -- upsert kafka连接器
        //7.1 创建动态表和要写入的主题进行映射
        //7.2 写入
    }
}
