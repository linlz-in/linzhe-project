package com.retailersv1.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.dwd.utils.BaseApp;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Package: com.retailersv1.dwd
 * @Author: lz
 * @Date: 2025/8/18 22:02
 * @version: 1.8
 */
public class DwdBaseLog extends BaseApp {
    private static final String kafka_topic_base_log_data = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");

    // 1. 将OutputTag定义为类的成员变量，显式指定泛型类型
    private static final OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};

    @SneakyThrows
    public static void main(String[] args) {
        new DwdBaseLog().start(8080, 4, "dwd_base_log", kafka_topic_base_log_data);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对流中数据类型进行转换 并做简单的ETL
        //ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out){
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            //如果转换的时候，没有发生异常，说明是标准的json，将数据传递到下游
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            //如果转换的时候，发生异常，说明不是标准的json，属于脏数据，将其放到侧输出流中
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        jsonObjDS.print("标准的json");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("脏数据：");

        // 将脏数据写入Kafka
        dirtyDS.sinkTo(KafkaUtils.buildKafkaSink(
                ConfigUtils.getString("kafka.bootstrap.servers"),
                ConfigUtils.getString("REALTIME.KAFKA.DIRTY.TOPIC") // 假设配置了脏数据主题
        ));

        //TODO 对新老访客标记进行修复
        //按照设备id进行分组
        //使用Flink的状态编程完成修复

        //TODO 分流 错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流
        //定义侧输出流标签
        //分流
        //TODO 将不同流的数据写到kafka的不同主题中
    }
}