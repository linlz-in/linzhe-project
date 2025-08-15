package com.stream;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/15 19:30
 * @version: 1.8
 */
public class FlinkNcMessage {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> dataStreamSource = env.socketTextStream("cdh03", 14777);

        dataStreamSource.print();

        env.execute();
    }
}
