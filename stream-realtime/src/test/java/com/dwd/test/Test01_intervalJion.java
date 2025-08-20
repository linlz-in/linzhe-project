package com.dwd.test;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/20 15:14
 * @version: 1.8
 */
public class Test01_intervalJion {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("cdh01", 7777)
                .map((MapFunction<String, Emp>) lineStr -> {
                    String[] fieldArr = lineStr.split(",");
                    return new Emp(Integer.valueOf(fieldArr[0]),fieldArr[1], Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));

                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Emp>) (emp, recordTimestamp) -> emp.getTs())
                );

        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("cdh01", 8889)
                .map(new MapFunction<String, Dept>() {
                    @Override
                    public Dept map(String lineStr){
                        String[] fieldArr = lineStr.split(",");
                        return new Dept(Integer.valueOf(fieldArr[0]),fieldArr[1],Long.valueOf(fieldArr[2]));

                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Dept>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Dept>() {
                                    @Override
                                    public long extractTimestamp(Dept dept, long recordTimestamp) {
                                        return dept.getTs();
                                    }
                                })
                );
        empDS
                .keyBy(Emp::getDeptno)
                .intervalJoin(deptDS.keyBy(Dept::getDeptno))
                .between(Time.seconds(-5),Time.seconds(5))
                .process(new ProcessJoinFunction<Emp, Dept, Tuple2<Emp,Dept>>() {
                    @Override
                    public void processElement(Emp emp, Dept dept, ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>.Context ctx, Collector<Tuple2<Emp, Dept>> out){
                        out.collect(Tuple2.of(emp,dept));
                    }
                }).print();
        env.execute();

    }
}
