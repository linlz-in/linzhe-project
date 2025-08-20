package com.retailersv1.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.dwd.utils.BaseApp;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
//        dirtyDS.sinkTo(KafkaUtils.buildKafkaSink(
//                ConfigUtils.getString("kafka.bootstrap.servers"),
//                ConfigUtils.getString("REALTIME.KAFKA.DIRTY.TOPIC") // 假设配置了脏数据主题
//        ));

        //TODO 对新老访客标记进行修复
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //使用Flink的状态编程完成修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //获取is_new的值
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        //从状态中获取首次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateTimeUtils.tsToDate(ts);
                        if ("1".equals(isNew)) {
                            //如果is_new的值为1
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //如果键控状态为null,认为本次是该访客首次访问 APP,将日志中 ts 对应的日期更新到状态中,不对is_new 字段做修改;
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                //如果键控状态不为null,且首次访问日期不是当日,说明访问的是老访客,将is_new 字段置为0;
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                                //如果键控状态不为 null,且首次访问日期是当日,说明访问的是新访客,不做操作;
                            }

                        } else {
                            //如果 is_new 的值为 0
                            //如果键控状态为 null,说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时,日志进入程序被判定为新访客。
                            //如果键控状态不为null,说明程序已经维护了首次访问日期,不做操作。
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yerterDay = DateTimeUtils.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yerterDay);
                            }
                        }

                        return jsonObj;
                    }
                }
        );
        fixedDS.print();
        //TODO 分流 错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流
        //定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        //分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) {
                        // 错误日志
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            // 启动日志
                            // 将启动日志写日到启动侧输出流
                            ctx.output(startTag, jsonObj.toJSONString());

                        } else {
                            // 页面日志
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
                            // 曝光日志
                            JSONArray displaysArr = jsonObj.getJSONArray("displays");
                            if (displaysArr != null && displaysArr.size() > 0) {
                                //遍历当前页面的所有曝光信息
                                for (int i = 0; i < displaysArr.size(); i++) {
                                    JSONObject displayJsonObj = displaysArr.getJSONObject(i);
                                    //定义一个新的JSON对象，用于封装遍历出来的曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }
                            //动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                //遍历出每一个动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    //定义一个新的JSON对象，用于封装动作信息
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    //将动作日志写到动作侧输出流
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
                            //页面日志 写道主流中
                            out.collect(jsonObj.toJSONString());
                        }

                    }
                }
        );

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.print("页面==>");
        errDS.print("错误==>");
        startDS.print("启动==>");
        displayDS.print("曝光==>");
        actionDS.print("动作==>");

        //TODO 将不同流的数据写到kafka的不同主题中

//        pageDS.sinkTo(KafkaUtils.sinkJson2KafkaMessage("dwd_page_log",));


    }
}