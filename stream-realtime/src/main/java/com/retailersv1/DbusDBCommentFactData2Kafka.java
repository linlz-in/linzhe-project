package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.AsyncHbaseDimBaseDicFunc;
import com.retailersv1.func.IntervalJoinOrderCommentAndOrderInfoFunc;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.DateTimeUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.SensitiveWordsUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/15 14:55
 * @version: 1.8
 *
 *
 * 电商平台的实时评论处理与风控。
 * 实时数据仓库的维度扩展与事实表构建。
 * 实时数据 pipeline 中的多源数据关联与 enrichment。
 */
public class DbusDBCommentFactData2Kafka {
    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }

    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String kafka_db_fact_comment_topic = ConfigUtils.getString("kafka.db.fact.comment.topic");

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关的设置
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));


//        EnvironmentSettingUtils.defaultParameter(env);



        // 评论表 取数
        // 通过cdc 获取数据 设置Watermark策略，使用ts_ms作为事件时间
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        new Date().toString(),
                        OffsetsInitializer.latest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");

        kafkaCdcDbSource.print("kafkaCdcDbSource -> :");

        // 订单主表
        // {"op":"c","after":{"payment_way":"3501","consignee":"窦先敬","create_time":1746660084000,"refundable_time":1747264884000,"original_total_amount":"6499.00","coupon_reduce_amount":"0.00","order_status":"1001","out_trade_no":"929361421194788","total_amount":"5999.00","user_id":184,"province_id":23,"consignee_tel":"13879332785","trade_body":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机等1件商品","id":1123,"activity_reduce_amount":"500.00"},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":31381479,"name":"mysql_binlog_source","thread":20265,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746596800000,"snapshot":"false","db":"realtime_v1","table":"order_info"},"ts_ms":1746596800483}
        DataStream<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .uid("kafka_cdc_db_order_source").name("kafka_cdc_db_order_source");

        // 评论表进行进行升维处理 和hbase的维度进行关联补充维度数据   进行keyby 便于后续异步查询
        // {"op":"c","after":{"create_time":1746624077000,"user_id":178,"appraise":"1201","comment_txt":"评论内容：44237268662145286925725839461514467765118653811952","nick_name":"珠珠","sku_id":14,"id":85,"spu_id":4,"order_id":1010},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":30637591,"name":"mysql_binlog_source","thread":20256,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746596796000,"snapshot":"false","db":"realtime_v1","table":"comment_info"},"ts_ms":1746596796319}
        DataStream<JSONObject> filteredStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("comment_info"))
                .keyBy(json -> json.getJSONObject("after").getString("appraise"));

        // 关联维度表 获取字典名称（如“好评”、“中评”等）
        // {"op":"c","after":{"create_time":1746568494000,"user_id":126,"appraise":"1202","comment_txt":"评论内容：43341158654483726916799957869464279782846343359228","nick_name":"琬琬","sku_id":5,"id":77,"spu_id":2,"order_id":334,"dic_name":"中评"},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":29984187,"name":"mysql_binlog_source","thread":19913,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746518022000,"snapshot":"false","db":"realtime_v1","table":"comment_info"},"ts_ms":1746518022747}
        DataStream<JSONObject> enrichedStream = AsyncDataStream
                .unorderedWait(
                        filteredStream,
                        new AsyncHbaseDimBaseDicFunc(),
                        60,
                        TimeUnit.SECONDS,
                        100
                ).uid("async_hbase_dim_base_dic_func")
                .name("async_hbase_dim_base_dic_func");


        // 数据转换与格式化。 提取并重命名字段，统一数据结构。保留关键字段
        // {"op":"c","create_time":1746653124000,"commentTxt":"评论内容：36913887965764674188858298813931966419435136341364","sku_id":19,"server_id":"1","dic_name":"好评","appraise":"1201","user_id":221,"id":89,"spu_id":5,"order_id":979,"ts_ms":1746596800251,"db":"realtime_v1","table":"comment_info"}
        SingleOutputStreamOperator<JSONObject> orderCommentMap = enrichedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject resJsonObj = new JSONObject();
                        Long tsMs = jsonObject.getLong("ts_ms");
                        JSONObject source = jsonObject.getJSONObject("source");
                        String dbName = source.getString("db");
                        String tableName = source.getString("table");
                        String serverId = source.getString("server_id");
                        if (jsonObject.containsKey("after")) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            resJsonObj.put("ts_ms", tsMs);
                            resJsonObj.put("db", dbName);
                            resJsonObj.put("table", tableName);
                            resJsonObj.put("server_id", serverId);
                            resJsonObj.put("appraise", after.getString("appraise"));
                            resJsonObj.put("commentTxt", after.getString("comment_txt"));
                            resJsonObj.put("op", jsonObject.getString("op"));
                            resJsonObj.put("nick_name", jsonObject.getString("nick_name"));
                            resJsonObj.put("create_time", after.getLong("create_time"));
                            resJsonObj.put("user_id", after.getLong("user_id"));
                            resJsonObj.put("sku_id", after.getLong("sku_id"));
                            resJsonObj.put("id", after.getLong("id"));
                            resJsonObj.put("spu_id", after.getLong("spu_id"));
                            resJsonObj.put("order_id", after.getLong("order_id"));
                            resJsonObj.put("dic_name", after.getString("dic_name"));
                            return resJsonObj;
                        }
                        return null;
                    }
                })
                .uid("map-order_comment_data")
                .name("map-order_comment_data");


        // {"op":"c","payment_way":"3501","consignee":"张贞","create_time":1746653800000,"refundable_time":1747258600000,"original_total_amount":"69.00","coupon_reduce_amount":"0.00","order_status":"1001","out_trade_no":"914927687659481","total_amount":"69.00","user_id":156,"province_id":10,"tm_ms":1746596799810,"consignee_tel":"13114791128","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M01醉蔷薇等1件商品","id":1108,"activity_reduce_amount":"0.00"}
        SingleOutputStreamOperator<JSONObject> orderInfoMapDs = filteredOrderInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject inputJsonObj){
                String op = inputJsonObj.getString("op");
                long tm_ms = inputJsonObj.getLongValue("ts_ms");
                JSONObject dataObj;
                if (inputJsonObj.containsKey("after") && !inputJsonObj.getJSONObject("after").isEmpty()) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else {
                    dataObj = inputJsonObj.getJSONObject("before");
                }
                JSONObject resultObj = new JSONObject();
                resultObj.put("op", op);
                resultObj.put("tm_ms", tm_ms);
                resultObj.putAll(dataObj);
                return resultObj;
            }
        }).uid("map-order_info_data").name("map-order_info_data");


        // orderCommentMap.order_id join orderInfoMapDs.id
        KeyedStream<JSONObject, String> keyedOrderCommentStream = orderCommentMap.keyBy(data -> data.getString("order_id"));
        KeyedStream<JSONObject, String> keyedOrderInfoStream = orderInfoMapDs.keyBy(data -> data.getString("id"));

        // Interval Join 评论与订单数据   按 order_id 和 id 进行 KeyBy。
        // {"info_original_total_amount":"56092.00","info_activity_reduce_amount":"1199.90","commentTxt":"评论内容：52198813817222113474133821791377912858419193882331","info_province_id":8,"info_payment_way":"3501","info_create_time":1746624020000,"info_refundable_time":1747228820000,"info_order_status":"1002","id":84,"spu_id":3,"table":"comment_info","info_tm_ms":1746596796189,"info_operate_time":1746624052000,"op":"c","create_time":1746624077000,"info_user_id":178,"info_op":"u","info_trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等6件商品","sku_id":11,"server_id":"1","dic_name":"好评","info_consignee_tel":"13316189177","info_total_amount":"54892.10","info_out_trade_no":"692358523797933","appraise":"1201","user_id":178,"info_id":1010,"info_coupon_reduce_amount":"0.00","order_id":1010,"info_consignee":"彭永","ts_ms":1746596796318,"db":"realtime_v1"}
        SingleOutputStreamOperator<JSONObject> orderMsgAllDs = keyedOrderCommentStream.intervalJoin(keyedOrderInfoStream)
                .between(Time.minutes(-1), Time.minutes(1))
                .process(new IntervalJoinOrderCommentAndOrderInfoFunc())
                .uid("interval_join_order_comment_and_order_info_func").name("interval_join_order_comment_and_order_info_func");


        // 通过AI 生成评论数据，`Deepseek 7B` 模型即可
        // {"info_original_total_amount":"1299.00","info_activity_reduce_amount":"0.00","commentTxt":"\n\n这款Redmi 10X虽然价格亲民，但续航能力一般且相机效果平平，在同类产品中竞争力不足。","info_province_id":32,"info_payment_way":"3501","info_create_time":1746566254000,"info_refundable_time":1747171054000,"info_order_status":"1004","id":75,"spu_id":2,"table":"comment_info","info_tm_ms":1746518021300,"info_operate_time":1746563573000,"op":"c","create_time":1746563573000,"info_user_id":149,"info_op":"u","info_trade_body":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 明月灰 游戏智能手机 小米 红米等1件商品","sku_id":7,"server_id":"1","dic_name":"好评","info_consignee_tel":"13144335624","info_total_amount":"1299.00","info_out_trade_no":"199223184973112","appraise":"1201","user_id":149,"info_id":327,"info_coupon_reduce_amount":"0.00","order_id":327,"info_consignee":"范琳","ts_ms":1746518021294,"db":"realtime_v1"}
        SingleOutputStreamOperator<JSONObject> supplementDataMap = orderMsgAllDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                // 直接使用降级评论，完全不调用可能出错的API
                String comment = generateFallbackComment(
                        jsonObject.getString("dic_name"),
                        jsonObject.getString("info_trade_body")
                );
                jsonObject.put("commentTxt", comment);
                return jsonObject;
            }

            private String generateFallbackComment(String dicName, String tradeBody) {
                String product = tradeBody.length() > 20 ? tradeBody.substring(0, 20) + "..." : tradeBody;
                switch (dicName) {
                    case "好评":
                        return "👍 " + product + " 物超所值，使用体验很棒！";
                    case "中评":
                        return "➖ " + product + " 中规中矩，有待提升。";
                    case "差评":
                        return "👎 " + product + " 不太满意，需要改进。";
                    default:
                        return "📝 对" + product + "的" + dicName + "评价";
                }
            }
        }).uid("map-generate_comment").name("map-generate_comment");


        // {"info_original_total_amount":"56092.00","info_activity_reduce_amount":"1199.90","commentTxt":"\n\n差评：续航差、相机效果一般,高自联","info_province_id":8,"info_payment_way":"3501","info_create_time":1746624020000,"info_refundable_time":1747228820000,"info_order_status":"1001","id":88,"spu_id":5,"table":"comment_info","info_tm_ms":1746596795948,"op":"c","create_time":1746624077000,"info_user_id":178,"info_op":"c","info_trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等6件商品","sku_id":19,"server_id":"1","dic_name":"好评","info_consignee_tel":"13316189177","info_total_amount":"54892.10","info_out_trade_no":"692358523797933","appraise":"1201","user_id":178,"info_id":1010,"info_coupon_reduce_amount":"0.00","order_id":1010,"info_consignee":"彭永","ts_ms":1746596796327,"db":"realtime_v1"}
        SingleOutputStreamOperator<JSONObject> suppleMapDs = supplementDataMap.map(new RichMapFunction<JSONObject, JSONObject>() {
            private transient Random random;

            @Override
            public void open(Configuration parameters){
                random = new Random();
            }

            @Override
            public JSONObject map(JSONObject jsonObject){
                // 注入敏感词
                if (random.nextDouble() < 0.2) {
                    jsonObject.put("commentTxt", jsonObject.getString("commentTxt") + "," + SensitiveWordsUtils.getRandomElement(sensitiveWordsLists));
                    System.err.println("change commentTxt: " + jsonObject);
                }
                return jsonObject;
            }
        }).uid("map-sensitive-words").name("map-sensitive-words");

        SingleOutputStreamOperator<JSONObject> suppleTimeFieldDs = suppleMapDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                jsonObject.put("ds", DateTimeUtils.format(new Date(jsonObject.getLong("ts_ms")), "yyyyMMdd"));
                return jsonObject;
            }
        }).uid("add json ds").name("add json ds");

        suppleTimeFieldDs.map(js -> js.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_db_fact_comment_topic)
                ).uid("kafka_db_fact_comment_sink").name("kafka_db_fact_comment_sink");

        suppleTimeFieldDs.print("suppleTimeFieldDs -> ");


        env.execute();
    }
}
