package com.userportraits.ods;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.HbaseUtils;
import com.stream.common.utils.KafkaUtils;

import com.userportraits.bean.ProductDim;
import com.userportraits.bean.WorkOrderConstant;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;
import java.util.Objects;

/**
 * ODS层：数据接入与标准化（工单编号：大数据-用户画像-02-服务主题店铺绩效）
 */
public class OdsLayer {
    private final StreamExecutionEnvironment env;

    public OdsLayer(StreamExecutionEnvironment env) {
        this.env = env;
    }

    /**
     * 1. 从Kafka读取MySQL CDC业务数据，输出标准化业务流
     */
    public SingleOutputStreamOperator<JSONObject> getStandardizedBusinessStream() {
        // 1.1 用KafkaUtils构建Kafka Source（读取CDC数据）
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(
                WorkOrderConstant.KAFKA_SERVER,
                WorkOrderConstant.KAFKA_CDC_TOPIC,
                WorkOrderConstant.KAFKA_CONSUMER_GROUP,
                OffsetsInitializer.latest() // 从最新偏移量消费（实时场景）
        );

        // 1.2 读取Kafka数据并转换为JSON格式
        DataStreamSource<String> kafkaRawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), // 后续DWD层补全Watermark
                "ods-kafka-cdc-source"
        );

        // 1.3 标准化与过滤：过滤无效商品ID场景（短链接/图片/多商品订单页）
        return kafkaRawStream
                .map(jsonStr -> {
                    try {
                        return JSONObject.parseObject(jsonStr);
                    } catch (Exception e) {
                        // 解析失败数据丢弃，记录日志
                        org.slf4j.LoggerFactory.getLogger(OdsLayer.class)
                                .error("JSON解析失败，数据：{}", jsonStr, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull) // 过滤解析失败数据
                .filter(this::isValidProductIdScene) // 过滤不支持的商品ID场景
                .uid("ods-standardize-filter")
                .name("ods-standardize-filter");
    }

    /**
     * 2. 从HBase读取商品维度表，输出维度广播流（供DWD层关联）
     */
    public BroadcastStream<ProductDim> getProductDimBroadcastStream() {
        // 2.1 用HbaseUtils读取HBase商品维度表
        SingleOutputStreamOperator<ProductDim> productDimSource = env.addSource(
                new RichSourceFunction<ProductDim>() {
                    private HbaseUtils hbaseUtils;
                    private volatile boolean isRunning = true;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化HBase连接（用ConfigUtils读取ZK地址）
                        hbaseUtils = new HbaseUtils(WorkOrderConstant.HBASE_ZK_QUORUM);
                    }

                    @Override
                    public void run(SourceContext<ProductDim> ctx) throws Exception {
                        // 全量读取商品维度表（HBase表名：namespace:table）
                        String fullTableName = WorkOrderConstant.HBASE_NAMESPACE + ":" + WorkOrderConstant.HBASE_PRODUCT_TABLE;
                        List<JSONObject> hbaseRawData = hbaseUtils.getAll(fullTableName, Long.MAX_VALUE);

                        // 2.2 转换为ProductDim对象并发送
                        for (JSONObject raw : hbaseRawData) {
                            if (!isRunning) break;
                            ProductDim productDim = new ProductDim(
                                    raw.getString("row_key"), // HBase行键=商品ID
                                    raw.getString("product_name"),
                                    raw.getString("shop_id"),
                                    "1".equals(raw.getString("is_valid")) // 有效标识：1=有效
                            );
                            ctx.collect(productDim);
                        }

                        // 维度表定时刷新（每1小时更新一次，避免维度数据过期）
                        while (isRunning) {
                            Thread.sleep(3600000);
                            hbaseRawData = hbaseUtils.getAll(fullTableName, Long.MAX_VALUE);
                            for (JSONObject raw : hbaseRawData) {
                                ProductDim productDim = new ProductDim(
                                        raw.getString("row_key"),
                                        raw.getString("product_name"),
                                        raw.getString("shop_id"),
                                        "1".equals(raw.getString("is_valid"))
                                );
                                ctx.collect(productDim);
                            }
                        }
                    }

                    @SneakyThrows
                    @Override
                    public void cancel() {
                        isRunning = false;
                        // 关闭HBase连接
                        if (hbaseUtils != null && hbaseUtils.isConnect()) {
                            hbaseUtils.getConnection().close();
                        }
                    }
                },
                TypeInformation.of(ProductDim.class)
        ).uid("ods-hbase-product-source").name("ods-hbase-product-source");

        // 2.3 构建广播流（用MapStateDescriptor存储维度数据）
        MapStateDescriptor<String, ProductDim> productDimDesc = new MapStateDescriptor<>(
                "product-dim-descriptor",
                BasicTypeInfo.STRING_TYPE_INFO, // key=商品ID
                TypeInformation.of(ProductDim.class) // value=商品维度
        );
        return productDimSource.broadcast(productDimDesc);
    }

    /**
     * 辅助方法：判断是否为支持的商品ID场景（参考工单文档）
     */
    private boolean isValidProductIdScene(JSONObject jsonObj) {
        String msgType = jsonObj.getString("msg_type");
        String linkType = jsonObj.getString("link_type");
        String entrySource = jsonObj.getString("entry_source");

        // 支持的场景：长链接/商品卡片/商品详情页进线
        boolean supportScene = ("long_link".equals(linkType) 
                || "product_card".equals(msgType) 
                || "product_detail_page".equals(entrySource));
        // 不支持的场景：短链接/图片/多商品订单页进线（直接排除）
        boolean notSupportScene = ("short_link".equals(linkType) 
                || "image_qrcode".equals(msgType) 
                || "multi_product_order_page".equals(entrySource));

        return supportScene && !notSupportScene;
    }
}