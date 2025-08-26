package com.userportraits.dwd;

import com.alibaba.fastjson.JSONObject;
import com.userportraits.bean.BusinessDetail;
import com.userportraits.bean.ProductDim;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * DWD层：明细数据清洗与维度关联（工单编号：大数据-用户画像-02-服务主题店铺绩效）
 */
public class DwdLayer {
    private final StreamExecutionEnvironment env;

    public DwdLayer(StreamExecutionEnvironment env) {
        this.env = env;
    }

    /**
     * 关联业务流与商品维度流，生成明细宽表
     */
    public SingleOutputStreamOperator<BusinessDetail> getBusinessDetailStream(
            SingleOutputStreamOperator<JSONObject> odsBusinessStream,
            BroadcastStream<ProductDim> productDimBroadcastStream,
            MapStateDescriptor<String, ProductDim> productDimDesc) {

        // 1. 连接业务流与维度广播流
        BroadcastConnectedStream<JSONObject, ProductDim> connectedStream = odsBusinessStream
                .connect(productDimBroadcastStream);

        // 2. 关联维度：补全店铺ID，转换为BusinessDetail
        SingleOutputStreamOperator<BusinessDetail> detailStream = connectedStream
                .process(new BroadcastProcessFunction<JSONObject, ProductDim, BusinessDetail>() {
                    // 处理业务流（每条业务数据关联维度）
                    @Override
                    public void processElement(JSONObject businessJson, ReadOnlyContext ctx, Collector<BusinessDetail> out) throws Exception {
                        String productId = businessJson.getString("product_id");
                        // 从广播状态中获取商品维度
                        ProductDim productDim = ctx.getBroadcastState(productDimDesc).get(productId);

                        if (productDim != null && productDim.isValid()) {
                            // 构建业务明细对象（补全店铺ID）
                            BusinessDetail detail = new BusinessDetail(
                                    businessJson.getString("user_id"), // 买家ID
                                    productId, // 商品ID
                                    productDim.getShopId(), // 关联店铺ID
                                    businessJson.getString("cs_id"), // 客服ID
                                    businessJson.getLong("event_time"), // 业务时间
                                    businessJson.getString("event_type"), // 业务类型（inquiry/order/payment）
                                    businessJson.getLong("amount"), // 金额（分）
                                    businessJson.getInteger("quantity") // 件数
                            );
                            out.collect(detail);
                        }
                    }

                    // 处理维度流（更新广播状态）
                    @Override
                    public void processBroadcastElement(ProductDim productDim, Context ctx, Collector<BusinessDetail> out) throws Exception {
                        ctx.getBroadcastState(productDimDesc).put(productDim.getProductId(), productDim);
                    }
                })
                .uid("dwd-dim-join")
                .name("dwd-dim-join");

        // 3. 添加Watermark（处理5秒乱序，基于业务时间）
        return detailStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<BusinessDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((detail, ts) -> detail.getEventTime())
                                .withIdleness(Duration.ofMinutes(1)) // 处理空闲流
                )
                .uid("dwd-add-watermark")
                .name("dwd-add-watermark");
    }
}