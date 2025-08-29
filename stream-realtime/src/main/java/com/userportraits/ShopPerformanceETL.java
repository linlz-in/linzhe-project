package com.userportraits;

import com.alibaba.fastjson.JSON;
import com.userportraits.bean.Common;
import com.userportraits.data.DataGenerator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/27 9:57
 * @version: 1.8
 *
 * ETL: 数据清洗、转换、商品ID识别判断
 */
public class ShopPerformanceETL {
    // 定义侧输出流标签，用于收集错误数据
    private static final OutputTag<String> errorTag = new OutputTag<String>("etl-error-data") {};
    /**
     * 执行ETL
     */
    public static DataStream<Common.CleanedShopData> processETL(DataStream<String> rawStream) {
        return rawStream
                // 1. JSON解析为原始数据
                .map(jsonStr -> {
                    try {
                        return JSON.parseObject(jsonStr, RawShopData.class);
                    } catch (Exception e) {
                        throw new RuntimeException("JSON 解析失败: " + jsonStr,e); //解析失败的数据发送到侧输出流
                    }
                })
                .process(new ProcessFunction<RawShopData, Common.CleanedShopData>() {
                    @Override
                    public void processElement(RawShopData rawData, ProcessFunction<RawShopData, Common.CleanedShopData>.Context ctx, Collector<Common.CleanedShopData> out) throws Exception {
                        try {
                            Common.CleanedShopData cleanedData = new Common.CleanedShopData();
                            DateTimeFormatter dtf = DateTimeFormatter.ofPattern(Common.DATETIME_FORMAT);

                            //基础字段赋值
                            cleanedData.setRecordId(rawData.getRecorId());
                            cleanedData.setUserId(rawData.getUserId());
                            cleanedData.setProductId(rawData.getProductId());
                            cleanedData.setCustomerServiceId(rawData.getCustomerServiceId());
                            cleanedData.setDimension(rawData.getDimension());
                            //时间字段转换
                            if (rawData.getConsultTime() != null){
                                cleanedData.setConsultTime(LocalDateTime.parse(rawData.getConsultTime(), dtf));
                            }
                            if (rawData.getInquireTime() != null){
                                cleanedData.setInquireTime(LocalDateTime.parse(rawData.getInquireTime(), dtf));
                            }
                            if (rawData.getPlaceOrderTime() != null){
                                cleanedData.setPlaceOrderTime(LocalDateTime.parse(rawData.getPlaceOrderTime(), dtf));
                            }
                            if (rawData.getPayTime() != null){
                                cleanedData.setPayTime(LocalDateTime.parse(rawData.getPayTime(), dtf));
                            }

                            //数值字段处理
                            cleanedData.setPayAmount(rawData.getPayAmount());
                            cleanedData.setPayQuantity(rawData.getPayQuantity());
                            cleanedData.setProductIdIdentified(!"UNIDENTIFIED".equals(rawData.getProductId()));

                            // 过滤无效数据
                            if (cleanedData.getUserId() != null && cleanedData.getConsultTime() != null){
                                out.collect(cleanedData);
                            }else {
                                ctx.output(errorTag, "无效数据 （缺少用户id或咨询时间）：" + JSON.toJSONString(rawData));
                            }
                        } catch (Exception e) {
                            ctx.output(errorTag, "数据转换失败：" + JSON.toJSONString(rawData) + ", 错误：" + e.getMessage());
                        }
                    }
                })
                .returns(TypeInformation.of(Common.CleanedShopData.class))
                .name("shop-performance-etl")
                .uid("shop-performance-etl-uid");

    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class RawShopData {
        private String recorId;
        private String userId;
        private String productId;
        private String customerServiceId;
        private String dimension;
        private String consultTime;
        private String inquireTime;
        private String placeOrderTime;
        private String payTime;
        private Double payAmount;
        private Integer payQuantity;
    }
}
