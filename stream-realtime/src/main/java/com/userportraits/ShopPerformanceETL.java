package com.userportraits;

import com.alibaba.fastjson.JSON;
import com.userportraits.bean.Common;
import com.userportraits.data.DataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

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
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(Common.DATETIME_FORMAT);
    /**
     * 执行ETL
     * @param rawStream 原始json数据流
     * @ return 清洗后的结构化数据流
     */
    public static DataStream<Common.CleanedShopData> processETL(DataStream<String> rawStream) {
        return rawStream
                // 1. JSON解析为实体
                .map(jsonStr -> {
                    try {
                        return JSON.parseObject(jsonStr, DataGenerator.ShopPerformanceData.class);
                    } catch (Exception e) {
                        // 过滤JSON格式错误的数据
                        return null;
                    }
                })
                .filter(Objects::nonNull) // 过滤null数据
                // 2. 数据清洗 转换
                .flatMap((DataGenerator.ShopPerformanceData rawData, Collector<Common.CleanedShopData> out) ->{
                    Common.CleanedShopData cleanedData = new Common.CleanedShopData();
                    cleanedData.setRecordId(rawData.getRecorId());
                    cleanedData.setUserId(rawData.getUserId());
                    cleanedData.setCustomerServiceId(rawData.getCustomerServiceId());
                    cleanedData.setDimension(rawData.getDimension());
                    cleanedData.setPayAmount(rawData.getPayAmount() == null ? 0.0 : rawData.getPayAmount());
                    cleanedData.setPayQuantity(rawData.getPayQuantity() == null ? 0 : rawData.getPayQuantity());

                    // 2.1 时间格式转换
                    try {
                        cleanedData.setConsultTime(rawData.getConsultTime() != null ? LocalDateTime.parse(rawData.getConsultTime(), dtf) : null);
                        cleanedData.setInquireTime(rawData.getInquireTime() != null ? LocalDateTime.parse(rawData.getInquireTime(), dtf) : null);
                        cleanedData.setPlaceOrderTime(rawData.getPlaceOrderTime() != null ? LocalDateTime.parse(rawData.getPlaceOrderTime(), dtf) : null);
                        cleanedData.setPayTime(rawData.getPayTime() != null ? LocalDateTime.parse(rawData.getPayTime(), dtf) : null);
                    } catch (DateTimeParseException e){
                        cleanedData.setConsultTime(null);
                        cleanedData.setInquireTime(null);
                        cleanedData.setPlaceOrderTime(null);
                        cleanedData.setPayTime(null);
                    }

                    // 2.2 商品id可识别判断
                    boolean isIdentified = false;
                    if (rawData.getProductId() != null && !rawData.getProductId().isEmpty()) {
                        switch (rawData.getProductId()){
                            case "长链接":
                            case "商品卡片":
                                isIdentified = true;
                                break;
                            case "短链接":
                            case "图片":
                            case "二维码":
                                break;
                            default:
                                // 进线渠道判断：商品详情页进线=可识别
                                isIdentified = "商品详情页".equals(rawData.getEntryChannel());
                        }
                    }
                    cleanedData.setProductId(isIdentified ? rawData.getProductId() : "UNIDENTIFIED");
                    cleanedData.setProductIdIdentified(isIdentified);

                    // 过滤关联维度表缺失的数据
                    if (cleanedData.getUserId() != null && cleanedData.getConsultTime() != null) {
                        out.collect(cleanedData);
                    }
                }).name("shop-performance-etl")
                .uid("shop-performance-etl-uid");

    }
}
