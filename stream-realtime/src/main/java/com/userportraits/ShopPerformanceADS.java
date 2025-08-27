package com.userportraits;

import com.userportraits.bean.Common;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/27 14:16
 * @version: 1.8
 *
 * ads： 店铺绩效核心指标计算（十个指标 + 3个链路分析）
 */
public class ShopPerformanceADS {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(Common.DATETIME_FORMAT);
    private static final int INQUIRE_VALIO_DURATION = 24 * 60 ;   //询单有效时长：24小时

    /**
     * 计算核心绩效指标
     * @param cleanedStream 清洗后的结构化数据流
     * @return 指标计算结果
     */

    public static DataStream<Common.ShopPerformanceResult> calculateIndicators(DataStream<Common.CleanedShopData> cleanedStream) {
        // 1. 按 商品ID、分析维度  分组        按5分钟滚动窗口聚合
        return cleanedStream
                .keyBy((KeySelector<Common.CleanedShopData, String>) data -> data.getProductId() + "_" + data.getDimension())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) // 5分钟窗口
                .aggregate(new AggregateFunction<Common.CleanedShopData, IndicatorAccumulator, Common.ShopPerformanceResult>() {
                    @Override
                    public IndicatorAccumulator createAccumulator() {
                        return new IndicatorAccumulator();
                    }

                    @Override
                    public IndicatorAccumulator add(Common.CleanedShopData data, IndicatorAccumulator accumulator) {
                        // 累加咨询人数（去重）
                        accumulator.consultUsers.add(data.getUserId());
                        // 累加询单人数 （有询单时间且去重）
                        if (data.getInquireTime() != null) {
                            accumulator.inquireUsers.add(data.getUserId());

                            // 判断是否为当日询单 （咨询与询单在同一天）
                            if (data.getConsultTime() != null && data.getConsultTime().toLocalDate().equals(data.getInquireTime().toLocalDate())) {
                                accumulator.sameDayInquireUsers.add(data.getUserId());
                            }
                        }

                        // 下单后付款行为分析
                        if (data.getPlaceOrderTime() != null && data.getPayTime() != null) {
                            // 判断下单、付款 在同一天
                            if (data.getPlaceOrderTime().toLocalDate().equals(data.getPayTime().toLocalDate())){
                                accumulator.sameDayPayUsers.add(data.getUserId());
                                accumulator.sameDayPayAmount += data.getPayAmount();
                            }
                            // 最终付款指标
                            accumulator.finalPayUsers.add(data.getUserId());
                            accumulator.finalPayAmount += data.getPayAmount();
                            accumulator.finalPayQuantity += data.getPayQuantity();
                        }
                        return accumulator;
                    }

                    @Override
                    public Common.ShopPerformanceResult getResult(IndicatorAccumulator accumulator) {
                        Common.ShopPerformanceResult result = new Common.ShopPerformanceResult();
                         // 基础指标赋值
                        result.setConsultUserCount(accumulator.consultUsers.size());
                        result.setInquireUserCount(accumulator.inquireUsers.size());
                        result.setSameDayInquireUserCount(accumulator.sameDayInquireUsers.size());
                        result.setSameDayPayUserCount(accumulator.sameDayPayUsers.size());
                        result.setSameDayPayAmount(accumulator.sameDayPayAmount);
                        result.setFinalPayUserCount(accumulator.finalPayUsers.size());
                        result.setFinalPayAmount(accumulator.finalPayAmount);
                        result.setFinalPayQuantity(accumulator.finalPayQuantity);

                        // 计算转换率
                        result.setInquireConversionRate(
                                accumulator.consultUsers.isEmpty() ? 0.0 :
                                accumulator.inquireUsers.size() * 1.0 / accumulator.consultUsers.size()
                        );

                        result.setSameDayInquireConversionRate(
                                accumulator.consultUsers.isEmpty() ? 0.0 :
                                accumulator.sameDayInquireUsers.size() * 1.0 / accumulator.consultUsers.size()
                        );
                        return result;
                    }

                    @Override
                    public IndicatorAccumulator merge(IndicatorAccumulator a, IndicatorAccumulator b) {
                        a.consultUsers.addAll(b.consultUsers);
                        a.inquireUsers.addAll(b.inquireUsers);
                        a.sameDayInquireUsers.addAll(b.sameDayInquireUsers);
                        a.sameDayPayUsers.addAll(b.sameDayPayUsers);
                        a.sameDayPayAmount += b.sameDayPayAmount;
                        a.finalPayUsers.addAll(b.finalPayUsers);
                        a.finalPayAmount += b.finalPayAmount;
                        a.finalPayQuantity += b.finalPayQuantity;
                        return a;
                    }
                },  // 窗口结果处理 （补充窗口时间、商品id、维度信息）
                        (String key, TimeWindow window, Iterable<Common.ShopPerformanceResult> results, Collector<Common.ShopPerformanceResult> out) -> {
                            Common.ShopPerformanceResult result = results.iterator().next();
                            //解析key获取商品id 和 维度
                            String[] keyParts = key.split("_");
                            result.setProductId(keyParts[0]);
                            result.setDimension(keyParts[1]);
                            // 格式化窗口时间
                            String windownTime = LocalDateTime.ofEpochSecond(window.getStart() / 1000, 0, ZoneOffset.UTC)
                                    .format(dtf) + " - " +
                                    LocalDateTime.ofEpochSecond(window.getEnd() / 1000, 0, ZoneOffset.UTC)
                                            .format(dtf);
                            result.setTimeWindow(windownTime);
                            out.collect(result);
                        }

                ).name("shop-performance-ads")
                .uid("shop-performance-ads-uid");

    }

    // 指标累加器，用于窗口内指标聚合
    private static class IndicatorAccumulator {
        // 使用set实现用户去重计数
        final Set<String> consultUsers = new HashSet<>();
        final Set<String> inquireUsers = new HashSet<>();
        final Set<String> sameDayInquireUsers = new HashSet<>();
        final Set<String> sameDayPayUsers = new HashSet<>();
        // 金额和数量累加
        double sameDayPayAmount = 0.0;
        double finalPayAmount = 0.0;
        int finalPayQuantity = 0;
        final Set<String> finalPayUsers = new HashSet<>();
    }
}
