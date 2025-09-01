package com.userportraits;

import com.userportraits.bean.Common;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
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
//    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(Common.DATETIME_FORMAT);

    public static DataStream<Common.ShopPerformanceResult> calculateIndicators(DataStream<Common.CleanedShopData> cleanedStream) {
        // 1. 提取事件时间并指定水印策略（处理乱序数据，允许30秒延迟）
        DataStream<Common.CleanedShopData> withEventTime = cleanedStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Common.CleanedShopData>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 允许两小时延迟
                        .withTimestampAssigner((data, timestamp) ->
                                // 以咨询时间作为事件时间基准
                                data.getConsultTime().toEpochSecond(ZoneOffset.UTC) * 1000
                        ));

        // 2. 调整窗口参数：测试环境使用5秒窗口+5秒滑动（确保快速出结果）
        return withEventTime
                .keyBy(data -> data.getProductId() + "_" + data.getDimension())
                // 滚动窗口：窗口大小1天，从每天0点开始（需指定时区，避免UTC时区偏差）
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))  // 减8小时适配北京时间（UTC+8）
                .allowedLateness(Time.seconds(0)) // 允许1小时迟到数据
                .aggregate(new IndicatorAggregate(), new WindowResultAssigner())
                .name("real-time-shop-performance")
                .uid("real-time-shop-performance-uid");
    }

    // 聚合函数：计算各项指标
    private static class IndicatorAggregate implements AggregateFunction<
            Common.CleanedShopData,
            IndicatorAccumulator,
            Common.ShopPerformanceResult> {

        @Override
        public IndicatorAccumulator createAccumulator() {
            return new IndicatorAccumulator();
        }

        @Override
        public IndicatorAccumulator add(Common.CleanedShopData data, IndicatorAccumulator accumulator) {
            // 咨询用户去重
            accumulator.consultUsers.add(data.getUserId());

            // 询单用户处理
            if (data.getInquireTime() != null) {
                accumulator.inquireUsers.add(data.getUserId());

                // 当日询单判断
                if (isSameDay(data.getConsultTime(), data.getInquireTime())) {
                    accumulator.sameDayInquireUsers.add(data.getUserId());
                }
            }

            // 付款用户处理
            if (data.getPayTime() != null && data.getPlaceOrderTime() != null) {
                accumulator.finalPayUsers.add(data.getUserId());
                accumulator.finalPayAmount += data.getPayAmount() != null ? data.getPayAmount() : 0;
                accumulator.finalPayQuantity += data.getPayQuantity() != null ? data.getPayQuantity() : 0;

                // 当日付款判断
                if (isSameDay(data.getPlaceOrderTime(), data.getPayTime())) {
                    accumulator.sameDayPayUsers.add(data.getUserId());
                    accumulator.sameDayPayAmount += data.getPayAmount() != null ? data.getPayAmount() : 0;
                }
            }

            return accumulator;
        }

        @Override
        public Common.ShopPerformanceResult getResult(IndicatorAccumulator accumulator) {
            Common.ShopPerformanceResult result = new Common.ShopPerformanceResult();

            // 基础计数指标
            result.setConsultUserCount(accumulator.consultUsers.size());
            result.setInquireUserCount(accumulator.inquireUsers.size());
            result.setSameDayInquireUserCount(accumulator.sameDayInquireUsers.size());
            result.setSameDayPayUserCount(accumulator.sameDayPayUsers.size());
            result.setSameDayPayAmount(accumulator.sameDayPayAmount);
            result.setFinalPayUserCount(accumulator.finalPayUsers.size());
            result.setFinalPayAmount(accumulator.finalPayAmount);
            result.setFinalPayQuantity(accumulator.finalPayQuantity);

            // 转化率指标（避免除零）
            int consultCount = result.getConsultUserCount();
            result.setInquireConversionRate(consultCount > 0 ?
                    (double) result.getInquireUserCount() / consultCount : 0);
            result.setSameDayInquireConversionRate(consultCount > 0 ?
                    (double) result.getSameDayInquireUserCount() / consultCount : 0);

            return result;
        }

        @Override
        public IndicatorAccumulator merge(IndicatorAccumulator a, IndicatorAccumulator b) {
            a.consultUsers.addAll(b.consultUsers);
            a.inquireUsers.addAll(b.inquireUsers);
            a.sameDayInquireUsers.addAll(b.sameDayInquireUsers);
            a.sameDayPayUsers.addAll(b.sameDayPayUsers);
            a.finalPayUsers.addAll(b.finalPayUsers);
            a.sameDayPayAmount += b.sameDayPayAmount;
            a.finalPayAmount += b.finalPayAmount;
            a.finalPayQuantity += b.finalPayQuantity;
            return a;
        }

        // 判断两个时间是否在同一天
        private boolean isSameDay(LocalDateTime time1, LocalDateTime time2) {
            return time1.toLocalDate().isEqual(time2.toLocalDate());
        }
    }

    // 窗口结果分配器：补充窗口信息
    private static class WindowResultAssigner implements WindowFunction<
            Common.ShopPerformanceResult,
            Common.ShopPerformanceResult,
            String,
            TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window,
                          Iterable<Common.ShopPerformanceResult> input,
                          Collector<Common.ShopPerformanceResult> out) {
            // 打印窗口信息，确认是否触发
            System.out.println("窗口触发：" + window.getStart() + " ~ " + window.getEnd());
            Common.ShopPerformanceResult result = input.iterator().next();
            // 解析key获取商品id和维度
            String[] keyParts = key.split("_");
            result.setProductId(keyParts[0]);
            result.setDimension(keyParts[1]);

            // 格式化窗口时间为“yyyy-MM-dd”（天级）
            String windowDate = LocalDateTime.ofEpochSecond(window.getStart() / 1000, 0, ZoneOffset.ofHours(8))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            result.setTimeWindow(windowDate);
            result.setWindowEndTs(window.getEnd());  // 直接存储日期，而非时间段

            out.collect(result);
        }
    }

    // 累加器类：存储聚合过程中的中间状态
    private static class IndicatorAccumulator {
        Set<String> consultUsers = new HashSet<>();
        Set<String> inquireUsers = new HashSet<>();
        Set<String> sameDayInquireUsers = new HashSet<>();
        Set<String> sameDayPayUsers = new HashSet<>();
        Set<String> finalPayUsers = new HashSet<>();
        Double sameDayPayAmount = 0.0;
        Double finalPayAmount = 0.0;
        Integer finalPayQuantity = 0;
    }
}
