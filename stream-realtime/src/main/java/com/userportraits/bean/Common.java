package com.userportraits.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/27 9:32
 * @version: 1.8
 *
 * 通用实体与常量定义
 */


public class Common {
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CleanedShopData { //核心业务数据
            private String recordId;
            private String userId;
            private String productId;
            private String customerServiceId;
            private String dimension;
            private LocalDateTime consultTime;
            private LocalDateTime inquireTime;
            private LocalDateTime placeOrderTime;
            private LocalDateTime payTime;
            private Double payAmount;
            private Integer payQuantity;
            private boolean isProductIdIdentified; // 商品ID是否可识别
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ShopPerformanceResult {  // 店铺绩效指标结果
        private String productId;
        private String dimension; // 店铺/客服/静默
        private String timeWindow; // 时间窗口（如：2025-01-22 09:00-10:00）
        private Integer consultUserCount; // 咨询人数
        private Integer inquireUserCount; // 询单人数
        private Integer sameDayInquireUserCount; // 当日询单人数
        private Double inquireConversionRate; // 询单转化率
        private Double sameDayInquireConversionRate; // 当日询单转化率
        private Integer sameDayPayUserCount; // 当日付款人数
        private Double sameDayPayAmount; // 当日付款金额
        private Integer finalPayUserCount; // 最终付款人数
        private Double finalPayAmount; // 最终付款金额
        private Integer finalPayQuantity; // 最终付款件数
        private Long windowEndTs; // 窗口结束时间戳（用于判断数据时效性）
    }



}
