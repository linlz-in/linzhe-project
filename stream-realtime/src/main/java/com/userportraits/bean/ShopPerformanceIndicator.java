package com.userportraits.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/26 20:55
 * @version: 1.8
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShopPerformanceIndicator {
    private String productId; // 商品ID
    private String shopId; // 店铺ID
    private String csId; // 客服ID（子维度）
    private String timeWindow; // 时间窗口（如20250122-DAY、2025012210-HOUR）
    // 工单10项指标
    private long consultCount; // 1.咨询人数
    private long inquiryCount; // 2.询单人数
    private long todayInquiryCount; // 3.当日询单人数
    private double inquiryConversionRate; // 4.询单转化率
    private double todayInquiryConversionRate; // 5.当日询单转化率
    private long todayPaymentCount; // 6.当日付款人数
    private long todayPaymentAmount; // 7.当日付款金额
    private long finalPaymentCount; // 8.最终付款人数
    private long finalPaymentAmount; // 9.最终付款金额
    private long finalPaymentQuantity; // 10.最终付款件数
}
