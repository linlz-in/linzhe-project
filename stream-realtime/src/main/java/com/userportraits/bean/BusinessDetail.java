package com.userportraits.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/26 20:54
 * @version: 1.8
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BusinessDetail {
    private String userId; // 买家ID
    private String productId; // 商品ID
    private String shopId; // 店铺ID
    private String csId; // 客服ID
    private long eventTime; // 业务时间（毫秒级）
    private String eventType; // 业务类型：inquiry（询单）、order（下单）、payment（付款）
    private long amount; // 金额（分）
    private int quantity; // 商品件数
}