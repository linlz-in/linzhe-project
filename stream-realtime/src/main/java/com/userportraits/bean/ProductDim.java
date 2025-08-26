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
public class ProductDim {
    private String productId; // 商品ID（唯一标识）
    private String productName; // 商品名称
    private String shopId; // 所属店铺ID
    private boolean isValid; // 是否有效（过滤无效商品）
}