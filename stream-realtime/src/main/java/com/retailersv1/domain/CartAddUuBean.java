package com.retailersv1.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/22 19:05
 * @version: 1.8
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
