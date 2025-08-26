package com.userportraits.bean;

import com.stream.common.utils.ConfigUtils;

/**
 * @Package:
 * @Author: lz
 * @Date: 2025/8/26 20:54
 * @version: 1.8
 */
public class WorkOrderConstant {
    // 工单标识（用于代码注释）
    public static final String WORK_ORDER_ID = "大数据-用户画像-02-服务主题店铺绩效";
    // Kafka配置键（从common-config.properties读取）
    public static final String KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    public static final String KAFKA_CDC_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    public static final String KAFKA_CONSUMER_GROUP = "workorder-02-group";
    // HBase配置键
    public static final String HBASE_ZK_QUORUM = ConfigUtils.getString("zookeeper.server.host.list");
    public static final String HBASE_NAMESPACE = ConfigUtils.getString("hbase.namespace");
    public static final String HBASE_PRODUCT_TABLE = "dim_product"; // 商品维度表
    public static final String HBASE_CUSTOMER_SERVICE_TABLE = "dim_customer_service"; // 客服维度表
    // 时间窗口配置（支持天/小时级统计）
    public static final long DAY_WINDOW_SIZE = 24 * 60 * 60 * 1000L; // 天窗口
    public static final long HOUR_WINDOW_SIZE = 60 * 60 * 1000L; // 小时窗口
}
