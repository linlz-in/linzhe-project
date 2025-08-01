from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 初始化SparkSession，使用Spark 3.2及以上版本
# 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板
spark = SparkSession.builder.appName("customer_layering") \
    .master("local[*]") \
    .config("spark.sql.version", "3.2") \
    .getOrCreate()

# 创建临时表结构并添加测试数据
# 1. 创建activity_info表
activity_info_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("activity_name", StringType(), True),
    StructField("activity_type", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True)
])
activity_info_data = [
    (1, "客服专属优惠活动1", "客服专属优惠", "2025-07-01", "2025-08-31"),
    (2, "普通优惠活动", "普通优惠", "2025-07-01", "2025-08-31"),
    (3, "客服专属优惠活动2", "客服专属优惠", "2025-07-15", "2025-07-30")
]
activity_info = spark.createDataFrame(activity_info_data, activity_info_schema)
activity_info.createOrReplaceTempView("activity_info")

# 2. 创建activity_rule表
activity_rule_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("activity_id", IntegerType(), True),
    StructField("benefit_amount", IntegerType(), True)
])
activity_rule_data = [
    (1, 1, 50),
    (2, 1, 100),
    (3, 3, 30)
]
activity_rule = spark.createDataFrame(activity_rule_data, activity_rule_schema)
activity_rule.createOrReplaceTempView("activity_rule")

# 3. 创建activity_sku表
activity_sku_schema = StructType([
    StructField("activity_id", IntegerType(), True),
    StructField("sku_id", IntegerType(), True)
])
activity_sku_data = [
    (1, 101),
    (1, 102),
    (3, 103)
]
activity_sku = spark.createDataFrame(activity_sku_data, activity_sku_schema)
activity_sku.createOrReplaceTempView("activity_sku")

# 4. 创建coupon_use表
coupon_use_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("activity_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("get_time", StringType(), True),
    StructField("used_time", StringType(), True),
    StructField("coupon_status", IntegerType(), True),
    StructField("order_id", IntegerType(), True)
])
coupon_use_data = [
    (1, 1, 1001, "2025-07-25 10:00:00", "2025-07-25 10:30:00", 2, 5001),
    (2, 1, 1002, "2025-07-26 14:00:00", None, 1, None),
    (3, 1, 1003, "2025-07-27 09:15:00", "2025-07-27 09:45:00", 2, 5002),
    (4, 3, 1004, "2025-07-28 16:20:00", "2025-07-28 16:50:00", 2, 5003)
]
coupon_use = spark.createDataFrame(coupon_use_data, coupon_use_schema)
coupon_use.createOrReplaceTempView("coupon_use")

# 5. 创建order_info表
order_info_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("create_time", StringType(), True),
    StructField("pay_status", IntegerType(), True)
])
order_info_data = [
    (5001, 1001, "2025-07-25 10:30:00", 1),
    (5002, 1003, "2025-07-27 09:45:00", 1),
    (5003, 1004, "2025-07-28 16:50:00", 1),
    (5004, 1001, "2025-07-29 11:20:00", 0)
]
order_info = spark.createDataFrame(order_info_data, order_info_schema)
order_info.createOrReplaceTempView("order_info")

# 6. 创建sku_info表
sku_info_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("sku_name", StringType(), True),
    StructField("price", IntegerType(), True)
])
sku_info_data = [
    (101, "商品A", 500),
    (102, "商品B", 1000),
    (103, "商品C", 300)
]
sku_info = spark.createDataFrame(sku_info_data, sku_info_schema)
sku_info.createOrReplaceTempView("sku_info")



print("========================================分层======================================")

# 1. ods层 原始数据清洗与整合（修改为临时视图）
spark.sql("""
create or replace temporary view ods_customer_service_discount as
select 
    a.id as activity_id,
    a.activity_name,
    a.activity_type,
    a.start_date,
    a.end_date,
    r.benefit_amount AS discount_amount,
    s.sku_id,
    sku.sku_name,
    sku.price AS original_price,
    c.id AS coupon_id,
    c.user_id,
    c.get_time AS send_time,
    c.used_time AS verify_time,
    c.coupon_status,
    c.order_id,
    o.pay_status,
    o.create_time AS order_create_time
from activity_info a
left join activity_rule r ON a.id = r.activity_id
left join activity_sku s ON a.id = s.activity_id
left join sku_info sku ON s.sku_id = sku.id
left join coupon_use c ON a.id = c.activity_id
left join order_info o ON c.order_id = o.order_id
where a.activity_type = '客服专属优惠'
""")

# 2. dwd层 明细数据处理（修改为临时视图）
spark.sql("""
create or replace temporary view dwd_customer_service_detail as
select
    *,
    original_price - discount_amount as estimated_price,
    case
        when current_date() between start_date and end_date then '进行中'
        else '已结束'
    end as activity_status,
    date_format(send_time, 'yyyy-MM-dd') as send_day,
    datediff(current_date(), to_date(send_time)) as days_since_send
from ods_customer_service_discount
""")

# 3. ADS层：指标汇总计算（修改为临时视图）
spark.sql("""
create or replace temporary view ads_customer_service_overview as
select 
    activity_id,
    activity_name,
    activity_status,
    -- 多时间维度发送次数统计
    COUNT(distinct case when days_since_send <= 30 then coupon_id end) as send_count_30d,
    COUNT(distinct case when days_since_send <= 7 then coupon_id end) as send_count_7d,
    COUNT(distinct case when days_since_send <= 1 then coupon_id end ) as send_count_1d,
    -- 发送总金额
    SUM(discount_amount) as total_send_amount,
    -- 核销次数
    COUNT(distinct case when coupon_status = 2 then coupon_id end) as verify_count,
    -- 支付次数（支持跨天统计）
    COUNT(distinct case when pay_status = 1 then order_id end) as pay_count
from dwd_customer_service_detail
group by activity_id, activity_name, activity_status
""")

# 查看ADS层结果
print("\n=== 分层ADS层结果 ===")
spark.sql("select * from ads_customer_service_overview").show(truncate=False)


print("========================================不分层======================================")

# 直接关联原始表计算指标
no_layer_result = activity_info.alias("a") \
    .join(activity_rule.alias("r"), col("a.id") == col("r.activity_id"), "left") \
    .join(coupon_use.alias("c"), col("a.id") == col("c.activity_id"), "left") \
    .join(order_info.alias("o"), col("c.order_id") == col("o.order_id"), "left") \
    .filter(col("a.activity_type") == "客服专属优惠") \
    .groupBy(col("a.id").alias("activity_id"), col("a.activity_name")) \
    .agg(
    countDistinct("c.id").alias("total_send_count"),
    sum("r.benefit_amount").alias("total_send_amount"),
    countDistinct(when(col("c.coupon_status") == 2, col("c.id"))).alias("total_verify_count"),
    countDistinct(when(col("o.pay_status") == 1, col("o.order_id"))).alias("total_pay_count"),
    # 处理支付次数可能大于发送次数的特殊情况
    when(
        countDistinct(when(col("o.pay_status") == 1, col("o.order_id"))) > countDistinct("c.id"),
        "包含历史发送优惠的支付订单"
    ).otherwise("正常").alias("pay_vs_send_note")
)

# 查看不分层结果
print("\n========================================= 不分层结果 ===================================")
no_layer_result.show(truncate=False)


print("========================================执行数据一致性测试======================================")

# 将不分层结果注册为临时视图
no_layer_result.createOrReplaceTempView("no_layer_result")

# 验证分层与不分层数据一致性
consistency_test = spark.sql("""
select 
    layer.activity_id,
    layer.send_count_30d as layer_send_count,
    no_layer.total_send_count as no_layer_send_count,
    case 
        when layer.send_count_30d = no_layer.total_send_count then '一致'
        else '不一致'
    end as consistency_check
from ads_customer_service_overview layer
left join no_layer_result no_layer
    on layer.activity_id = no_layer.activity_id
""")

print("\n=================== 分层与不分层数据一致性测试结果 ==========================")
consistency_test.show(truncate=False)

# 验证支付次数大于发送次数的场景
pay_over_send_test = spark.sql("""
select 
    activity_id,
    activity_name,
    total_send_count,
    total_pay_count,
    pay_vs_send_note
from no_layer_result
where total_pay_count > total_send_count
""")

print("\n=================================== 支付次数大于发送次数的场景测试 ====================================")
pay_over_send_test.show(truncate=False)

# 保存结果用于看板展示（如果需要持久化到Hive，确保Hive配置正确）
spark.sql("select * from ads_customer_service_overview").write \
    .mode("overwrite") \
    .saveAsTable("customer_service_discount_dashboard")

print("\n=========================== 看板数据已保存至 customer_service_discount_dashboard 表 ==============================")

spark.stop()