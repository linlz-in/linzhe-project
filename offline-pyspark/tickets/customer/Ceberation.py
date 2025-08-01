from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# 初始化Spark会话，启用Hive支持
spark = SparkSession.builder.appName("spu_order") \
    .master("local[*]") \
    .enableHiveSupport() \
    .config("spark.sql.version", "3.2") \
    .config("hive.metastore.uris", "thrift://192.168.200.101:9083") \
    .getOrCreate()

# 设置随机种子，保证结果可重现
random.seed(42)

# 生成日期范围（近30天）
end_date = datetime(2025, 1, 25)
dates = [(end_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]


# 函数：通过SQL创建表并验证结构     表名        建表sql        是否重建
def create_and_verify_table(table_name, create_sql, recreate=False):
    """创建表后立即验证字段类型，确保与预期一致"""
    if recreate:
        spark.sql(f"drop table if exists {table_name}")
    spark.sql(create_sql)
    # 验证表结构
    desc = spark.sql(f"describe {table_name}")
    print(f"\n表 {table_name} 结构验证:")
    desc.show()
    print(f"Hive表 {table_name} 已创建或已存在")


# 1. 生成活动信息表 (activity_info)
activity_data = [
    (1, "客服专属满减活动", "客服专属优惠", "2025-01-01", "2025-01-31"),
    (2, "客服专享折扣活动", "客服专属优惠", "2025-01-10", "2025-02-10"),
    (3, "新用户专享优惠", "新用户优惠", "2025-01-01", "2025-01-31"),
    (4, "客服限时特惠", "客服专属优惠", "2025-01-20", "2025-01-27"),
    (5, "客服节日特惠", "客服专属优惠", "2025-01-15", "2025-01-25")
]

activity_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("activity_name", StringType(), False),
    StructField("activity_type", StringType(), False),
    StructField("start_date", StringType(), False),
    StructField("end_date", StringType(), False)
])

activity_info = spark.createDataFrame(activity_data, activity_schema)
activity_sql = """
CREATE TABLE IF NOT EXISTS tickets.activity_info (
    id INT,
    activity_name STRING,
    activity_type STRING,
    start_date STRING,
    end_date STRING
) STORED AS PARQUET
"""
create_and_verify_table("tickets.activity_info", activity_sql, recreate=True)
activity_info.write.mode("overwrite").insertInto("tickets.activity_info")
activity_info.createOrReplaceTempView("activity_info")


# 2. 生成活动规则表 (activity_rule)
rule_data = [
    (1, 1, 50), (2, 1, 100), (3, 2, 10),
    (4, 2, 20), (5, 4, 30), (6, 5, 40)
]

rule_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("activity_id", IntegerType(), False),
    StructField("benefit_amount", IntegerType(), False)
])

activity_rule = spark.createDataFrame(rule_data, rule_schema)
rule_sql = """
CREATE TABLE IF NOT EXISTS tickets.activity_rule (
    id INT,
    activity_id INT,
    benefit_amount INT
) STORED AS PARQUET
"""
create_and_verify_table("tickets.activity_rule", rule_sql, recreate=True)
activity_rule.write.mode("overwrite").insertInto("tickets.activity_rule")
activity_rule.createOrReplaceTempView("activity_rule")


# 3. 生成商品信息表 (sku_info)
sku_data = [
    (101, "智能手机A", 2999), (102, "笔记本电脑B", 5999),
    (103, "平板电脑C", 2499), (104, "智能手表D", 1599),
    (105, "无线耳机E", 899), (106, "智能音箱F", 399),
    (107, "移动电源G", 199), (108, "蓝牙音箱H", 599)
]

sku_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("sku_name", StringType(), False),
    StructField("price", IntegerType(), False)
])

sku_info = spark.createDataFrame(sku_data, sku_schema)
sku_sql = """
CREATE TABLE IF NOT EXISTS tickets.sku_info (
    id INT,
    sku_name STRING,
    price INT
) STORED AS PARQUET
"""
create_and_verify_table("tickets.sku_info", sku_sql, recreate=True)
sku_info.write.mode("overwrite").insertInto("tickets.sku_info")
sku_info.createOrReplaceTempView("sku_info")


# 4. 生成活动商品关联表 (activity_sku)
activity_sku_data = [
    (1, 101), (1, 102), (1, 103), (1, 104),
    (2, 103), (2, 104), (2, 105),
    (4, 104), (4, 105), (4, 106),
    (5, 107), (5, 108), (5, 101)
]

activity_sku_schema = StructType([
    StructField("activity_id", IntegerType(), False),
    StructField("sku_id", IntegerType(), False)
])
# 调用spark.createDataFrame将列表activity_sku_data转换为 Spark DataFrame，
# 同时应用activity_sku_schema定义的结构
activity_sku = spark.createDataFrame(activity_sku_data, activity_sku_schema)
activity_sku_sql = """
CREATE TABLE IF NOT EXISTS tickets.activity_sku (
    activity_id INT,
    sku_id INT
) STORED AS PARQUET
"""
create_and_verify_table("tickets.activity_sku", activity_sku_sql, recreate=True)
activity_sku.write.mode("overwrite").insertInto("tickets.activity_sku")
activity_sku.createOrReplaceTempView("activity_sku")


# 5. 生成订单表 (order_info) - 最终解决方案
order_ids = list(range(10001, 11001))  # 生成1000个订单ID（10001到11000）
order_data = []  # 存储订单数据的列表

for order_id in order_ids:
    user_id = random.randint(1000, 9999)   # 随机生成用户ID（1000-9999之间）
    order_date = random.choice(dates)  # 随机选择订单创建日期（近30天内）
    pay_status = random.choice([0, 1])   # 随机生成支付状态（0=未支付，1=已支付）
    # 将订单信息添加到列表（元组形式）
    order_data.append((order_id, user_id, order_date, pay_status))

# 定义DataFrame并显式转换pay_status为整数（强制确保类型）
order_info = spark.createDataFrame(order_data,
                                   StructType([
                                       StructField("order_id", IntegerType(), False),
                                       StructField("user_id", IntegerType(), False),
                                       StructField("create_time", StringType(), False),
                                       StructField("pay_status", IntegerType(), False)
                                   ])
                                   ).withColumn("pay_status", col("pay_status").cast(IntegerType()))  # 显式转换
# 因为一直出现字段类型错误
# 建表SQL（再次确认pay_status为INT）
order_sql = """
CREATE TABLE IF NOT EXISTS tickets.order_info (
    order_id INT,
    user_id INT,
    pay_status INT
) PARTITIONED BY (create_time STRING)
STORED AS PARQUET
"""
create_and_verify_table("tickets.order_info", order_sql, recreate=True)

# 写入前再次验证数据类型
print("\n订单数据类型验证:")
order_info.printSchema()

# 使用saveAsTable而非insertInto，直接绑定表结构
order_info.write.mode("overwrite").saveAsTable("tickets.order_info")
order_info.createOrReplaceTempView("order_info")


# 6. 生成优惠券使用表 (coupon_use)
coupon_data = []  # 存储优惠券数据的列表
coupon_id = 1   # 优惠券唯一ID，从1开始自增
total_records = 2000  # 模拟2000条数据
records_per_activity = total_records // 3    # 每个活动分配的记录数（2000/3=666..）

for activity in activity_data:   # 遍历所有活动
    activity_id = activity[0]   # 获取活动ID
    activity_type = activity[2]    # 获取活动类型

    # 只处理"客服专属优惠"类型的活动
    if activity_type != "客服专属优惠":
        continue
    # 为当前活动生成指定数量的优惠券记录
    for _ in range(records_per_activity):
        user_id = random.randint(1000, 9999)    # 随机生成用户ID
        send_date = random.choice(dates)    # 随机选择领取日期（近30天内）
        if random.random() < 0.3:   #30%概率使用优惠卷
            coupon_status = 2  # 状态：已核销
            # 计算使用时间（领取后0-3天内）
            used_date = (datetime.strptime(send_date, "%Y-%m-%d") +
                         timedelta(days=random.randint(0, 3))).strftime("%Y-%m-%d")
            # 使用优惠后80%概率支付成功
            order_id = random.choice(order_ids) if random.random() < 0.8 else None
        else:
            coupon_status = 1  # 状态：未使用
            used_date = None    # 未使用则无使用时间
            order_id = None         # 未使用则无关联订单
        # 将生成的记录添加到列表
        coupon_data.append((coupon_id, activity_id, user_id, send_date, used_date, coupon_status, order_id))
        coupon_id += 1     # 自增优惠券ID
        # 达到目标数量后停止生成
        if len(coupon_data) >= total_records:
            break
    if len(coupon_data) >= total_records:
        break

coupon_use = spark.createDataFrame(coupon_data,
                                   StructType([
                                       StructField("id", IntegerType(), False),
                                       StructField("activity_id", IntegerType(), False),
                                       StructField("user_id", IntegerType(), False),
                                       StructField("get_time", StringType(), False),
                                       StructField("used_time", StringType(), True),
                                       StructField("coupon_status", IntegerType(), False),
                                       StructField("order_id", IntegerType(), True)
                                   ])
                                   ).withColumn("coupon_status", col("coupon_status").cast(IntegerType()))  # 显式转换

coupon_sql = """
CREATE TABLE IF NOT EXISTS tickets.coupon_use (
    id INT,
    activity_id INT,
    user_id INT,
    used_time STRING,
    coupon_status INT,
    order_id INT
) PARTITIONED BY (get_time STRING)
STORED AS PARQUET
"""
create_and_verify_table("tickets.coupon_use", coupon_sql, recreate=True)
# 使用saveAsTable确保类型匹配
coupon_use.write.mode("overwrite").saveAsTable("tickets.coupon_use")
coupon_use.createOrReplaceTempView("coupon_use")    # 创建临时视图

print(f"\n生成的优惠券数据量: {coupon_use.count()}条")


# 7. 验证最终表结构
print("\n===== 最终表结构验证 =====")
spark.sql("DESCRIBE tickets.order_info").show()
spark.sql("DESCRIBE tickets.coupon_use").show()

spark.stop()

