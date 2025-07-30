from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# 初始化SparkSession，支持Hive
spark = SparkSession.builder.appName("spu_order") \
    .master("local[*]") \
    .enableHiveSupport() \
    .config("spark.sql.version", "3.2") \
    .config("hive.metastore.uris", "thrift://192.168.200.101:9083") \
    .getOrCreate()

# 定义数据 schema
schema = StructType([
    StructField("product_id", StringType(), nullable=False),  # 商品ID
    StructField("product_name", StringType(), nullable=False),  # 商品名称
    StructField("category", StringType(), nullable=False),  # 商品分类
    StructField("price", DoubleType(), nullable=False),  # 价格
    StructField("visit_count", IntegerType(), nullable=False),  # 访客数（流量）
    StructField("conversion_rate", DoubleType(), nullable=False),  # 转化率
    StructField("sales_volume", IntegerType(), nullable=False),  # 销量
    StructField("score", IntegerType(), nullable=False),  # 评分
    StructField("content_engagement", IntegerType(), nullable=False),  # 内容互动量
    StructField("new_user_count", IntegerType(), nullable=False),  # 拉新数
    StructField("service_rating", DoubleType(), nullable=False),  # 服务评分
    StructField("data_date", StringType(), nullable=False)  # 数据日期
])

# 生成模拟数据
data = []
categories = ["电子产品", "服装鞋帽", "家居用品", "食品饮料", "美妆个护"]
brands = ["品牌A", "品牌B", "品牌C", "品牌D", "品牌E"]
dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]  # 近30天数据

# 将数据量修改为2000条，满足更全面的商品分布分析需求
for i in range(2000):
    product_id = f"P{10000 + i}"
    category = random.choice(categories)
    brand = random.choice(brands)
    product_name = f"{brand}-{category}-{i % 10 + 1}"
    price = round(random.uniform(10, 2000), 2)
    visit_count = random.randint(100, 5000)
    conversion_rate = round(random.uniform(0.01, 0.2), 4)
    sales_volume = int(visit_count * conversion_rate) + random.randint(-10, 10)
    sales_volume = max(0, sales_volume)  # 确保销量非负

    # 评分设置为1-100的随机整数，70分以上占总数据的70%，符合文档中的评分分级基础
    if random.random() < 0.7:
        score = random.randint(71, 100)
    else:
        score = random.randint(1, 70)

    content_engagement = random.randint(0, visit_count // 5)  # 内容互动量，对应文档中内容营销维度
    new_user_count = random.randint(0, sales_volume // 2)  # 新用户数，对应文档中客户拉新维度
    service_rating = round(random.uniform(3.5, 5.0), 1)  # 服务评分，对应文档中服务质量维度
    data_date = random.choice(dates)

    data.append((
        product_id, product_name, category, price, visit_count,
        conversion_rate, sales_volume, score, content_engagement,
        new_user_count, service_rating, data_date
    ))

# 创建DataFrame
df = spark.createDataFrame(data, schema)

# 使用SparkSQL创建Hive表并写入数据
df.createOrReplaceTempView("temp_product_data")

spark.sql("""
    CREATE TABLE IF NOT EXISTS product_data (
        product_id STRING,
        product_name STRING,
        category STRING,
        price DOUBLE,
        visit_count INT,
        conversion_rate DOUBLE,
        sales_volume INT,
        score INT,
        content_engagement INT,
        new_user_count INT,
        service_rating DOUBLE,
        data_date STRING
    )
    STORED AS PARQUET
""")

spark.sql("INSERT OVERWRITE TABLE product_data SELECT * FROM temp_product_data")

print("2000条模拟数据已成功写入Hive表product_data")

# 关闭SparkSession
spark.stop()