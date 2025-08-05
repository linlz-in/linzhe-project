
# 工单编号:大数据-电商数仓-07-商品主题商品诊断看板
from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. 初始化SparkSession并配置Hive连接
spark = SparkSession.builder.appName("product_diagnosis") \
    .master("local[*]") \
    .enableHiveSupport() \
    .config("spark.sql.version", "3.2") \
    .config("hive.metastore.uris", "thrift://192.168.200.101:9083") \
    .getOrCreate()
# 2. 读取宽表数据
spu_df = spark.table("default.product_data")

# 全品价值评估
# 维度1：评分-金额
score_amount = spark.sql("""
    SELECT product_id, product_name, 
           CASE WHEN score >=85 THEN 'A' 
                WHEN score >=70 THEN 'B' 
                WHEN score >=50 THEN 'C' 
                ELSE 'D' END AS score_level,
           price * sales_volume AS total_amount,
           'score-amount' AS analysis_dimension
    FROM product_data
    ORDER BY total_amount DESC
""")
print("评分-金额=====================>")
score_amount.show()

# 维度2：价格-销量（价格区间与销量分布分析）
price_sales = spark.sql("""
    SELECT product_id, product_name,
           -- 按价格划分区间（根据业务场景可调整区间范围）
           CASE WHEN price < 100 THEN '0-100元'
                WHEN price >= 100 AND price < 500 THEN '100-500元'
                WHEN price >= 500 AND price < 1000 THEN '500-1000元'
                ELSE '1000元以上' END AS price_range,
           sales_volume,
           'price-sales' AS analysis_dimension  -- 标记为价格-销量维度
    FROM product_data
    ORDER BY price_range, sales_volume DESC
""")  # 按价格区间和销量排序，便于观察不同价格带的商品销量表现
print("价格-销量=====================>")
price_sales.show()

# 维度3：访客-销量（访客数与销量的转化关系分析）
visit_sales = spark.sql("""
    SELECT product_id, product_name,
           visit_count,  -- 访客数（流量指标）
           sales_volume,  -- 销量
           -- 计算访客-销量转化率（销量/访客数），反映流量转化效率
           ROUND(CASE WHEN visit_count = 0 THEN 0 ELSE sales_volume / visit_count END, 4) AS visit_sales_rate,
           'visit-sales' AS analysis_dimension  -- 标记为访客-销量维度
    FROM product_data
    ORDER BY visit_sales_rate DESC, visit_count DESC
""")  # 按转化率和访客数排序，聚焦高转化、高流量的核心商品
print("访客-销量=====================>")
visit_sales.show()
# 单品竞争力诊断
competitor_analysis = spark.sql("""
    SELECT a.product_id, a.product_name,
           a.visit_count / b.avg_visit AS traffic_index,
           a.conversion_rate / b.avg_conversion AS conversion_index,
           a.content_engagement / b.avg_content AS content_index,
           a.new_user_count / b.avg_new_user AS new_user_index,
           a.service_rating / b.avg_service AS service_index
    FROM product_data a
    JOIN (SELECT category,
                 AVG(visit_count) AS avg_visit,
                 AVG(conversion_rate) AS avg_conversion,
                 AVG(content_engagement) AS avg_content,
                 AVG(new_user_count) AS avg_new_user,
                 AVG(service_rating) AS avg_service
          FROM product_data
          GROUP BY category) b
    ON a.category = b.category
""")
print("单品竞争力诊断=====================>")
competitor_analysis.show()

# 单品竞争力构成（加权计算）
competitor_constitute = competitor_analysis.withColumn(
    "traffic_weight", col("traffic_index") * 0.35
).withColumn(
    "conversion_weight", col("conversion_index") * 0.3
).withColumn(
    "content_weight", col("content_index") * 0.15
).withColumn(
    "new_user_weight", col("new_user_index") * 0.1
).withColumn(
    "service_weight", col("service_index") * 0.1
).withColumn(
    "total_score",
    col("traffic_weight") + col("conversion_weight") +
    col("content_weight") + col("new_user_weight") + col("service_weight")
)

# 结果输出
score_amount.write.mode("overwrite").saveAsTable("ads_score_amount")
price_sales.write.mode("overwrite").saveAsTable("ads_price_sales")
visit_sales.write.mode("overwrite").saveAsTable("ads_visit_sales")
competitor_constitute.write.mode("overwrite").saveAsTable("ads_competitor_constitute")

spark.stop()