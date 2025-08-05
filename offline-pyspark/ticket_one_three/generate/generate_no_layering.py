from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# 初始化SparkSession，启用Hive支持
spark = SparkSession.builder \
    .appName("商品主题商品排行看板") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://192.168.200.101:9083") \
    .getOrCreate()

# 工单编号：大数据-电商数仓-02-商品主题商品排行看板
# 时间参数配置（支持7天/30天/日/周/月，此处以30天为例）
stat_date = "2025-01-31"
time_dimensions = {"7天": 7, "30天": 30, "月": 31}  # 2025年1月共31天
target_days = time_dimensions["30天"]
start_date = (spark.sql(f"select date_sub('{stat_date}', {target_days})").collect()[0][0]).strftime("%Y-%m-%d")


# 读取ODS层销售数据并清洗异常值
dwd_sales = spark.table("ods_db.ods_product_sales_data") \
    .filter((col("sales_amount") >= 0) & (col("sales_volume") >= 0)) \
    .withColumn("cleaned_sales_amount", col("sales_amount")) \
    .withColumn("cleaned_sales_volume", col("sales_volume")) \
    .select("shop_id", "product_id", "sku_id", "stat_time",
            "cleaned_sales_amount", "cleaned_sales_volume",
            "pay_buyer_num", "product_visitor_num")

# 写入Hive DWD层表
dwd_sales.write.mode("overwrite") \
    .saveAsTable("ods_db.dwd_product_sales_cleaned")
print("DWD层表 dwd_product_sales_cleaned 写入完成")


# 关联商品基础信息，按日汇总
base_df = spark.table("ods_db.ods_sku_base_info")
dws_daily = spark.table("ods_db.dwd_product_sales_cleaned") \
    .join(base_df.select("product_id", "category_id", "category_name"), on="product_id", how="left") \
    .groupBy("shop_id", "product_id", "category_id", "category_name", "stat_time") \
    .agg(
    sum("cleaned_sales_amount").alias("daily_sales_amount"),
    sum("cleaned_sales_volume").alias("daily_sales_volume"),
    sum("pay_buyer_num").alias("daily_pay_buyer"),
    sum("product_visitor_num").alias("daily_visitor")
)

# 写入Hive DWS层表
dws_daily.write.mode("overwrite") \
    .saveAsTable("ods_db.dws_product_sales_daily")
print("DWS层表 dws_product_sales_daily 写入完成")


# 计算30天商品排行（按销售额/销量，支持分类）
ads_product_rank = spark.table("ods_db.dws_product_sales_daily") \
    .filter(col("stat_time").between(start_date, stat_date)) \
    .groupBy("shop_id", "product_id", "category_id", "category_name") \
    .agg(
    sum("daily_sales_amount").alias("total_sales_amount"),
    sum("daily_sales_volume").alias("total_sales_volume"),
    sum("daily_pay_buyer").alias("total_pay_buyer"),
    sum("daily_visitor").alias("total_visitor")
) \
    .withColumn("pay_conversion_rate",  # 支付转化率（文档定义：支付买家数/商品访客数）
                round(col("total_pay_buyer") / col("total_visitor"), 4)) \
    .withColumn("sales_rank",  # 全量商品销售额排行
                row_number().over(Window.orderBy(col("total_sales_amount").desc()))) \
    .withColumn("category_sales_rank",  # 分类内销售额排行
                row_number().over(Window.partitionBy("category_id")
                                  .orderBy(col("total_sales_amount").desc())))

# 写入Hive ADS层表（供看板使用）
ads_product_rank.write.mode("overwrite") \
    .saveAsTable("ods_db.ads_product_ranking")
print("ADS层表 ads_product_ranking 写入完成")


# 1. 价格力商品概览（分层统计）
price_strength_overview = spark.table("ods_db.ods_price_strength_data") \
    .filter(col("stat_time") == stat_date) \
    .groupBy("price_strength_star") \
    .agg(countDistinct("product_id").alias("product_count")) \
    .withColumn("time_type", lit(f"{target_days}天"))

# 写入Hive
price_strength_overview.write.mode("overwrite") \
    .saveAsTable("ods_db.ads_price_strength_overview")

# 2. 价格力预警商品
price_warning = spark.table("ods_db.ods_price_strength_data") \
    .filter((col("stat_time") == stat_date) &
            ((col("price_strength_warning") == "Y") |
             (col("product_strength_warning") == "Y"))) \
    .select("shop_id", "product_id",
            col("price_strength_warning").alias("price_warn"),
            col("product_strength_warning").alias("product_warn"),
            col("price_strength_star"),
            col("coupon_after_price"))

# 写入Hive
price_warning.write.mode("overwrite") \
    .saveAsTable("ods_db.ads_price_strength_warning")
print("价格力指标表写入完成")


# 1. Top10流量来源（按访客数）
traffic_top10 = spark.table("ods_db.ods_traffic_source_data") \
    .filter(col("stat_time").between(start_date, stat_date)) \
    .groupBy("traffic_source") \
    .agg(
    sum("visitor_num_from_source").alias("total_visitor"),
    avg(regexp_extract(col("pay_conversion_rate_from_source"), r"(\d+\.\d+)", 1).cast("double"))
        .alias("avg_conversion_rate")
) \
    .orderBy(col("total_visitor").desc()) \
    .limit(10)

# 写入Hive
traffic_top10.write.mode("overwrite") \
    .saveAsTable("ods_db.ads_traffic_source_top10")

# 2. Top10搜索词（按访客数）
search_word_top10 = spark.table("ods_db.ods_search_word_data") \
    .filter(col("stat_time").between(start_date, stat_date)) \
    .groupBy("search_word") \
    .agg(sum("search_visitor_num").alias("total_search_visitor")) \
    .orderBy(col("total_search_visitor").desc()) \
    .limit(10)

# 写入Hive
search_word_top10.write.mode("overwrite") \
    .saveAsTable("ods_db.ads_search_word_top10")
print("流量与搜索词指标表写入完成")

