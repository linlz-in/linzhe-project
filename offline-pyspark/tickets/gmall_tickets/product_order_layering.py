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
# 工单编号：大数据-电商数仓-07-商品主题商品诊断看板

# 1. ODS层：读取原始数据设product_data为ods层
# 直接使用已有ods层表product_data，不需要额外处理
print("=====================ODS==================")
spark.sql("select * from product_data limit 10").show()  # 验证ods


print("=====================DWD==================")
# dwd层 数据清洗与转换
# 创建dwd_product_clean 进行增加字段 score_level 对评分进行处理
spark.sql("""
    create table if not exists dwd_product_clean(
        product_id         string,
        product_name       string,
        category           string,
        price              double,
        visit_count        int,
        conversion_rate    double,
        sales_volume       int,
        score              int,
        content_engagement int,
        new_user_count     int,
        service_rating     double,
        data_date          string,
        score_level        string  
    )stored as parquet
""")
# -- stored as parquet 这是一种存储格式
#     -- parquet ： 是列式存储文件格式，适用于大数据处理场景
#     -- 具有压缩效率高、能减少 I/O 操作、支持高效的列裁剪和过滤等特点 有主助于提升效率

# 为这张表添加处理评分的数据
spark.sql("""
    insert overwrite table dwd_product_clean
    select
        *,
        case when score >= 85 then 'A级'
            when score >= 70 then 'B级'
            when score >= 50 then 'C级'
            else 'D级' end as score_level
    from product_data
    where product_id is  not NULL  -- 过滤无效数据
""")



print("=====================DWS==================")
#-- dws层 聚合计算每个维度指标
# 全品价值评估
spark.sql("""
    create table if not exists dws_comprehensive_product (
        product_id         string,
        product_name       string,
        category           string,
        score_level        string,
        total_amount       double,   -- 评分-金额（价格*销量）
        price_sales_ratio  double,   -- 价格-销量（价格/销量）
        visit_sales_ration double    -- 访客-销量（访客数/销量）
    )stored as parquet
""")

spark.sql("""
    insert overwrite table dws_comprehensive_product
    select
        product_id,
        product_name,
        category,
        score_level,
        price * sales_volume as total_amount,
        price / sales_volume as price_sales_ratio,
        visit_count / sales_volume as visit_sales_ratio
    from dwd_product_clean
""")

# 单品竞争力诊断  计算同类商品的平均值
spark.sql("""
    create table if not exists dws_category_avg (
        category        string,
        avg_visit       double,
        avg_conversion  double,
        avg_content     double,
        avg_new_user    double,
        avg_service     double
    )
    stored as parquet
""")

spark.sql("""
    insert overwrite table dws_category_avg
    select
        category,
        avg(visit_count) as avg_visit,
        avg(conversion_rate) as avg_conversion,
        avg(content_engagement) as avg_content,
        avg(new_user_count) as avg_new_user,
        avg(service_rating) as avg_service
    from dwd_product_clean
    group by category
""")



print("=====================ADS==================")
# ads层 看板数据
# 全品价值评估结果
spark.sql("""
    create table if not exists ads_comprehensive_product_overall (
        product_id        string,
        product_name      string,
        category          string,
        score_level       string,
        total_amount      double,
        price_sales_ratio double,
        visit_sales_ratio double,
        value_tag         string  -- 潜力商品标签
    )
    stored as parquet
""")

# 标记核心商品（A级 总金额前20%） 潜力商品（B级 销量增长快）
spark.sql("""
    insert overwrite table ads_comprehensive_product_overall
    select
        a.*,
        case when a.score_level = 'A级' and a.total_amount >= b.p80 then '核心商品'
            when a.score_level = 'B级' and a.visit_sales_ration < b.avg_ratio then '潜力商品'
            else '一般商品' end as value_tag
    from dws_comprehensive_product a
    join (
        select
            percentile(cast(total_amount * 100 as bigint), 0.8) / 100 as p80,
            avg(visit_sales_ration) as avg_ratio
    from dws_comprehensive_product
    ) b
""")

# 4.2 单品竞争力诊断结果表（含五个维度对比）
spark.sql("""
    create table if not exists ads_product_competitor_dimension (
        product_id string,
        product_name string,
        traffic_index double,  -- 流量获取 （本品/同类平均）
        conversion_index double, -- 流量转换
        service_index double, -- 服务质量
        content_index double,  --客户拉新
        new_user_index double  -- 内容营销
    )stored as parquet
""")

spark.sql("""
    insert overwrite table ads_product_competitor_dimension
    select
        a.product_id,
        a.product_name,
        a.visit_count / b.avg_visit as traffic_index,
        a.conversion_rate / b.avg_conversion as conversion_index,
        a.service_rating / b.avg_service as service_index,
        a.content_engagement / b.avg_content as content_index,
        a.new_user_count / b.avg_new_user as new_user_index
    from dwd_product_clean a
    join dws_category_avg b
    ON a.category = b.category
""")

# 4.3 单品竞争力构成表（按权重计算）
spark.sql("""
    create table if not exists ads_product_competitor_detail (
        product_id       string,
        product_name     string,
        traffic_weight    double, -- 流量
        conversion_weight double, -- 转换
        service_weight    double, -- 服务
        new_user_weight    double, -- 拉新
        content_weight    double, -- 内容
        total_score   double -- 总得分
    )stored as parquet
""")

spark.sql("""
    insert overwrite table ads_product_competitor_detail
    select
        product_id,
        product_name,
        traffic_index * 0.35 as traffic_weight,
        conversion_index * 0.3 as conversion_weight,
        service_index * 0.15 as service_weight,
        new_user_index * 0.1 as new_user_weight,
        content_index * 0.1 as content_weight,
        (traffic_index * 0.35 + conversion_index * 0.3 +
        service_index * 0.15 + new_user_index * 0.1 +
        content_index * 0.1) as total_score
    from ads_product_competitor_dimension
""")

print("分层实现已完成，数据已写入ADS层表")
spark.stop()