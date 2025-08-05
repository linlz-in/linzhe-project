from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# 初始化SparkSession，启用Hive支持
spark = SparkSession.builder.appName("GenerateODSData") \
    .master("local[*]") \
    .enableHiveSupport() \
    .config("spark.sql.version", "3.2") \
    .config("hive.metastore.uris", "thrift://192.168.200.101:9083") \
    .getOrCreate()

# 设置随机种子，确保结果可复现
random.seed(42)

# 生成日期范围（2025年1月）
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 1, 31)
date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
date_strings = [d.strftime("%Y-%m-%d") for d in date_range]

# 定义基础数据
shops = [1001, 1002, 1003, 1004, 1005]  # 5个店铺
# 分类
categories = {
    201: "糕点零食",
    202: "水果生鲜",
    301: "女装",
    302: "男装",
    401: "手机",
    402: "电脑",
    405: "数码配件"
}
# 流量来源类型
traffic_sources = [
    "手淘搜索", "京东搜索", "效果广告", "内容广告", "站外广告",
    "直播带货", "推荐流量", "社交分享", "直接访问", "其他"
]
# 搜索词
search_words = {
    201: ["蛋黄酥", "网红零食", "糕点礼盒", "早餐点心", "休闲食品"],
    202: ["新鲜水果", "进口水果", "有机蔬菜", "生鲜礼盒", "当季水果"],
    301: ["女装新款", "连衣裙", "牛仔裤", "T恤", "毛衣"],
    302: ["男装新款", "纯棉T恤", "休闲裤", "夹克", "衬衫"],
    401: ["智能手机", "5G手机", "拍照手机", "游戏手机", "手机新款"],
    402: ["笔记本电脑", "轻薄本", "游戏本", "商务本", "台式机"],
    405: ["蓝牙耳机", "充电器", "手机壳", "数据线", "移动电源"]
}
# 商品名称
product_names = {
    201: ["轩妈家蛋黄酥", "三只松鼠坚果", "良品铺子礼盒", "徐福记糖果", "达利园面包"],
    202: ["智利车厘子", "海南芒果", "山东苹果", "有机蔬菜礼盒", "进口香蕉"],
    301: ["韩版连衣裙", "修身牛仔裤", "纯棉T恤", "针织毛衣", "时尚外套"],
    302: ["商务衬衫", "休闲牛仔裤", "纯棉T恤", "夹克外套", "运动套装"],
    401: ["苹果iPhone", "华为Mate", "小米手机", "OPPO手机", "vivo手机"],
    402: ["MacBook Pro", "联想小新", "戴尔XPS", "华为MateBook", "华硕天选"],
    405: ["AirPods", "华为耳机", "快充充电器", "防摔手机壳", "大容量移动电源"]
}
colors = ["白色", "黑色", "红色", "蓝色", "绿色", "黄色", "灰色", "粉色", "紫色", "橙色"]


def create_hive_tables():
    """创建Hive表（如果不存在）"""
    # 创建数据库
    # spark.sql("create database if not exists ods_db")
    spark.sql("use ods_db")

    # 1. 商品基础信息表
    spark.sql("""
    create table if not exists ods_sku_base_info(
        shop_id int comment '店铺 ID',
        product_id int comment '商品 ID',
        sku_id string comment 'SKU ID',
        product_name string comment '商品名称',
        category_id int comment '商品分类 ID',
        category_name string comment '商品分类名称',
        color_type string comment '颜色分类'
    ) comment '商品基础信息表'
    stored as parquet
    """)

    # 2. 商品销售数据表
    spark.sql("""
    create table if not exists ods_product_sales_data(
        shop_id int comment '店铺 ID',
        product_id int comment '商品 ID',
        sku_id string comment 'SKU ID',
        stat_time string comment '统计时间',
        sales_amount double comment '销售额',
        sales_volume int comment '销量',
        pay_buyer_num int comment '支付买家数',
        product_visitor_num int comment '商品访客数（访问详情页人数',
        pay_pieces int comment '支付件数',
        pay_pieces_ratio string comment '支付件数占比'
    ) comment '商品销售数据表'
    stored as parquet
    """)

    # 3. 流量来源数据表
    spark.sql("""
    create table if not exists ods_traffic_source_data(
        shop_id int comment '店铺 ID',
        product_id int comment '商品 ID',
        stat_time string comment '统计时间',
        traffic_source string comment '流量来源类型',
        visitor_num_from_source int comment '来源访客数',
        pay_conversion_rate_from_source string comment '来源支付转化率'
    ) comment '流量来源数据表'
    stored as parquet
    """)

    # 4. 库存数据表
    spark.sql("""
    create table if not exists ods_inventory_data(
        shop_id int comment '店铺 ID',
        product_id int comment '商品 ID',
        sku_id string comment 'SKU ID',
        stat_time string comment '统计时间',
        current_stock int comment '当前库存（件）',
        stock_available_days int comment '库存可售天数'
    ) comment '库存数据表'
    stored as parquet
    """)

    # 5. 搜索词数据表
    spark.sql("""
    create table if not exists ods_search_word_data(
        shop_id int comment '店铺 ID',
        product_id int comment '商品 ID',
        stat_time string comment '统计时间',
        search_word string comment '搜索词',
        search_visitor_num int comment '搜索词带来的访客数'
    ) comment '搜索词数据表'
    stored as parquet
    """)

    # 6. 价格力商品数据表
    spark.sql("""
    create table if not exists ods_price_strength_data(
        shop_id int comment '店铺 ID',
        product_id int comment '商品 ID',
        stat_time string comment '统计时间',
        price_strength_star string comment '价格力星级',
        coupon_after_price double comment '普惠券后价',
        price_strength_warning string comment '价格力预警标识',
        product_strength_warning string comment '商品力预警标识',
        product_strength_core_index int comment '商品力核心指标'
    ) comment '价格力商品数据表'
    stored as parquet
    """)

    print("Hive表创建完成")


def generate_sku_base_info():
    """生成商品基础信息表数据"""
    base_info_data = []

    for i in range(200):
        shop_id = random.choice(shops)
        product_id = 10000 + i + 1

        # 随机选择分类
        category_id = random.choice(list(categories.keys()))
        category_name = categories[category_id]

        # 根据分类选择商品名称
        product_name = random.choice(product_names[category_id])

        # 每个商品生成1-3个SKU
        num_skus = random.randint(1, 3)
        for sku_idx in range(num_skus):
            sku_id = f"{product_id}_{sku_idx + 1:02d}"
            color_type = random.choice(colors)

            base_info_data.append((
                shop_id, product_id, sku_id, product_name,
                category_id, category_name, color_type
            ))

    # 定义Schema
    schema = StructType([
        StructField("shop_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("sku_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("category_name", StringType(), True),
        StructField("color_type", StringType(), True)
    ])

    # 创建DataFrame并限制约200条数据
    df = spark.createDataFrame(base_info_data, schema)
    if df.count() > 200:
        df = df.sample(False, 200 / df.count(), seed=42)

    return df


def generate_product_sales_data(base_df):
    """生成商品销售数据表数据"""
    sales_data = []
    base_list = [row.asDict() for row in base_df.collect()]

    for item in base_list:
        # 随机选择3-5天
        num_days = random.randint(3, 5)
        selected_dates = random.sample(date_strings, num_days)

        for date in selected_dates:
            # 生成合理的销售数据
            sales_volume = random.randint(10, 500)
            # 价格根据分类有所不同
            if item['category_id'] in [401, 402]:  # 手机和电脑价格较高
                price_per_unit = random.uniform(1000, 10000)
            elif item['category_id'] in [405, 301, 302]:  # 数码配件和服装
                price_per_unit = random.uniform(50, 1000)
            else:  # 食品类
                price_per_unit = random.uniform(10, 200)

            sales_amount = round(sales_volume * price_per_unit, 2)
            # 支付买家数略少于销量
            pay_buyer_num = random.randint(max(1, int(sales_volume * 0.5)), sales_volume)
            # 访客数应多于支付买家数
            product_visitor_num = random.randint(pay_buyer_num, pay_buyer_num * 10)
            # 支付件数略多于销量
            pay_pieces = random.randint(sales_volume, sales_volume + int(sales_volume * 0.2))
            # 计算支付件数占比
            pay_pieces_ratio = f"{round(random.uniform(0.3, 1.0), 2) * 100}%"

            sales_data.append((
                item['shop_id'], item['product_id'], item['sku_id'], date,
                sales_amount, sales_volume, pay_buyer_num, product_visitor_num,
                pay_pieces, pay_pieces_ratio
            ))

    # 定义Schema
    schema = StructType([
        StructField("shop_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("sku_id", StringType(), True),
        StructField("stat_time", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("sales_volume", IntegerType(), True),
        StructField("pay_buyer_num", IntegerType(), True),
        StructField("product_visitor_num", IntegerType(), True),
        StructField("pay_pieces", IntegerType(), True),
        StructField("pay_pieces_ratio", StringType(), True)
    ])

    # 创建DataFrame并限制约500条数据
    df = spark.createDataFrame(sales_data, schema)
    if df.count() > 500:
        df = df.sample(False, 500 / df.count(), seed=42)

    return df


def generate_traffic_source_data(base_df):
    """生成流量来源数据表数据"""
    traffic_data = []
    # 获取唯一的商品和店铺组合
    product_shop = base_df.select("shop_id", "product_id", "category_id").dropDuplicates()
    ps_list = [row.asDict() for row in product_shop.collect()]

    for item in ps_list:
        # 为每个商品选择3-5个流量来源
        num_sources = random.randint(3, 5)
        selected_sources = random.sample(traffic_sources, num_sources)
        # 随机选择2-3天
        num_days = random.randint(2, 3)
        selected_dates = random.sample(date_strings, num_days)

        for date in selected_dates:
            for source in selected_sources:
                visitor_num = random.randint(10, 500)
                # 转化率与来源类型相关
                if "搜索" in source:
                    conversion_rate = f"{round(random.uniform(0.1, 0.3), 4) * 100}%"
                else:
                    conversion_rate = f"{round(random.uniform(0.01, 0.15), 4) * 100}%"

                traffic_data.append((
                    item['shop_id'], item['product_id'], date, source,
                    visitor_num, conversion_rate
                ))

    # 定义Schema
    schema = StructType([
        StructField("shop_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("stat_time", StringType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("visitor_num_from_source", IntegerType(), True),
        StructField("pay_conversion_rate_from_source", StringType(), True)
    ])

    # 创建DataFrame并限制约400条数据
    df = spark.createDataFrame(traffic_data, schema)
    if df.count() > 400:
        df = df.sample(False, 400 / df.count(), seed=42)

    return df


def generate_inventory_data(base_df):
    """生成库存数据表数据"""
    inventory_data = []
    base_list = [row.asDict() for row in base_df.collect()]

    for item in base_list:
        # 随机选择2-4天
        num_days = random.randint(2, 4)
        selected_dates = random.sample(date_strings, num_days)

        for date in selected_dates:
            # 根据商品类别设置合理的库存范围
            if item['category_id'] in [401, 402]:  # 手机和电脑库存较少
                current_stock = random.randint(5, 50)
            else:
                current_stock = random.randint(20, 500)

            # 估计可售天数
            stock_available_days = random.randint(1, 100)

            inventory_data.append((
                item['shop_id'], item['product_id'], item['sku_id'], date,
                current_stock, stock_available_days
            ))

    # 定义Schema
    schema = StructType([
        StructField("shop_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("sku_id", StringType(), True),
        StructField("stat_time", StringType(), True),
        StructField("current_stock", IntegerType(), True),
        StructField("stock_available_days", IntegerType(), True)
    ])

    # 创建DataFrame并限制约300条数据
    df = spark.createDataFrame(inventory_data, schema)
    if df.count() > 300:
        df = df.sample(False, 300 / df.count(), seed=42)

    return df


def generate_search_word_data(base_df):
    """生成搜索词数据表数据"""
    search_data = []
    # 获取唯一的商品和店铺组合
    product_shop = base_df.select("shop_id", "product_id", "category_id").dropDuplicates()
    ps_list = [row.asDict() for row in product_shop.collect()]

    for item in ps_list:
        # 获取该商品所属分类
        category_id = item['category_id']
        # 随机选择2-4个相关搜索词
        num_words = random.randint(2, 4)
        selected_words = random.sample(search_words[category_id], num_words)
        # 随机选择1-3天
        num_days = random.randint(1, 3)
        selected_dates = random.sample(date_strings, num_days)

        for date in selected_dates:
            for word in selected_words:
                search_visitor_num = random.randint(10, 300)
                search_data.append((
                    item['shop_id'], item['product_id'], date, word, search_visitor_num
                ))

    # 定义Schema
    schema = StructType([
        StructField("shop_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("stat_time", StringType(), True),
        StructField("search_word", StringType(), True),
        StructField("search_visitor_num", IntegerType(), True)
    ])

    # 创建DataFrame并限制约300条数据
    df = spark.createDataFrame(search_data, schema)
    if df.count() > 300:
        df = df.sample(False, 300 / df.count(), seed=42)

    return df


def generate_price_strength_data(base_df):
    """生成价格力商品数据表数据"""
    price_data = []
    # 获取唯一的商品和店铺组合
    product_shop = base_df.select("shop_id", "product_id", "category_id").dropDuplicates()
    ps_list = [row.asDict() for row in product_shop.collect()]

    for item in ps_list:
        # 随机选择2-4天
        num_days = random.randint(2, 4)
        selected_dates = random.sample(date_strings, num_days)

        for date in selected_dates:
            # 生成商品力核心指标（0-100）
            core_index = random.randint(30, 100)

            # 根据核心指标确定价格力星级
            if core_index >= 80:
                price_strength_star = "优秀"
            elif core_index >= 60:
                price_strength_star = "良好"
            else:
                price_strength_star = "较差"

            # 根据分类设置合理价格
            category_id = item['category_id']
            if category_id in [401, 402]:  # 手机和电脑价格较高
                coupon_after_price = round(random.uniform(1000, 10000), 2)
            elif category_id in [405, 301, 302]:  # 数码配件和服装
                coupon_after_price = round(random.uniform(50, 1000), 2)
            else:  # 食品类
                coupon_after_price = round(random.uniform(10, 200), 2)

            # 确定预警标识
            price_strength_warning = "Y" if (price_strength_star == "较差" and random.random() < 0.7) or (
                    price_strength_star != "较差" and random.random() < 0.1) else "N"
            product_strength_warning = "Y" if (core_index < 60 and random.random() < 0.6) or (
                    core_index >= 60 and random.random() < 0.2) else "N"

            price_data.append((
                item['shop_id'], item['product_id'], date, price_strength_star,
                coupon_after_price, price_strength_warning,
                product_strength_warning, core_index
            ))

    # 定义Schema
    schema = StructType([
        StructField("shop_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("stat_time", StringType(), True),
        StructField("price_strength_star", StringType(), True),
        StructField("coupon_after_price", DoubleType(), True),
        StructField("price_strength_warning", StringType(), True),
        StructField("product_strength_warning", StringType(), True),
        StructField("product_strength_core_index", IntegerType(), True)
    ])

    # 创建DataFrame并限制约300条数据
    df = spark.createDataFrame(price_data, schema)
    if df.count() > 300:
        df = df.sample(False, 300 / df.count(), seed=42)

    return df


def main():
    try:
        # 创建Hive表
        create_hive_tables()

        # 生成各表数据
        print("生成商品基础信息表数据...")
        base_df = generate_sku_base_info()
        print(f"商品基础信息表: {base_df.count()}条数据")

        print("生成商品销售数据表数据...")
        sales_df = generate_product_sales_data(base_df)
        print(f"商品销售数据表: {sales_df.count()}条数据")

        print("生成流量来源数据表数据...")
        traffic_df = generate_traffic_source_data(base_df)
        print(f"流量来源数据表: {traffic_df.count()}条数据")

        print("生成库存数据表数据...")
        inventory_df = generate_inventory_data(base_df)
        print(f"库存数据表: {inventory_df.count()}条数据")

        print("生成搜索词数据表数据...")
        search_df = generate_search_word_data(base_df)
        print(f"搜索词数据表: {search_df.count()}条数据")

        print("生成价格力商品数据表数据...")
        price_df = generate_price_strength_data(base_df)
        print(f"价格力商品数据表: {price_df.count()}条数据")

        # 写入Hive表
        base_df.write.mode("overwrite").insertInto("ods_sku_base_info")
        sales_df.write.mode("overwrite").insertInto("ods_product_sales_data")
        traffic_df.write.mode("overwrite").insertInto("ods_traffic_source_data")
        inventory_df.write.mode("overwrite").insertInto("ods_inventory_data")
        search_df.write.mode("overwrite").insertInto("ods_search_word_data")
        price_df.write.mode("overwrite").insertInto("ods_price_strength_data")

        print("所有数据已成功写入Hive表")

        # 验证数据总数
        total = base_df.count() + sales_df.count() + traffic_df.count() + \
                inventory_df.count() + search_df.count() + price_df.count()
        print(f"总数据量: {total}条")

    except Exception as e:
        print(f"操作出错: {str(e)}")
    finally:
        # 停止SparkSession
        spark.stop()
        print("SparkSession已停止")


if __name__ == "__main__":
    main()
