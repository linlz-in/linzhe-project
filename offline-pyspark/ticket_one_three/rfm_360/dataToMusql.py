import pandas as pd
from datetime import datetime, timedelta
import random

# 基础配置参数
task_id = "商品主题商品360看板"  # 工单编号（验收标准要求）
data_size = 2000  # 模拟2000条核心数据
product_count = 100  # 商品数量 用于生成基础商品信息
user_count = 500  # 用户数量 用于模拟用户消费行为
category_list = ["服饰", "家电", "美妆", "食品", "数码"]  # 商品类目
channel_list = ["搜索", "手淘推荐", "直播", "短视频", "直通车"]  # 流量渠道


# 数据生成核心函数(任务描述所有维度)
def generate_original_data():
    """生成原始数据 模拟ODS层
        products: 商品基础信息表（含SKU、价格等属性）
        orders: 订单明细表（含用户行为、支付状态等）
        reviews: 商品评价表（含评分、评价内容）
        keywords: 商品关键词表（含词根引流数据）
    """
    # 商品基础数据（SKU分析/价格分析）
    products = pd.DataFrame({
        "product_id": [f"P{str(i).zfill(4)}" for i in range(1, product_count + 1)], # 商品id
        "category": [random.choice(category_list) for _ in range(product_count)], # 商品类目
        "base_price": [round(random.uniform(50, 2000), 2) for _ in range(product_count)],  # 价格
        "sku_attr": [f"颜色:{random.choice(['红', '黑', '蓝'])};尺寸:{random.choice(['S', 'M', 'L'])}" for _ in
                     range(product_count)], # sku描述
        "price_force_star": [random.randint(1, 5) for _ in range(product_count)]  # 价格力星级
    })

    # 订单和用户行为数据（流量、RFM模型基础数据）
    orders = pd.DataFrame({
        "order_id": [f"O{str(i).zfill(5)}" for i in range(1, data_size + 1)],  # 订单id
        "user_id": [f"U{str(random.randint(1, user_count)).zfill(4)}" for _ in range(data_size)],  # 下单用户ID
        "product_id": [random.choice(products["product_id"].tolist()) for _ in range(data_size)],  # 关联的商品ID
        "order_time": [datetime.now() - timedelta(days=random.randint(1, 180)) for _ in range(data_size)],  # 下单时间（6个月）
        "amount": [round(random.uniform(100, 5000), 2) for _ in range(data_size)],   # 订单金额
        "channel": [random.choice(channel_list) for _ in range(data_size)],  # 流量来源渠道
        "pay_status": [random.choice([0, 1]) for _ in range(data_size)]  # 0未支付，1已支付
    })

    # 商品评价数据（服务体验维度）
    paid_orders = orders[orders["pay_status"] == 1]["order_id"].tolist()  # 仅为已支付订单生成评价
    reviews = pd.DataFrame({
        "review_id": [f"R{str(i).zfill(5)}" for i in range(1, len(paid_orders) + 1)],  # 评价
        "order_id": paid_orders,   # 关联的订单ID
        "score": [random.randint(1, 5) for _ in range(len(paid_orders))],  # 评价分数
        "review_content": [random.choice(["好评", "差评", "物流快", "质量差", "推荐"]) for _ in
                           range(len(paid_orders))],  # 评价内容
        "review_time": [datetime.now() - timedelta(days=random.randint(1, 30)) for _ in range(len(paid_orders))]   # 评价时间
    })

    # 关键词与标题数据（标题优化维度）
    keywords = pd.DataFrame({
        "product_id": products["product_id"].tolist(),   # 关联的商品ID
        "title_root": [f"{random.choice(['新款', '正品'])}+{random.choice(['短袖', '手机', '口红'])}" for _ in
                       range(product_count)],    # 标题词根
        "root_uv": [random.randint(100, 1000) for _ in range(product_count)],  # 词根引流人数
        "root_cvr": [round(random.uniform(0.01, 0.2), 3) for _ in range(product_count)]  # 词根转化率
    })

    return {
        "products": products,
        "orders": orders,
        "reviews": reviews,
        "keywords": keywords
    }


# 2. 分层实现（ODS->DWD->DWS->ADS）
def layered_implementation(original_data):
    """分层实现"""
    # ODS层
    ods_products = original_data["products"].copy()  # 商品原始数据
    ods_orders = original_data["orders"].copy()      # 订单原始数据
    ods_reviews = original_data["reviews"].copy()    # 评价原始数据
    ods_keywords = original_data["keywords"].copy()  # 关键词原始数据

    # DWD层
    dwd_orders = ods_orders.merge(ods_products[["product_id", "category", "base_price"]],
                                  on="product_id", how="left") # 关联商品基础信息
    dwd_orders["order_date"] = dwd_orders["order_time"].dt.date  # 提取日期
    dwd_orders = dwd_orders[dwd_orders["pay_status"] == 1]  # 只保留已支付订单

    # DWS层
    # 商品销售指标
    dws_product_sales = dwd_orders.groupby("product_id").agg({
        "order_id": "nunique",  # 订单数  去重
        "amount": "sum",  # 销售额 求和
        "user_id": "nunique",  # 购买用户数  去重
        "channel": lambda x: x.value_counts().to_dict()  # 渠道分布  各渠道订单的占比
    }).rename(columns={
        "order_id": "order_count",
        "amount": "total_sales",
        "user_id": "buyer_count",
        "channel": "channel_distribution"
    })

    # RFM模型计算
    today = datetime.now().date()
    dws_rfm = dwd_orders.groupby("user_id").agg({
        "order_date": lambda x: (today - x.max()).days,  # R：最近消费天数 （越小价值越高）
        "order_id": "nunique",  # F：消费频率 （购买次数）
        "amount": "sum"  # M：消费金额 （总消费额）
    }).rename(columns={
        "order_date": "recency",
        "order_id": "frequency",
        "amount": "monetary"
    })

    # ADS层
    ads_product_360 = dws_product_sales.merge(
        original_data["products"][["product_id", "sku_attr", "price_force_star"]],   # 关联SKU和价格力信息
        on="product_id", how="left"
    ).merge(
        original_data["keywords"][["product_id", "title_root", "root_uv"]], # 关联关键词引流信息
        on="product_id", how="left"
    )

    return {
        "ods": {"products": ods_products, "orders": ods_orders},
        "dws": {"sales": dws_product_sales, "rfm": dws_rfm},
        "ads": ads_product_360  # 最终看板数据
    }


# 3. 不分层实现
def non_layered_implementation(original_data):
    # 筛选有效订单（已支付） 并关联商品信息
    orders = original_data["orders"][original_data["orders"]["pay_status"] == 1].copy()
    orders = orders.merge(original_data["products"], on="product_id", how="left")

    # 直接计算核心指标
    product_metrics = orders.groupby("product_id").agg({
        "order_id": "nunique",  # 订单数量
        "amount": "sum",  # 总销售额
        "user_id": "nunique", # 购买用户数
        "sku_attr": lambda x: x.value_counts().index[0],  # 热销SKU
        "base_price": "mean", # 平均价格
        "channel": lambda x: x.value_counts().to_dict()   # 渠道分布数据
    }).rename(columns={
        "order_id": "order_count",
        "amount": "total_sales",
        "user_id": "buyer_count",
        "sku_attr": "hot_sku",
        "base_price": "avg_price",
        "channel": "channel_data"
    })

    # RFM计算（与分层结果一致）
    today = datetime.now().date()
    rfm = orders.groupby("user_id").agg({
        "order_time": lambda x: (today - x.max().date()).days,  # R:最近消费天数
        "order_id": "nunique",  # F:消费频率
        "amount": "sum"  # M:消费金额
    }).rename(columns={
        "order_time": "recency",
        "order_id": "frequency",
        "amount": "monetary"
    })

    return {
        "product_metrics": product_metrics,  # 商品核心指标
        "rfm": rfm  # RFM模型结果
    }


# 生成并验证数据
if __name__ == "__main__":
    # 生成原始数据
    original_data = generate_original_data()
    print(f"=== 原始数据生成完成===")
    print(f"商品数据量：{len(original_data['products'])}条")
    print(f"订单数据量：{len(original_data['orders'])}条（目标2000条）\n")

    # 分层实现
    layered_data = layered_implementation(original_data)
    print(f"=== 分层实现结果===")
    print(f"ADS层商品指标示例：\n{layered_data['ads'].head(2)}\n")

    # 不分层实现
    non_layered_data = non_layered_implementation(original_data)
    print(f"=== 不分层实现结果===")
    print(f"指标示例：\n{non_layered_data['product_metrics'].head(2)}\n")

    # 验证RFM一致性
    rfm_layered = layered_data["dws"]["rfm"].reset_index()
    rfm_non_layered = non_layered_data["rfm"].reset_index()
    rfm_merge = rfm_layered.merge(rfm_non_layered, on="user_id", suffixes=("_layered", "_nonlayered"))
    print(f"RFM数据一致性验证前3条：\n{rfm_merge[['user_id', 'recency_layered', 'recency_nonlayered']].head(3)}")

    # 保存数据
    layered_data["ads"].to_csv(f"{task_id}_分层_ADS数据.csv", index=False)
    non_layered_data["product_metrics"].to_csv(f"{task_id}_不分层_指标数据.csv", index=False)
    print(f"\n数据已保存，工单编号：{task_id}")