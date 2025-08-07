import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from hdfs import InsecureClient
import os
import requests
import sys
import tempfile  # 用于创建本地临时文件，解决HDFS流写入兼容性问题

# 环境与配置参数定义
# CDH集群WebHDFS配置（适配hdfs 2.7.3版本）
CDH_NAMENODE_HOST = "192.168.200.101"  # NameNode节点实际IP地址，需根据实际集群修改
CDH_WEBHDFS_PORT = 9870  # Hadoop 3.x默认WebHDFS端口为9870，Hadoop 2.x为50070
HDFS_USER = "hdfs"  # 具有HDFS写入权限的用户（通常为hdfs管理员用户）
HDFS_URL = f"http://{CDH_NAMENODE_HOST}:{CDH_WEBHDFS_PORT}"  # WebHDFS访问地址
LOCAL_DEBUG = False  # 本地调试开关：True-数据保存到本地；False-写入HDFS
ENCODING = "utf-8"  # 统一编码格式为UTF-8，确保中文等特殊字符正常处理


# HDFS连接测试函数
def test_hdfs_connection():
    """
    测试与CDH集群WebHDFS的连接是否正常
    通过调用WebHDFS的GETFILESTATUS操作检查根目录状态
    返回值：True-连接成功；False-连接失败
    """
    try:
        # 构造WebHDFS根目录状态检查请求
        test_url = f"{HDFS_URL}/webhdfs/v1/"
        params = {
            "op": "GETFILESTATUS",  # WebHDFS标准操作：获取文件状态
            "user.name": HDFS_USER  # 指定HDFS操作用户
        }

        # 发送请求并允许自动重定向（旧版本hdfs库依赖此机制）
        response = requests.get(
            test_url,
            params=params,
            timeout=10,  # 10秒超时设置，避免无限等待
            allow_redirects=True
        )

        # 状态码200表示连接成功
        if response.status_code == 200:
            print("✅ HDFS连接测试成功")
            return True
        else:
            print(f"❌ HDFS连接测试失败，状态码：{response.status_code}")
            print(f"响应内容：{response.text}")  # 打印错误详情便于排查
            return False
    except Exception as e:
        print(f"❌ HDFS连接测试出错：{str(e)}")
        return False


# 1. 商品基础信息表生成与存储
# 叶子类目定义（包含多级分类，用于模拟电商实际商品分类体系）
leaf_categories = [
    "女装-连衣裙", "女装-T恤", "男装-牛仔裤", "男装-衬衫",
    "数码-手机", "数码-耳机", "家居-床单", "家居-枕头"
]

# 生成200个商品的基础信息数据
products = []
for i in range(200):
    # 随机选择商品类目
    category = random.choice(leaf_categories)
    # 生成随机价格（范围：39.9~4999.9元，保留2位小数）
    price = round(random.uniform(39.9, 4999.9), 2)
    # 生成商品创建时间（2025年7月1日至7月31日之间随机日期）
    create_time = (datetime(2025, 7, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")

    # 组装商品信息字典
    products.append({
        "product_id": f"prod_{i + 1:03d}",  # 商品ID，格式：prod_001, prod_002...
        "leaf_category": category,  # 叶子类目
        "price": price,  # 商品价格
        "create_time": create_time  # 创建时间
    })

# 将商品列表转换为DataFrame，便于后续数据处理和存储
df_products = pd.DataFrame(products)

# 数据存储逻辑
if LOCAL_DEBUG:
    # 本地调试模式：数据保存到本地文件系统
    local_path = 'D:/local_ods/product_base/202507/'
    # 创建本地目录（若不存在），exist_ok=True避免目录已存在时报错
    os.makedirs(local_path, exist_ok=True)
    # 写入Parquet文件（UTF-8编码由pandas自动处理）
    df_products.to_parquet(f'{local_path}/product_base.parquet', index=False)
    print(f"商品基础信息已保存到本地：{local_path}")
else:
    # HDFS模式：先检查HDFS连接是否可用
    if not test_hdfs_connection():
        print("❌ 无法连接HDFS，程序退出")
        sys.exit(1)  # 连接失败则退出程序

    try:
        # 初始化HDFS客户端（兼容旧版本hdfs库，不使用session参数）
        hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

        # HDFS存储路径（符合CDH+Hive规范：ods.db为数据库目录，202507为分区目录）
        hdfs_path = '/user/hive/warehouse/ods.db/product_base/202507/'

        # 创建HDFS目录（若不存在）
        if not hdfs_client.status(hdfs_path, strict=False):
            hdfs_client.makedirs(hdfs_path)
            print(f"已创建HDFS目录：{hdfs_path}")

        # 本地临时文件中转方案：
        # 1. 先将数据写入本地临时文件（解决fastparquet与HDFS流不兼容问题）
        # 2. 再将临时文件上传到HDFS
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
            # 写入本地临时文件（指定fastparquet引擎，确保UTF-8编码）
            df_products.to_parquet(
                temp_file,
                index=False,
                engine='fastparquet'  # 使用fastparquet引擎，避免pyarrow的兼容性问题
            )
            temp_file.flush()  # 强制刷新缓冲区，确保数据写入磁盘

            # 上传临时文件到HDFS（覆盖已有文件）
            hdfs_target = f'{hdfs_path}/product_base.parquet'
            # 若目标文件已存在则先删除
            if hdfs_client.status(hdfs_target, strict=False):
                hdfs_client.delete(hdfs_target)
            # 上传本地临时文件到HDFS目标路径
            hdfs_client.upload(hdfs_target, temp_file.name)
            print(f"商品基础信息已写入CDH HDFS：{hdfs_target}")

        # 删除本地临时文件，清理磁盘空间
        os.unlink(temp_file.name)

    except Exception as e:
        print(f"❌ 商品数据写入CDH HDFS失败：{str(e)}")
        sys.exit(1)  # 写入失败则退出程序

# 2. 用户行为数据表生成与存储
# 生成2025年7月全月日期列表（用于按日期生成用户行为数据）
start_date = datetime(2025, 7, 1)
end_date = datetime(2025, 7, 31)
dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

# 生成用户行为数据
user_actions = []
for date in dates:
    date_str = date.strftime("%Y-%m-%d")  # 日期格式：YYYY-MM-DD
    for prod in products:
        product_id = prod["product_id"]  # 关联商品ID

        # 核心流量指标生成
        visitor_count = random.randint(50, 10000)  # 访客数：50~10000随机
        pv = random.randint(visitor_count, visitor_count * 5)  # 浏览量：不小于访客数
        avg_stay_sec = round(random.uniform(5, 600), 1)  # 平均停留秒数：5~600秒
        bounce_rate = round(random.uniform(0.1, 0.9), 4)  # 跳出率：10%~90%

        # 转化指标生成
        cart_user_count = random.randint(0, int(visitor_count * 0.15))  # 加购人数：最多15%访客
        cart_item_count = random.randint(cart_user_count, int(visitor_count * 0.3))  # 加购件数
        collect_count = random.randint(0, int(visitor_count * 0.2))  # 收藏人数：最多20%访客

        order_user_count = random.randint(0, cart_user_count)  # 下单人数：不超过加购人数
        order_item_count = random.randint(order_user_count, cart_item_count)  # 下单件数
        # 下单金额：基于商品价格，叠加0.9~1.1的随机系数（模拟折扣/溢价）
        order_amount = round(order_item_count * prod["price"] * random.uniform(0.9, 1.1), 2)

        pay_user_count = random.randint(0, order_user_count)  # 支付人数：不超过下单人数
        pay_item_count = random.randint(pay_user_count, order_item_count)  # 支付件数
        # 支付金额：基于商品价格，叠加0.9~1.1的随机系数
        pay_amount = round(pay_item_count * prod["price"] * random.uniform(0.9, 1.1), 2)

        # 新老用户划分
        new_buyer_count = random.randint(0, pay_user_count)  # 新买家数
        old_buyer_count = pay_user_count - new_buyer_count  # 老买家数：支付人数 - 新买家数

        # 组装用户行为数据字典
        user_actions.append({
            "date": date_str,
            "product_id": product_id,
            "leaf_category": prod["leaf_category"],  # 关联商品类目
            "visitor_count": visitor_count,
            "pv": pv,
            "avg_stay_sec": avg_stay_sec,
            "bounce_rate": bounce_rate,
            "collect_count": collect_count,
            "cart_item_count": cart_item_count,
            "cart_user_count": cart_user_count,
            "order_user_count": order_user_count,
            "order_item_count": order_item_count,
            "order_amount": order_amount,
            "pay_user_count": pay_user_count,
            "pay_item_count": pay_item_count,
            "pay_amount": pay_amount,
            "new_buyer_count": new_buyer_count,
            "old_buyer_count": old_buyer_count
        })

# 将用户行为列表转换为DataFrame
df_actions = pd.DataFrame(user_actions)

# 数据存储逻辑
if LOCAL_DEBUG:
    # 本地调试模式：数据保存到本地文件系统
    local_path = 'D:/local_ods/user_action/202507/'
    os.makedirs(local_path, exist_ok=True)
    # 写入Parquet文件（UTF-8编码由pandas自动处理）
    df_actions.to_parquet(f'{local_path}/user_action.parquet', index=False)
    print(f"用户行为数据已保存到本地：{local_path}")
else:
    try:
        # 初始化HDFS客户端
        hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

        # HDFS存储路径（符合CDH+Hive规范）
        hdfs_path = '/user/hive/warehouse/ods.db/user_action/202507/'

        # 创建HDFS目录（若不存在）
        if not hdfs_client.status(hdfs_path, strict=False):
            hdfs_client.makedirs(hdfs_path)
            print(f"已创建HDFS目录：{hdfs_path}")

        # 本地临时文件中转方案（与商品数据处理逻辑一致）
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
            # 写入本地临时文件（指定fastparquet引擎）
            df_actions.to_parquet(
                temp_file,
                index=False,
                engine='fastparquet'
            )
            temp_file.flush()  # 确保数据落盘

            # 上传临时文件到HDFS
            hdfs_target = f'{hdfs_path}/user_action.parquet'
            if hdfs_client.status(hdfs_target, strict=False):
                hdfs_client.delete(hdfs_target)  # 删除旧文件
            hdfs_client.upload(hdfs_target, temp_file.name)  # 上传新文件
            print(f"用户行为数据已写入CDH HDFS：{hdfs_target}")

        # 删除本地临时文件
        os.unlink(temp_file.name)

    except Exception as e:
        print(f"❌ 用户行为数据写入CDH HDFS失败：{str(e)}")
        sys.exit(1)  # 写入失败则退出程序

print("✅ 所有数据处理完成")
