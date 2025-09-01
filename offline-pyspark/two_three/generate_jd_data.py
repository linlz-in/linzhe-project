# generate_jd_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# 设置随机种子保证结果可重现
np.random.seed(42)
random.seed(42)

print("正在生成模拟京东评论数据...")


# 1. 生成商品信息数据
def generate_product_data(num_products=1000):
    print("生成商品信息数据...")
    products = []
    categories = []

    # 创建一些示例商品名称
    product_names = [
        "苹果iPhone 14 Pro Max", "华为Mate 60", "小米13 Ultra",
        "三星Galaxy S23", "OPPO Find X6", "vivo X90",
        "联想小新笔记本电脑", "戴尔XPS 13", "华硕ROG游戏本",
        "索尼 PlayStation 5", "任天堂Switch", "Xbox Series X",
        "耐克运动鞋", "阿迪达斯跑鞋", "李宁篮球鞋",
        "优衣库T恤", "ZARA连衣裙", "H&M牛仔裤"
    ]

    # 创建类别体系
    first_categories = ["手机数码", "电脑办公", "家用电器", "服装鞋帽", "食品生鲜", "美妆个护",
                        "母婴玩具", "图书音像", "运动户外", "汽车用品"]

    second_categories = {}
    for i, first_cat in enumerate(first_categories):
        if first_cat == "手机数码":
            second_categories[i] = ["手机", "平板电脑", "数码相机", "耳机", "智能手表"]
        elif first_cat == "电脑办公":
            second_categories[i] = ["笔记本电脑", "台式机", "打印机", "办公文具", "网络设备"]
        elif first_cat == "服装鞋帽":
            second_categories[i] = ["男装", "女装", "童装", "鞋类", "箱包"]
        else:
            second_categories[i] = [f"{first_cat}子类{j + 1}" for j in range(5)]

    for i in range(num_products):
        first_cat_idx = random.randint(0, len(first_categories) - 1)
        second_cats = second_categories[first_cat_idx]
        second_cat = random.choice(second_cats)
        third_cat = f"三级类目{random.randint(1, 10)}"

        category_str = f"{first_cat_idx}-{second_cats.index(second_cat)}-{third_cat.split('级类目')[-1]}"

        product_name = random.choice(product_names) + f" {random.randint(1, 1000)}型"

        products.append({
            "商品ID": f"PRODUCT_{i}",
            "商品名称": product_name,
            "所属类别": category_str
        })

        # 收集所有类别
        for first_idx, first_name in enumerate(first_categories):
            if first_idx == first_cat_idx:
                for second_idx, second_name in enumerate(second_categories[first_idx]):
                    categories.append({
                        "类别ID": f"{first_idx}-{second_idx}",
                        "类别名称": f"{first_name}-{second_name}"
                    })

    # 去重类别
    categories_df = pd.DataFrame(categories).drop_duplicates()
    return pd.DataFrame(products), categories_df


# 2. 生成用户评论数据
def generate_review_data(num_users=5000, num_reviews=20000):
    print("生成用户评论数据...")

    # 生成用户ID
    user_ids = list(range(num_users))

    # 生成时间范围：2011年1月1日到2014年3月31日
    start_date = datetime(2011, 1, 1)
    end_date = datetime(2014, 3, 31)

    reviews = []

    # 正面评论模板
    positive_comments = [
        "质量很好，非常满意！", "物流很快，包装完好。", "性价比很高，推荐购买！",
        "使用效果很棒，物超所值。", "正品保证，值得信赖。", "客服态度很好，解决问题及时。",
        "外观漂亮，功能齐全。", "操作简单，容易上手。", "续航能力强，很实用。",
        "颜色正，尺寸合适。"
    ]

    # 负面评论模板
    negative_comments = [
        "质量一般，有点失望。", "物流太慢，等了很久。", "价格偏高，性价比不高。",
        "使用效果不如预期。", "疑似假货，需要验证。", "客服回应慢，解决问题效率低。",
        "外观有瑕疵，不满意。", "操作复杂，不容易上手。", "续航能力差，需要改进。",
        "颜色偏差大，尺寸不合适。"
    ]

    # 中性评论模板
    neutral_comments = [
        "还可以，中规中矩。", "一般般，没什么特别。", "符合预期，没什么惊喜。",
        "正常水平，可以接受。", "没什么特别感觉。", "就是普通的产品。",
        "基本满足需求。", "没什么大问题。", "普通使用没问题。", "还算满意吧。"
    ]

    for i in range(num_reviews):
        user_id = random.choice(user_ids)
        product_id = f"PRODUCT_{random.randint(0, 999)}"

        # 生成随机时间戳
        time_diff = (end_date - start_date).days
        random_days = random.randint(0, time_diff)
        random_date = start_date + timedelta(days=random_days)
        timestamp = int(random_date.timestamp())

        # 根据评分生成相应的评论内容
        rating = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.1, 0.2, 0.3, 0.35])[0]

        if rating >= 4:
            comment = random.choice(positive_comments)
            title = "好评！推荐购买"
        elif rating == 3:
            comment = random.choice(neutral_comments)
            title = "中评，一般般"
        else:
            comment = random.choice(negative_comments)
            title = "差评，需要改进"

        # 添加一些细节使评论更真实
        comment += f" 购买后使用了{random.randint(1, 30)}天，整体感觉"

        reviews.append({
            "数据ID": f"TRAIN_{i}",
            "用户ID": user_id,
            "商品ID": product_id,
            "评论时间戳": timestamp,
            "评论标题": title,
            "评论内容": comment,
            "评分": rating
        })

    return pd.DataFrame(reviews)


# 3. 生成测试集数据
def generate_test_data(num_test=5000):
    print("生成测试集数据...")

    test_reviews = []
    user_ids = list(range(5000, 6000))  # 使用不同的用户ID

    # 时间范围保持一致
    start_date = datetime(2011, 1, 1)
    end_date = datetime(2014, 3, 31)

    for i in range(num_test):
        user_id = random.choice(user_ids)
        product_id = f"PRODUCT_{random.randint(0, 999)}"

        time_diff = (end_date - start_date).days
        random_days = random.randint(0, time_diff)
        random_date = start_date + timedelta(days=random_days)
        timestamp = int(random_date.timestamp())

        # 测试集也需要有评论内容，但评分是我们要预测的
        comment = f"这是测试评论内容 {i}，商品使用体验"
        title = f"测试评论标题 {i}"

        # 测试集的评分可以是随机的，因为我们要预测它
        rating = random.randint(1, 5)

        test_reviews.append({
            "数据ID": f"TEST_{i}",
            "用户ID": user_id,
            "商品ID": product_id,
            "评论时间戳": timestamp,
            "评论标题": title,
            "评论内容": comment,
            "评分": rating
        })

    return pd.DataFrame(test_reviews)


# 生成所有数据
def generate_all_data():
    # 创建数据目录
    import os
    os.makedirs('数据', exist_ok=True)

    # 生成数据
    df_products, df_categories = generate_product_data(1000)
    df_train = generate_review_data(5000, 20000)
    df_test = generate_test_data(5000)

    # 保存数据
    df_products.to_csv('数据/商品信息.csv', index=False, encoding='utf-8')
    df_categories.to_csv('数据/商品类别列表.csv', index=False, encoding='utf-8')
    df_train.to_csv('数据/训练集.csv', index=False, encoding='utf-8')
    df_test.to_csv('数据/测试集.csv', index=False, encoding='utf-8')

    print("数据生成完成！")
    print(f"商品信息: {len(df_products)} 条")
    print(f"商品类别: {len(df_categories)} 条")
    print(f"训练集: {len(df_train)} 条")
    print(f"测试集: {len(df_test)} 条")


if __name__ == "__main__":
    generate_all_data()