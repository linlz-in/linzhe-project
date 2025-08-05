import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from typing import List, Dict, Any, Tuple

# 解决中文错误
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
# 设置负号显示
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题


class RFM:
    def __init__(self, data: pd.DataFrame = None):
        self.data = data
        self.rfm_data = None
        self.segmented_data = None
        self.scaler = StandardScaler()

    def preprocess_data(self, customer_id: str = 'CustomerID',
                        order_date: str = 'OrderData',
                        order_id: str = 'OrderID',
                        revenue: str = 'Revenue') -> None:
        if self.data is None:
            raise ValueError("请先设置数据")
        self.data[order_date] = pd.to_datetime(self.data[order_date])
        snapshot_date = self.data[order_date].max() + timedelta(days=1)
        self.rfm_data = self.data.groupby(customer_id).agg({
            order_date: lambda x: (snapshot_date - x.max()).days,
            order_id: 'count',
            revenue: 'sum'
        }).reset_index()

        self.rfm_data.rename(columns={
            customer_id: 'CustomerID',
            order_date: 'Recency',
            order_id: 'Frequency',
            revenue: 'Monetary'
        }, inplace=True)

        print(f"RFM数据已计算，包含{len(self.rfm_data)}个客户")

    def calculate_rfm_scores(self, bins: List[int] = [4, 4, 4], labels: List[List[str]] = None) -> None:
        if self.rfm_data is not None:
            raise ValueError("请先预处理数据")
        if labels is None:
            labels = [
                ['4', '3', '2', '1'],
                ['1', '2', '3', '4'],
                ['1', '2', '3', '4']
            ]

        for i in range(3):
            if len(labels[i] != bins[i]):
                raise ValueError(f"标签数量与分箱数不匹配，维度{i + 1}需要{bins[i]}个标签")

        self.rfm_data['R_Score'] = pd.qcut(
            self.rfm_data['Recency'], q=bins[0], labels=labels[0], duplicates='drop'
        )
        self.rfm_data['F_Score'] = pd.qcut(
            self.rfm_data['Frequency'], q=bins[1], labels=labels[1], duplicates='drop'
        )
        self.rfm_data['M_Score'] = pd.qcut(
            self.rfm_data['Monetary'], q=bins[2], labels=labels[2], duplicates='drop'
        )

        self.rfm_data['RFM_Segment'] = self.rfm_data['R_Score'].astype(str) + \
                                       self.rfm_data['F_Score'].astype(str) + \
                                       self.rfm_data['M_Score'].astype(str)
        self.rfm_data['RFM_Score'] = self.rfm_data[['R_Score', 'F_Score', 'M_Score']].astype(int).sum(axis=1)
        print("RFM分数已计算完成")

    def segment_customers(self, n_segment: int = 5) -> None:
        if self.rfm_data is None:
            raise ValueError("请先预处理数据")

        rfm_values = self.rfm_data[['Recency', 'Frequency', 'Monetary']]
        if rfm_values.isna().any().any():
            raise ValueError("RFM数据包含缺失值，请先处理")
        if (rfm_values == 0).all().all():
            raise ValueError("RFM数据全为0，无法进行聚类")

        rfm_log = np.log(rfm_values + 1)
        rfm_scaled = self.scaler.fit_transform(rfm_log)
        kmeans = KMeans(n_clusters=n_segment, random_state=42)
        self.rfm_data['Cluster'] = kmeans.fit_predict(rfm_scaled)

        if 'Cluster' not in self.rfm_data.columns:
            raise RuntimeError("Cluster字段生成失败，请检查K-means执行过程")

        self.segmented_data = self.rfm_data.copy()
        print("segment_customers成功：", self.segmented_data.columns.tolist())

    def generate_promotions(self, discount_rates: Dict[int, float] = None,
                            min_purchase: Dict[int, float] = None) -> pd.DataFrame:
        if self.segmented_data is None:
            discount_rates = {i: 0.1 + i * 0.05 for i in range(self.segmented_data['Cluster'].nunique())}
        if min_purchase is None:
            min_purchase = {i: 100 + i * 100 for i in range(self.segmented_data['Cluster'].nunique())}

        self.segmented_data['Discount_Rate'] = self.segmented_data['Cluster'].map(discount_rates)
        self.segmented_data['Min_Purchase'] = self.segmented_data['Cluster'].map(min_purchase)
        self.segmented_data['Promotion'] = self.segmented_data.apply(
            lambda x: f"满{x['Min_Purchase']}元减{x['Min_Purchase'] * x['Discount_Rate']:.0f}元", axis=1
        )
        return self.segmented_data[['CustomerID', 'Cluster', 'Discount_Rate', 'Min_Purchase', 'Promotion']]

    def visualize_rfm(self) -> None:
        if self.rfm_data is not None:
            raise ValueError("请先预处理数据")

        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        sns.histplot(self.rfm_data['Recency'], bins=20, ax=axes[0])
        axes[0].set_title('最近购买时间分布')
        sns.histplot(self.rfm_data['Frequency'], bins=20, ax=axes[1])
        axes[1].set_title('购买频率分布')
        sns.histplot(self.rfm_data['Monetary'], bins=20, ax=axes[2])
        axes[2].set_title('消费金额分布')

        plt.tight_layout()
        plt.show()

        if 'RFM_Score' in self.rfm_data.columns:
            plt.figure(figsize=(10, 6))
            sns.countplot(x='Recency', y='Frequency', hue='Cluster',
                          size='Monetary', sizes=(50, 200), alpha=0.6,
                          data=self.rfm_data, palette='viridis')
            plt.title('客户聚类结果（Recency vs Frequency）')
            plt.show()


class ProductPromotionTool:
    def __init__(self, products: pd.DataFrame = None, rfm: RFM = None):
        self.products = products
        self.rfm = rfm
        self.campaigns = {}

    def set_products(self, products: pd.DataFrame) -> None:
        self.products = products

    def set_rfm_analyzer(self, rfm: RFM) -> None:
        self.rfm = rfm

    def create_campaign(self, campaign_name: str,
                        product_categories: List[str],
                        target_clusters: List[int],
                        discount_strategy: Dict[int, float] = None) -> None:
        if self.products is None:
            raise ValueError("请先设置商品数据")
        if self.rfm is None or self.rfm.segmented_data is None:
            raise ValueError("请先设置RFM分析器并完成客户细分")
        if discount_strategy is None:
            discount_strategy = {cluster: 0.1 for cluster in target_clusters}

        campaign_products = self.products[self.products['Category'].isin(product_categories)]
        target_customers = self.rfm.segmented_data[
            self.rfm.segmented_data['Cluster'].isin(target_clusters)
        ]

        self.campaigns[campaign_name] = {
            'products': campaign_products,
            'target_customers': target_customers,
            'discount_strategy': discount_strategy,
            'creation_date': datetime.now()
        }

        print(f"营销活动 '{campaign_name}' 创建成功")
        print(f"  参与商品: {len(campaign_products)}个")
        print(f"  目标客户: {len(target_customers)}个")

    def generate_campaign_recommendations(self, campaign_name: str) -> pd.DataFrame:
        if campaign_name not in self.campaigns:
            raise ValueError(f"营销活动 '{campaign_name}' 不存在")

        campaign = self.campaigns[campaign_name]
        recommendations = []
        for _, customer in campaign['target_customers'].iterrows():
            cluster = customer['Cluster']
            discount = campaign['discount_strategy'].get(cluster, 0.1)

            recommended_products = campaign['products'].sample(
                min(3, len(campaign['products'])), random_state=42
            )

            for _, product in recommended_products.iterrows():
                recommendations.append({
                    'CustomerID': customer['CustomerID'],
                    'Recommended_Cluster': cluster,  # 重命名字段，避免冲突
                    'ProductID': product['ProductID'],
                    'ProductName': product['ProductName'],
                    'Category': product['Category'],
                    'OriginalPrice': product['Price'],
                    'DiscountedPrice': product['Price'] * (1 - discount),
                    'DiscountRate': discount,
                    'Promotion': f"{int(discount * 10)}折优惠"
                })

            return pd.DataFrame(recommendations)
