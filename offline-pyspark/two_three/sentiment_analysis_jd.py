# sentiment_analysis_jd.py
# -*- coding: utf-8 -*-
"""
京东评论情感预测模型
工单编号: 大数据-用户画像-18-京东评论情感预测
创建人: 郭洵
创建时间: 2025-05-03
目标: 预测用户对商品的评分（1-5分），为后续用户画像特征计算做准备
产出:
    1. 评论情感预测代码
    2. 模型优化文档（需另见优化记录）
    3. 模型部署（需另见部署脚本）
要求:
    - 最少使用三种模型
    - 需有特征衍生、特征选择
    - 明确的注释
    - 评估指标: Micro F1
"""

import pandas as pd
import re
import jieba
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import lightgbm as lgb
import warnings

warnings.filterwarnings('ignore')

# 初始化jieba分词
jieba.initialize()


# 数据加载函数
def load_data():
    """
    加载数据文件
    返回: 训练集、测试集、商品信息、商品类别列表的DataFrame
    """
    print("正在加载数据...")
    try:
        df_product = pd.read_csv('./数据/商品信息.csv', encoding='utf-8')
        df_category = pd.read_csv('./数据/商品类别列表.csv', encoding='utf-8')
        df_train = pd.read_csv('./数据/训练集.csv', encoding='utf-8')
        df_test = pd.read_csv('./数据/测试集.csv', encoding='utf-8')

        print("数据加载成功!")
        print(f"训练集形状: {df_train.shape}")
        print(f"测试集形状: {df_test.shape}")
        print(f"评分分布:\n{df_train['评分'].value_counts().sort_index()}")

        return df_product, df_category, df_train, df_test
    except Exception as e:
        print(f"数据加载失败: {e}")
        return None, None, None, None


# 文本预处理函数
def text_preprocessing(text):
    """
    对中文文本进行预处理
    包括: 去除特殊字符、分词等
    """
    if pd.isna(text):
        return ""

    text = str(text)
    # 去除标点符号和特殊字符
    text = re.sub(r'[^\w\s]', '', text)
    # 分词
    words = jieba.cut(text)
    return ' '.join(words)


# 特征工程类
class FeatureEngineer:
    """
    特征工程类
    负责特征衍生和特征选择
    """

    def __init__(self):
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words=['的', '了', '是', '在', '和'])
        self.selected_features = None

    def create_features(self, df, df_product, is_train=True):
        """
        创建特征
        """
        print("正在进行特征工程...")

        # 复制数据避免修改原始数据
        df = df.copy()

        # 1. 文本特征
        print("处理文本特征...")
        df['clean_title'] = df['评论标题'].apply(text_preprocessing)
        df['clean_content'] = df['评论内容'].apply(text_preprocessing)
        df['text_combined'] = df['clean_title'] + ' ' + df['clean_content']

        # 2. 时间特征
        print("处理时间特征...")
        df['评论时间'] = pd.to_datetime(df['评论时间戳'], unit='s')
        df['评论年份'] = df['评论时间'].dt.year
        df['评论月份'] = df['评论时间'].dt.month
        df['评论日'] = df['评论时间'].dt.day
        df['评论小时'] = df['评论时间'].dt.hour
        df['评论星期'] = df['评论时间'].dt.dayofweek

        # 3. 长度特征
        print("处理长度特征...")
        df['标题长度'] = df['评论标题'].apply(lambda x: len(str(x)))
        df['内容长度'] = df['评论内容'].apply(lambda x: len(str(x)))
        df['总文本长度'] = df['标题长度'] + df['内容长度']

        # 4. 合并商品信息
        print("合并商品信息...")
        df = pd.merge(df, df_product, on='商品ID', how='left')

        # 5. 提取类别信息
        print("提取类别信息...")
        df[['一级类目', '二级类目', '三级类目']] = df['所属类别'].str.split('-', expand=True)

        # 6. 文本向量化
        print("文本向量化...")
        if is_train:
            text_features = self.vectorizer.fit_transform(df['text_combined'])
        else:
            text_features = self.vectorizer.transform(df['text_combined'])

        # 7. 数值特征
        print("准备数值特征...")
        numeric_features = df[['标题长度', '内容长度', '总文本长度',
                               '评论年份', '评论月份', '评论日',
                               '评论小时', '评论星期']].fillna(0)

        # 8. 类别特征编码
        print("编码类别特征...")
        for col in ['一级类目', '二级类目']:
            if col in df.columns:
                encoded = pd.get_dummies(df[col].fillna('未知'), prefix=col)
                numeric_features = pd.concat([numeric_features, encoded], axis=1)

        # 合并所有特征
        from scipy.sparse import hstack
        all_features = hstack([numeric_features, text_features])

        print(f"特征工程完成，特征维度: {all_features.shape}")

        return all_features, df['评分']


# 模型训练与评估
def train_and_evaluate(models, X_train, y_train, X_test, y_test):
    """
    训练和评估多个模型
    """
    results = {}

    for name, model in models.items():
        print(f"\n正在训练 {name} 模型...")
        model.fit(X_train, y_train)

        # 预测
        y_pred = model.predict(X_test)

        # 计算Micro F1
        micro_f1 = f1_score(y_test, y_pred, average='micro')
        results[name] = micro_f1

        print(f"{name} 模型的 Micro F1 分数: {micro_f1:.4f}")
        print("分类报告:")
        print(classification_report(y_test, y_pred))

    return results


# 特征重要性分析
def analyze_feature_importance(model, feature_engineer, X_train):
    """
    分析特征重要性
    """
    if hasattr(model, 'feature_importances_'):
        print("\n分析特征重要性...")
        feature_importance = pd.DataFrame({
            'feature': range(X_train.shape[1]),
            'importance': model.feature_importances_
        })
        feature_importance = feature_importance.sort_values('importance', ascending=False)
        print("Top 10 重要特征:")
        print(feature_importance.head(10))


# 主函数
def main():
    """
    主函数
    """
    print("开始京东评论情感预测任务")
    print("工单编号: 大数据-用户画像-18-京东评论情感预测")

    # 1. 加载数据
    df_product, df_category, df_train, df_test = load_data()
    if df_train is None:
        return

    # 2. 特征工程
    feature_engineer = FeatureEngineer()
    X, y = feature_engineer.create_features(df_train, df_product, is_train=True)

    # 3. 划分训练集和验证集
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"\n训练集: {X_train.shape}, 验证集: {X_val.shape}")

    # 4. 定义模型
    models = {
        '逻辑回归': Pipeline([
            ('scaler', StandardScaler(with_mean=False)),
            ('lr', LogisticRegression(
                multi_class='multinomial',
                solver='lbfgs',
                max_iter=1000,
                random_state=42,
                C=1.0
            ))
        ]),
        '随机森林': RandomForestClassifier(
            n_estimators=100,
            random_state=42,
            n_jobs=-1,
            max_depth=10
        ),
        'LightGBM': lgb.LGBMClassifier(
            n_estimators=100,
            random_state=42,
            n_jobs=-1,
            objective='multiclass',
            num_class=5,
            learning_rate=0.1
        )
    }

    # 5. 训练和评估模型
    print("\n开始模型训练和评估...")
    results = train_and_evaluate(models, X_train, y_train, X_val, y_val)

    # 6. 选择最佳模型
    best_model_name = max(results, key=results.get)
    best_model = models[best_model_name]
    print(f"\n最佳模型: {best_model_name}, Micro F1: {results[best_model_name]:.4f}")

    # 7. 分析特征重要性
    if hasattr(best_model, 'feature_importances_') or hasattr(best_model.steps[-1][1], 'feature_importances_'):
        analyze_feature_importance(best_model.steps[-1][1] if hasattr(best_model, 'steps') else best_model,
                                   feature_engineer, X_train)

    # 8. 在测试集上评估最佳模型
    print("\n在测试集上评估最佳模型...")
    X_test, y_test = feature_engineer.create_features(
        df_test, df_product, is_train=False
    )
    y_pred_test = best_model.predict(X_test)
    test_f1 = f1_score(y_test, y_pred_test, average='micro')
    print(f"测试集 Micro F1 分数: {test_f1:.4f}")

    # 9. 保存模型
    import joblib
    joblib.dump(best_model, f'best_model_{best_model_name}.pkl')
    joblib.dump(feature_engineer.vectorizer, 'tfidf_vectorizer.pkl')
    print("模型保存完成!")

    # 10. 输出优化建议
    print("\n=== 优化建议 ===")
    print("1. 第一轮优化: 调整模型超参数，使用网格搜索寻找最佳参数")
    print("2. 第二轮优化: 增加文本特征维度，使用Word2Vec或BERT嵌入")
    print("3. 第三轮优化: 添加用户历史行为特征，构建更复杂的特征工程")
    print("4. 考虑使用深度学习模型如TextCNN或Transformer")

    return best_model, results


if __name__ == "__main__":
    # 执行主函数
    best_model, results = main()