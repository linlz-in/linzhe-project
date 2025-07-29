import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier
from sklearn.metrics import classification_report
from sklearn.pipeline import Pipeline
import joblib
from typing import List, Tuple, Dict, Any


class TagMiningModel:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(max_features=5000, ngram_range=(1, 2))
        self.mlb = MultiLabelBinarizer()
        self.classifier = MultiOutputClassifier(
            RandomForestClassifier(n_estimators=100, random_state=42)
        )
        self.model = Pipeline([
            ('vectorizer', self.vectorizer),
            ('classifier', self.classifier)
        ])

    def preprocess_data(self, texts: List[str], tags: List[List[str]] = None) -> Tuple[List[str], np.ndarray]:
        if not isinstance(texts[0], str):
            raise ValueError("texts参数必须是字符串列表")

        if tags is not None:
            y = self.mlb.fit_transform(tags) if not hasattr(self.mlb, 'classes_') else self.mlb.transform(tags)
            return texts, y
        return texts, None

    def train(self, texts: List[str], tags: List[List[str]]) -> None:
        X, y = self.preprocess_data(texts, tags)
        self.model.fit(X, y)

    def evaluate(self, texts: List[str], tags: List[List[str]]) -> Dict[str, Any]:
        X, y_ture = self.preprocess_data(texts, tags)
        y_pred = self.model.predict(X)

        y_true_labels = self.mlb.inverse_transform(y_ture)
        y_pred_labels = self.mlb.inverse_transform(y_pred)

        report = classification_report(
            y_ture, y_pred, target_names=self.mlb.classes_, output_dict=True
        )
        return report

    def predict(self, texts: List[str]) -> List[List[str]]:
        X, _ = self.preprocess_data(texts)
        y_pred = self.model.predict(X)
        predicted_tags = self.mlb.inverse_transform(y_pred)
        return predicted_tags

    def save_model(self, model_path: str) -> None:
        joblib.dump({
            'vectorizer': self.vectorizer,
            'mlb': self.mlb,
            'model': self.model
        }, model_path)

    @classmethod
    def load_model(cls, model_path: str) -> 'TagMiningModel':
        loaded = joblib.load(model_path)
        model = cls()
        model.vectorizer = loaded['vectorizer']
        model.mlb = loaded['mlb']
        model.model = loaded['model']
        return model


if __name__ == "__main__":
    texts = [
        "python是一种编程语言，用于机器学习和数据分析",
        "Java是一种广泛使用的面向对象编程语言",
        "数据分析需要统计学知识和编程技能",
        "深度学习是机器学习的一个分支，专注于神经网络"
    ]
    tags = [
        ["Python", "机器学习", "数据分析"],
        ["Java", "编程语言"],
        ["数据分析", "统计学", "编程"],
        ["深度学习", "机器学习", "神经网络"]
    ]

    model = TagMiningModel()
    model.train(texts, tags)

    report = model.evaluate(texts, tags)
    print("模型评估报告：")
    for label, metrics in report.items():
        if label in ["micro avg", "macro avg", "weighted avg", "samples avg"]:
            continue
        print(f"\n标签：{label}")
        for metric, value in metrics.items():
            print(f" {metric}: {value:.4f}")

    new_texts = [
        "Python在人工智能领域应用广泛",
        "神经网络需要大量数据进行训练"
    ]
    predicted_tags = model.predict(new_texts)
    print("\n预测结果")
    for text, tags in zip(new_texts, predicted_tags):
        print(f"文本：{text}")
        print(f"预测标签：{', '.join(tags)}")
        print()
    model.save_model("tag_mining_model.pkl")
    loaded_model = TagMiningModel.load_model("tag_mining_model.pkl")
    loaded_preds = loaded_model.predict(new_texts)
    print("加载模型的预测结果：")
    for text, tags in zip(new_texts, loaded_preds):
        print(f"文本：{text}")
        print(f"预测标签: {', '.join(tags)}")
