import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import joblib
import os

DATA_DIR = os.getenv("DATA_DIR", "/home/cha/news_project_data")
train_path = os.path.join(DATA_DIR, "imdb_train.csv")
test_path  = os.path.join(DATA_DIR, "imdb_test.csv")
model_dir  = os.path.join("/home/cha", "models")
os.makedirs(model_dir, exist_ok=True)

def main():
    # 1️⃣ 데이터 로드
    train = pd.read_csv(train_path)
    test = pd.read_csv(test_path)

    # 2️⃣ 벡터화
    vectorizer = TfidfVectorizer(max_features=10000)
    X_train = vectorizer.fit_transform(train["clean_text"])
    X_test  = vectorizer.transform(test["clean_text"])

    # 3️⃣ 모델 학습
    model = LogisticRegression(max_iter=200)
    model.fit(X_train, train["label"])

    # 4️⃣ 평가
    preds = model.predict(X_test)
    print(classification_report(test["label"], preds, digits=3))

    # 5️⃣ 저장
    joblib.dump(model, os.path.join(model_dir, "sentiment_model.joblib"))
    joblib.dump(vectorizer, os.path.join(model_dir, "vectorizer.joblib"))
    print(f"💾 Model saved to {model_dir}")

if __name__ == "__main__":
    main()
