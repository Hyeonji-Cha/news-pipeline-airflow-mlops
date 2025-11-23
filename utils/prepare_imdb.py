import pandas as pd
import os
from sklearn.model_selection import train_test_split
from nlp_utils import preprocess_text

# 데이터 파일 경로
DATA_DIR = os.getenv("DATA_DIR", "/home/cha/news_project_data")
RAW_PATH = os.path.join(DATA_DIR, "IMDB Dataset.csv")

def main():
    # 1️⃣ 데이터 로드
    df = pd.read_csv(RAW_PATH)
    print(f"✅ Loaded: {df.shape[0]} rows")

    # 2️⃣ 전처리: 텍스트 정리
    df["clean_text"] = df["review"].astype(str).apply(lambda x: " ".join(preprocess_text(x)))
    df["label"] = df["sentiment"].apply(lambda x: 1 if x.lower() == "positive" else 0)

    # 3️⃣ 데이터 분할 (train 80%, test 20%)
    train_df, test_df = train_test_split(df[["clean_text", "label"]], test_size=0.2, random_state=42)

    # 4️⃣ 저장
    os.makedirs(DATA_DIR, exist_ok=True)
    train_path = os.path.join(DATA_DIR, "imdb_train.csv")
    test_path = os.path.join(DATA_DIR, "imdb_test.csv")

    train_df.to_csv(train_path, index=False)
    test_df.to_csv(test_path, index=False)

    print(f"💾 Saved train: {train_path}")
    print(f"💾 Saved test:  {test_path}")

if __name__ == "__main__":
    main()
