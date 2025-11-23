import os
import pandas as pd
import psycopg2
import joblib
from dotenv import load_dotenv
load_dotenv(dotenv_path="/home/cha/airflow/.env")  # [SEAM] DB_PASSWORD 자동 로드


# [SEAM] 모델 및 데이터 경로 --------------------------------------------------
MODEL_DIR = os.path.join("/home/cha", "models")
DATA_DIR = os.getenv("DATA_DIR", "/home/cha/news_project_data")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

# [CORE] 메인 함수 ------------------------------------------------------------
def apply_sentiment():
    # 1️⃣ 모델 로드
    model_path = os.path.join(MODEL_DIR, "sentiment_model.joblib")  # [SEAM]
    vec_path = os.path.join(MODEL_DIR, "vectorizer.joblib")         # [SEAM]
    model = joblib.load(model_path)
    vectorizer = joblib.load(vec_path)
    print("✅ Model & Vectorizer loaded")

    # 2️⃣ 뉴스 데이터 가져오기
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
    )
    cur = conn.cursor()
    cur.execute("SELECT news_id, content_clean FROM news_preprocessed;")  # [SEAM]
    rows = cur.fetchall()
    if not rows:
        print("⚠️ No rows to predict")
        return

    # 3️⃣ DataFrame으로 변환 및 예측
    df = pd.DataFrame(rows, columns=["news_id", "content_clean"])
    X = vectorizer.transform(df["content_clean"].astype(str))  # [CORE]
    preds = model.predict(X)                                   # [CORE]
    df["label"] = preds

    # 4️⃣ 결과 테이블 생성 및 UPSERT
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news_sentiment (
            news_id      INTEGER PRIMARY KEY,
            label        INTEGER,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)  # [RISK]

    for _, row in df.iterrows():  # [CORE]
        cur.execute("""
            INSERT INTO news_sentiment (news_id, label, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (news_id) DO UPDATE
            SET label = EXCLUDED.label,
                updated_at = now();
        """, (row["news_id"], int(row["label"])))  # [RISK]

    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 {len(df)} predictions saved to news_sentiment")

if __name__ == "__main__":
    apply_sentiment()
