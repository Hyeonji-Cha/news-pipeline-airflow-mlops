from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from datetime import datetime, timedelta
import requests
import psycopg2
import pandas as pd
import re
import os
import sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "utils"))
from apply_sentiment import apply_sentiment
from nlp_utils import preprocess_text


def tokenize_news(**kwargs):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        port=int(os.getenv("DB_PORT", "5432")),
    )
    cur = conn.cursor()

    # 전처리 결과 저장용 테이블
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news_preprocessed (
            news_id      INTEGER PRIMARY KEY,
            title_clean  TEXT,
            content_clean TEXT,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    # 원본 뉴스 가져오기
    cur.execute("SELECT id, title, content FROM news;")
    rows = cur.fetchall()

    # 전처리 & UPSERT
    for news_id, title, content in rows:
        title_clean   = " ".join(preprocess_text(title or ""))
        content_clean = " ".join(preprocess_text(content or ""))
        cur.execute("""
            INSERT INTO news_preprocessed (news_id, title_clean, content_clean, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (news_id) DO UPDATE
            SET title_clean = EXCLUDED.title_clean,
                content_clean = EXCLUDED.content_clean,
                updated_at = now();
        """, (news_id, title_clean, content_clean))

    conn.commit()
    cur.close()
    conn.close()

# 이사 대비: .env 지원 (없어도 동작하도록 기본값 유지)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---- 이사 대비: 환경 변수(없으면 기존 값 사용) ----
DATA_DIR = os.getenv("DATA_DIR", "/tmp")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

# 📢 Slack 실패 알람 콜백
def slack_alert(context):
    slack_msg = f"""
    :red_circle: Task Failed!
    *DAG*: {context.get('dag').dag_id}
    *Task*: {context.get('task_instance').task_id}
    *Execution Time*: {context.get('execution_date')}
    *Log Url*: {context.get('task_instance').log_url}
    *Exception*: {context.get('exception')}
    """
    alert = SlackWebhookOperator(
        task_id="slack_notification_fail",
        slack_webhook_conn_id="slack_default",
        message=slack_msg,
        channel="#airflow-alert",
        username="airflow-bot",
    )
    return alert.execute(context=context)

# 📢 Slack 성공 알람 콜백 (DAG 레벨)
def dag_success_alert(context):
    slack_msg = f"""
    :large_blue_circle: DAG Succeed!
    *DAG*: {context.get('dag').dag_id}
    *Run Id*: {context.get('run_id')}
    *Execution Time*: {context.get('execution_date')}
    """
    alert = SlackWebhookOperator(
        task_id="slack_notification_success",
        slack_webhook_conn_id="slack_default",
        message=slack_msg,
        channel="#airflow-alert",
        username="airflow-bot",
    )
    return alert.execute(context=context)

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": slack_alert,
}

# 1️⃣ 뉴스 API 호출
def fetch_news(**kwargs):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": "AI OR 인공지능 OR 머신러닝",
        "pageSize": 50,
        "sortBy": "publishedAt", 
        "apiKey": NEWS_API_KEY,
    }
    res = requests.get(url, params=params, timeout=30)
    res.raise_for_status()
    articles = res.json().get("articles", [])
    # 데이터 0건일 때 경고 출력
    if not articles:
        print("⚠️ No articles fetched! Check query or API limits.")
    else:
        print(f"✅ {len(articles)} articles fetched.")

    kwargs["ti"].xcom_push(key="articles", value=articles)

# 2️⃣ 전처리 + CSV 저장
def preprocess_news(**kwargs):
    articles = kwargs["ti"].xcom_pull(key="articles", task_ids="fetch_news")
    if not articles:
        raise Exception("No articles fetched for preprocessing!")

    df = pd.DataFrame(articles)
    df = df[["title", "content", "publishedAt", "url", "source", "description"]].dropna(
        subset=["title", "content", "publishedAt"]
    )
    df["source"] = df["source"].apply(
        lambda source: source.get("name", "") if isinstance(source, dict) else source
    )

    def _clean(s: str) -> str:
        s = s.lower()
        s = re.sub(r"[^a-zA-Z0-9가-힣 ]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    df["title"] = df["title"].astype(str).apply(_clean)
    df["content"] = df["content"].astype(str).apply(_clean)

    os.makedirs(DATA_DIR, exist_ok=True)
    output_path = os.path.join(DATA_DIR, "preprocessed_news.csv")
    df.to_csv(output_path, index=False)

    kwargs["ti"].xcom_push(key="csv_path", value=output_path)

# 3️⃣ CSV → Postgres 적재 (중복 방지: title UNIQUE)
def save_to_postgres(**kwargs):
    csv_path = kwargs["ti"].xcom_pull(key="csv_path", task_ids="preprocess_news")
    if not csv_path or not os.path.exists(csv_path):
        raise Exception("CSV file not found!")

    df = pd.read_csv(csv_path)

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT UNIQUE,
            content TEXT,
            published_at TIMESTAMP
        );
    """)

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO news (title, content, published_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (title) DO NOTHING;
        """, (row["title"], row["content"], row["publishedAt"]))

    conn.commit()
    cur.close()
    conn.close()

# 4️⃣ 일별 집계 테이블 적재 (AI/인공지능/머신러닝 키워드 카운트)
def compute_daily_stats(**kwargs):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
    )
    cur = conn.cursor()

    # 집계 테이블
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news_daily_stats (
            stat_date DATE PRIMARY KEY,
            article_count INTEGER NOT NULL,
            ai_count INTEGER NOT NULL,
            ml_count INTEGER NOT NULL,
            ko_ai_count INTEGER NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)

    # 뉴스 테이블에서 일자별로 집계해서 upsert
    cur.execute("""
        WITH base AS (
            SELECT
                DATE(published_at) AS d,
                COUNT(*) AS article_count,
                SUM( (title ILIKE '%%ai%%' OR content ILIKE '%%ai%%')::int ) AS ai_count,
                SUM( (title ILIKE '%%머신러닝%%' OR content ILIKE '%%머신러닝%%')::int ) AS ml_count,
                SUM( (title ILIKE '%%인공지능%%' OR content ILIKE '%%인공지능%%')::int ) AS ko_ai_count
            FROM news
            WHERE published_at IS NOT NULL
            GROUP BY 1
        )
        INSERT INTO news_daily_stats (stat_date, article_count, ai_count, ml_count, ko_ai_count, updated_at)
        SELECT d, article_count, ai_count, ml_count, ko_ai_count, now()
        FROM base
        ON CONFLICT (stat_date) DO UPDATE
        SET article_count = EXCLUDED.article_count,
            ai_count      = EXCLUDED.ai_count,
            ml_count      = EXCLUDED.ml_count,
            ko_ai_count   = EXCLUDED.ko_ai_count,
            updated_at    = now();
    """)

    conn.commit()
    cur.close()
    conn.close()


# DAG 정의
with DAG(
    "news_pipeline",
    default_args=default_args,
    description="뉴스 수집 → 전처리 → CSV → DB → 일별 집계 (Slack 알림 포함)",
    schedule_interval= "*/10 * * * *",  # 스케줄 
    start_date=datetime(2025, 10, 18),
    catchup=False,
    on_success_callback=dag_success_alert,
) as dag:

    t1 = PythonOperator(
        task_id="fetch_news",
        python_callable=fetch_news,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="preprocess_news",
        python_callable=preprocess_news,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="save_to_postgres",
        python_callable=save_to_postgres,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="compute_daily_stats",
        python_callable=compute_daily_stats,
        provide_context=True,
    )

    t5 = PythonOperator(
    task_id="tokenize_news",
    python_callable=tokenize_news,
    provide_context=True,
    )

    t6 = PythonOperator(
    task_id="apply_sentiment",
    python_callable=apply_sentiment,
    provide_context=True,
)



    t1 >> t2 >> t3 >> t4 >> t5 >> t6
