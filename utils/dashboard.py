import psycopg2
import pandas as pd
import streamlit as st
from wordcloud import WordCloud
import requests
import os
import matplotlib.pyplot as plt
from dotenv import load_dotenv
load_dotenv(dotenv_path="/home/cha/airflow/.env")

# ----------------------------
# [1] 환경 변수 로드
# ----------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
API_URL = "http://127.0.0.1:8000/predict"   # FastAPI 서버 주소

# ----------------------------
# [2] DB 연결 함수
# ----------------------------
@st.cache_data(ttl=300)
def load_data():
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT
    )
    df_news = pd.read_sql("SELECT * FROM news_sentiment;", conn)
    df_stats = pd.read_sql("SELECT * FROM news_daily_stats;", conn)
    conn.close()
    return df_news, df_stats

# ----------------------------
# [3] Streamlit UI 구성
# ----------------------------
st.set_page_config(page_title="AI 뉴스 감성 대시보드", layout="wide")
st.title("📰 AI 뉴스 감성 분석 대시보드")

df_news, df_stats = load_data()

# --- 통계 요약 ---
st.subheader("📊 전체 통계")
col1, col2 = st.columns(2)
with col1:
    st.metric("총 기사 수", len(df_news))
with col2:
    pos_ratio = df_news["label"].mean() * 100
    st.metric("긍정 기사 비율", f"{pos_ratio:.1f}%")

# --- 일별 통계 ---
st.subheader("📅 일별 기사 추이")
st.line_chart(df_stats.set_index("stat_date")[["article_count", "ai_count", "ml_count", "ko_ai_count"]])


# ✅ 조인 쿼리 — sentiment + preprocessed
conn = psycopg2.connect(
    host=DB_HOST, dbname=DB_NAME, user=DB_USER,
    password=DB_PASSWORD, port=DB_PORT
)

query = """
SELECT 
    s.news_id,
    s.label,
    p.title_clean,
    p.content_clean,
    p.updated_at
FROM news_sentiment s
JOIN news_preprocessed p 
    ON s.news_id = p.news_id;
"""
df_news = pd.read_sql(query, conn)
conn.close()

# ===========================
# 🎨 워드클라우드 생성
# ===========================
st.subheader("☁️ 워드클라우드 (Positive vs Negative)")

# content_clean이 없는 경우 대비
if "content_clean" not in df_news.columns:
    st.error("❌ content_clean 컬럼이 없습니다. news_preprocessed 테이블 확인 필요.")
else:
    text_pos = " ".join(df_news.query("label == 1")["content_clean"].astype(str))
    text_neg = " ".join(df_news.query("label == 0")["content_clean"].astype(str))

    wc_pos = WordCloud(width=800, height=400, background_color="white").generate(text_pos)
    wc_neg = WordCloud(width=800, height=400, background_color="black").generate(text_neg)

    st.image(wc_pos.to_array(), caption="Positive")
    st.image(wc_neg.to_array(), caption="Negative")

# ===========================
# 📅 날짜별 감성 비율 추이
# ===========================
st.subheader("📅 날짜별 긍정 비율(%)")

conn = psycopg2.connect(
    host=DB_HOST, dbname=DB_NAME, user=DB_USER,
    password=DB_PASSWORD, port=DB_PORT
)

query_ratio = """
SELECT
  DATE(n.published_at) AS stat_date,
  COUNT(*) AS total_cnt,
  SUM(CASE WHEN s.label = 1 THEN 1 ELSE 0 END) AS pos_cnt,
  SUM(CASE WHEN s.label = 0 THEN 1 ELSE 0 END) AS neg_cnt
FROM news_sentiment s
JOIN news n ON s.news_id = n.id
WHERE n.published_at IS NOT NULL
GROUP BY 1
ORDER BY 1;
"""
df_ratio = pd.read_sql(query_ratio, conn)
conn.close()

if not df_ratio.empty:
    df_ratio["pos_ratio"] = (df_ratio["pos_cnt"] / df_ratio["total_cnt"]) * 100
    st.line_chart(df_ratio.set_index("stat_date")[["pos_ratio"]])
else:
    st.info("아직 날짜별 감성 집계가 없습니다.")
