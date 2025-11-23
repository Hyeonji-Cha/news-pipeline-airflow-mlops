from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import os

# ---------------------------
# [INIT] 앱, 모델, 벡터라이저
# ---------------------------
app = FastAPI(title="News Sentiment API", version="1.0")

MODEL_DIR = os.path.join(os.path.dirname(__file__), "../models")
model_path = os.path.join(MODEL_DIR, "sentiment_model.joblib")
vectorizer_path = os.path.join(MODEL_DIR, "vectorizer.joblib")

model = joblib.load(model_path)
vectorizer = joblib.load(vectorizer_path)
print("✅ Model & Vectorizer loaded successfully")

# ---------------------------
# [INPUT SCHEMA]
# ---------------------------
class TextInput(BaseModel):
    text: str

# ---------------------------
# [ROUTES]
# ---------------------------
@app.get("/")
def root():
    return {"message": "✅ Sentiment API is running"}

@app.post("/predict")
def predict(input_data: TextInput):
    text = input_data.text
    vec = vectorizer.transform([text])
    pred = model.predict(vec)[0]
    label = "Positive" if pred == 1 else "Negative"
    return {"input": text, "prediction": int(pred), "label": label}
