# AI Workflow Log

## 2026-05-26

### Goal
GX validation 확장을 위한 사전 준비

### AI Roles
- ChatGPT: PM / Debugger / Planner
- Codex: Code Generator
- Human: Final approval

### Task 1. CSV 컬럼 보존
- `dags/news_dag.py`의 `preprocess_news()` 수정
- 기존 `title`, `content`, `publishedAt` 유지
- 검증용 컬럼 `url`, `source`, `description` 추가 보존
- `source` dict를 source name 문자열로 변환
- 샘플 테스트 결과: 6개 컬럼 생성 확인

### Task 2. GX 설치
- venv 환경에 `great_expectations==1.17.2` 설치
- CLI는 잡히지 않았지만 Python import 방식으로 진행하기로 결정

### Task 3. 기본 validation script 생성
- `utils/validate_news_data.py` 생성
- CSV 존재 여부, 필수 컬럼, row count 검증
- 실제 CSV 테스트 결과:
  - row count: 50
  - missing columns: []
  - status: PASSED
- 없는 파일 입력 시 Exception 발생 확인

### Key Commands
```bash
python -m py_compile dags/news_dag.py
python utils/validate_news_data.py --input /tmp/news_gx_real/preprocessed_news.csv
python utils/validate_news_data.py --input /tmp/not_exists.csv
```

### commits
```bash
git commit -m "Preserve news metadata columns for validation"
git commit -m "Add basic news data validation script"
```

## 2026-05-31

### Task 4. Row-level validation 규칙 추가

- `utils/validate_news_data.py`에 row별 `fail_reason` 생성 로직 추가
- 검증 규칙:
  - missing_title
  - missing_url
  - invalid_url_format
  - missing_source
  - missing_publishedAt
  - invalid_publishedAt
  - future_publishedAt
  - short_text
- 실제 News API CSV 테스트 결과:
  - total rows: 50
  - valid rows: 50
  - quarantine rows: 0
- 불량 샘플 CSV 테스트 결과:
  - total rows: 3
  - valid rows: 1
  - quarantine rows: 2
  - fail_reason counts 정상 출력 확인

### Commit

```bash
git commit -m "Add row-level news data validation rules"
```