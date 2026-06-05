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

### Task 5. validation 결과 파일 저장

- `utils/validate_news_data.py`에 `--output-dir` 옵션 추가
- validation 결과를 아래 파일로 저장하도록 구현
  - `valid_news.csv`
  - `quarantine_news.csv`
  - `validation_summary.json`
- 정상 CSV, 일부 불량 CSV, 전체 불량 CSV로 테스트
- 전체 불량 케이스에서도 결과 파일 저장 후 `status: FAILED`와 Exception 발생 확인

### Commit

```bash
git commit -m "Save validation results to output files"
```
### Key Commands

```bash
python -m py_compile utils/validate_news_data.py

python utils/validate_news_data.py \
  --input /tmp/news_gx_real/preprocessed_news.csv \
  --output-dir /tmp/news_validation_results

cat /tmp/news_validation_results/validation_summary.json
```
### Result

- normal sample: valid 50 / quarantine 0 / status PASSED
- bad sample: valid 1 / quarantine 2 / status PASSED
- all bad sample: valid 0 / quarantine 1 / status FAILED + Exception

### Task 6. GX validation summary 추가

- `utils/validate_news_data.py`에 GX sidecar validation 추가
- GX 결과를 `gx_validation_summary.json`으로 저장
- GX는 quarantine 판단이 아니라 dataset-level expectation 결과 기록 용도로 사용
- GX 실패가 Pandas validation output 저장을 막지 않도록 non-blocking 처리
- 테스트 결과:
  - success: true
  - evaluated_expectations: 6
  - successful_expectations: 6
  - unsuccessful_expectations: 0

### Key Commands

```bash
cd ~/airflow

python -m py_compile utils/validate_news_data.py

python utils/validate_news_data.py \
  --input /tmp/news_gx_real/preprocessed_news.csv \
  --output-dir /tmp/news_validation_results

ls -lh /tmp/news_validation_results

python -m json.tool /tmp/news_validation_results/gx_validation_summary.json
```

### Issue

처음 GX 실행 시 아래 에러 발생:

```text
module 'great_expectations.expectations' has no attribute 'ExpectTableColumnsToContainSet'
```

### Fix

`ExpectTableColumnsToContainSet` 대신 `ExpectTableColumnsToMatchSet`을 사용하도록 수정했다.

```python
gx.expectations.ExpectTableColumnsToMatchSet(
    column_set=REQUIRED_COLUMNS,
    exact_match=False,
)
```

### Result

- `gx_validation_summary.json` 생성 확인
- GX expectation 6개 모두 성공
- 기존 `valid_news.csv`, `quarantine_news.csv`, `validation_summary.json` 저장 흐름 유지 확인

### Commit

```bash
git commit -m "Add GX validation summary output"
```
## 2026-06-03

### Task 7. validation_summary.json에 GX 요약 통합

### Summary
- `validation_summary.json`에 Pandas/GX/overall 상태 필드 추가
- GX 상세 결과는 `gx_validation_summary.json`에 유지
- 실제 데이터에서 `title` null 2건으로 GX expectation 1개 실패
- Pandas는 해당 row를 quarantine 처리하고 valid row가 남아 `overall_status: PASSED`

### Result
- `pandas_status`: `PASSED`
- `gx_status`: `FAILED`
- `overall_status`: `PASSED`
- `gx_unsuccessful_expectations`: `1`

### Key Commands
- Common validation commands 실행

### Commit
```bash
git commit -m "Add GX summary fields to validation report"
```

### Task 8. GX URL regex expectation 추가

- GX expectation에 `ExpectColumnValuesToMatchRegex` 추가
- `url` 컬럼이 `http://` 또는 `https://`로 시작하는지 GX summary에도 기록
- Pandas는 기존처럼 `invalid_url_format` row를 quarantine 처리
- GX는 dataset-level 검증 결과 기록 역할만 유지
- `overall_status`는 계속 Pandas 기준으로 유지

### Result

- `gx_evaluated_expectations`: 7
- 기존 validation output 파일 생성 유지 확인
- `validation_summary.json`과 `gx_validation_summary.json` 정상 생성 확인

### Commit

```bash
git commit -m "Add GX URL regex expectation"
```

### Task 13. Airflow DAG integration test

- `validate_news_data` task가 DAG dependency에 정상 삽입됨
- DAG 실행 결과 `state=success` 확인
- validation output 생성 확인
  - `valid_news.csv`
  - `quarantine_news.csv`
  - `validation_summary.json`
- `save_to_postgres`가 `valid_news_path`를 읽도록 변경하여 검증 통과 데이터만 적재하도록 연결
- PostgreSQL 조회 정상 확인
- Slack callback은 `slack_default` connection 미설정으로 실패했으나 DAG 본체 실행은 성공

### Note

Slack 알림 문제는 validation/DB 적재와 분리하여 별도 Task로 처리 예정

### Task 14. Slack callback optional 처리

- 로컬 Airflow 테스트에서 `slack_default` connection이 없어도 DAG 본체 실행이 방해받지 않도록 수정
- `ENABLE_SLACK_ALERT=true`일 때만 Slack callback 실행
- 기본값은 false로 두어 로컬 테스트에서는 Slack 알림을 skip
- success callback과 failure callback 모두 동일하게 optional 처리

### Result

- `ENABLE_SLACK_ALERT` 미설정 상태에서 Slack callback skip 확인
- Slack connection 조회 없이 callback return
- DAG validation/DB 로직 변경 없음

### Commit

```bash
git commit -m "Make Slack alerts optional for local testing"
```

### Task 17. validation_log 테이블 추가

- `validation_summary.json`의 핵심 지표를 PostgreSQL `validation_log` 테이블에 저장하도록 추가
- `save_validation_log` task를 `validate_news_data`와 `save_to_postgres` 사이에 삽입
- DAG 실행별 `dag_id`, `run_id`, `logical_date`, row count, Pandas/GX/overall status를 기록
- `(dag_id, run_id)` unique constraint와 `ON CONFLICT DO UPDATE`를 적용해 같은 DAG run 재실행 시 중복 로그를 방지

### Result

- Airflow task tree 정상 확인
- `validation_log` 적재 확인
- 적재 결과:
  - total_rows: 49
  - valid_rows: 48
  - quarantine_rows: 1
  - pandas_status: PASSED
  - gx_status: FAILED
  - overall_status: PASSED
  - gx_unsuccessful_expectations: 1

## 2026-06-05
  ### Task 20. 실패 사유별 집계 결과 확인

- `validation_log.fail_reason_counts` JSONB 컬럼을 SQL로 펼쳐 실패 사유별 건수를 확인
- `jsonb_each_text()`와 `LATERAL`을 사용해 JSONB key/value를 row 형태로 변환
- 누적 실패 사유 집계 결과:
  - `missing_title`: 1건

### Result

```text
fail_reason   | total_failed_count
missing_title | 1
```
### Key SQL
```bash
SELECT
    reason.key AS fail_reason,
    SUM(reason.value::integer) AS total_failed_count
FROM validation_log,
LATERAL jsonb_each_text(fail_reason_counts) AS reason
GROUP BY reason.key
ORDER BY total_failed_count DESC;
```
## Task 21. dbt 도입 및 PostgreSQL 연결 확인

### Goal

PostgreSQL에 적재된 `news`, `validation_log` 테이블을 이후 dbt 모델로 관리하기 위해 dbt Core 프로젝트를 도입한다.

Airflow 실행 환경과 dbt 실행 환경의 의존성 충돌을 피하기 위해 dbt 전용 가상환경을 분리했다.

### Why

기존 파이프라인은 Airflow에서 수집, 전처리, validation, PostgreSQL 적재까지 처리했다.

하지만 적재 이후의 SQL 모델링, 집계, 품질 지표 계산을 Airflow Python 코드 안에 계속 두면 역할이 섞인다.

따라서 dbt를 도입해 다음 역할을 분리한다.

```text
Airflow = 전체 task orchestration
GX/Pandas = 적재 전 데이터 품질 검증
PostgreSQL = 원천/로그 데이터 저장
dbt = 적재 후 SQL 모델링 및 테스트
```

### Changes

* `~/dbt_venv` dbt 전용 가상환경 생성
* `dbt-core 1.10.22`, `dbt-postgres 1.10.0` 설치
* `dbt_news/dbt_project.yml` 생성
* dbt profile `news_pipeline`을 `~/.dbt/profiles.yml`에 설정
* PostgreSQL 연결 확인 완료
* dbt 실행 중 생성되는 local artifact를 `.gitignore`에 추가

  * `/dbt_news/logs/`
  * `/dbt_news/target/`

### Key Commands

```bash
cd ~
python3 -m venv dbt_venv
source ~/dbt_venv/bin/activate

python -m pip install --upgrade pip
PIP_ONLY_FINAL=:all: python -m pip install "dbt-core>=1.10,<1.11" "dbt-postgres>=1.10,<1.11"

dbt --version
```

```bash
cd ~/airflow

mkdir -p ~/.dbt

cat > ~/.dbt/profiles.yml <<'YAML'
news_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST', '127.0.0.1') }}"
      user: "{{ env_var('DB_USER') }}"
      password: "{{ env_var('DB_PASSWORD') | string }}"
      port: "{{ env_var('DB_PORT', 5432) | int }}"
      dbname: "{{ env_var('DB_NAME') }}"
      schema: public
      threads: 4
      connect_timeout: 10
YAML
```

```bash
cd ~/airflow

set -a
source .env
set +a

export DB_HOST=127.0.0.1

dbt debug --project-dir dbt_news
```

### Verification Result

```text
dbt-core: 1.10.22
dbt-postgres: 1.10.0
profiles.yml: OK
dbt_project.yml: OK
Connection test: OK
All checks passed
```

### Notes

* 처음에는 Airflow venv에 dbt를 설치했으나 `dbt-core 2.0.0-alpha.1`이 설치되어 안정성 문제가 있었다.
* Airflow venv와 dbt venv를 분리하는 방향으로 수정했다.
* `~/.dbt/profiles.yml`과 `~/dbt_venv`는 로컬 실행 환경이므로 Git에 커밋하지 않는다.

### Commit

```bash
git add .gitignore dbt_news/dbt_project.yml
git commit -m "Add dbt project skeleton"
```

---

## Task 22. dbt staging 모델 생성

### Goal

PostgreSQL 원천 테이블을 dbt source로 등록하고, 분석용 staging view를 생성한다.

이번 단계에서는 mart 모델, dbt test, Airflow 연결은 추가하지 않는다.

### Source Tables

```text
public.news
public.validation_log
```

### Added Files

```text
dbt_news/models/staging/schema.yml
dbt_news/models/staging/stg_news.sql
dbt_news/models/staging/stg_validation_log.sql
```

### Changes

#### 1. Source definition

`schema.yml`에서 PostgreSQL의 `public.news`, `public.validation_log`를 dbt source로 등록했다.

```yaml
version: 2

sources:
  - name: raw
    schema: public
    tables:
      - name: news
      - name: validation_log
```

#### 2. `stg_news`

`news` 테이블을 분석용 staging view로 정리했다.

```sql
select
    id,
    title,
    content,
    published_at,
    cast(published_at as date) as published_date
from {{ source('raw', 'news') }}
```

역할:

```text
news 원천 테이블에서 필요한 컬럼을 가져오고,
published_at에서 날짜 단위 분석을 위한 published_date를 파생한다.
```

#### 3. `stg_validation_log`

`validation_log` 테이블을 DAG 실행별 품질 지표 분석용 staging view로 정리했다.

```sql
select
    dag_id,
    run_id,
    logical_date,
    total_rows,
    valid_rows,
    quarantine_rows,
    pandas_status,
    gx_status,
    overall_status,
    gx_unsuccessful_expectations,
    created_at
from {{ source('raw', 'validation_log') }}
```

역할:

```text
validation_log의 핵심 품질 지표를 staging 모델로 정리하고,
이후 mart_validation_quality 모델에서 valid_rate, quarantine_rate 계산에 사용한다.
```

### Key Commands

```bash
source ~/dbt_venv/bin/activate
cd ~/airflow

set -a
source .env
set +a

export DB_HOST=127.0.0.1
```

```bash
dbt parse --project-dir dbt_news
```

```bash
dbt compile --project-dir dbt_news --select stg_news stg_validation_log
```

```bash
dbt run --project-dir dbt_news --select stg_news stg_validation_log
```

### Verification Notes

* `dbt parse` 실행 시 `models.news_dbt.marts` unused configuration warning이 발생했다.
* 이는 아직 `models/marts/`에 mart 모델이 없기 때문에 발생한 warning이며, 현재 단계에서는 정상이다.
* 다음 단계에서 mart 모델을 추가하면 자연스럽게 해소될 수 있다.

### Expected dbt Objects

```text
public.stg_news
public.stg_validation_log
```

### Meaning

이번 단계로 PostgreSQL 원천 테이블을 바로 mart에서 사용하지 않고, dbt staging layer를 통해 한 번 정리하는 구조가 생겼다.

```text
news → stg_news
validation_log → stg_validation_log
```

이후 단계에서는 이 staging 모델을 기반으로 mart 모델을 생성한다.

### Commit

```bash
git add dbt_news/models/staging/schema.yml \
        dbt_news/models/staging/stg_news.sql \
        dbt_news/models/staging/stg_validation_log.sql

git commit -m "Add dbt staging models"
```



