import argparse
import json
import os

import pandas as pd


REQUIRED_COLUMNS = [
    "title",
    "content",
    "publishedAt",
    "url",
    "source",
    "description",
]


def is_missing_or_blank(value):
    if pd.isna(value):
        return True

    return str(value).strip() == ""


def get_text_length(value):
    if pd.isna(value):
        return 0

    return len(str(value).strip())


def validate_row(row):
    fail_reasons = []

    if is_missing_or_blank(row["title"]):
        fail_reasons.append("missing_title")

    if is_missing_or_blank(row["url"]):
        fail_reasons.append("missing_url")
    else:
        url = str(row["url"]).strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            fail_reasons.append("invalid_url_format")

    if is_missing_or_blank(row["source"]):
        fail_reasons.append("missing_source")

    if is_missing_or_blank(row["publishedAt"]):
        fail_reasons.append("missing_publishedAt")
    else:
        published_at = pd.to_datetime(row["publishedAt"], errors="coerce", utc=True)
        if pd.isna(published_at):
            fail_reasons.append("invalid_publishedAt")
        else:
            now = pd.Timestamp.now(tz="UTC")
            if published_at > now:
                fail_reasons.append("future_publishedAt")

    content_length = get_text_length(row["content"])
    description_length = get_text_length(row["description"])
    if content_length < 20 and description_length < 10:
        fail_reasons.append("short_text")

    return ";".join(fail_reasons)


def count_fail_reasons(fail_reason_series):
    reason_counts = {}

    for fail_reason in fail_reason_series:
        if is_missing_or_blank(fail_reason):
            continue

        reasons = str(fail_reason).split(";")
        for reason in reasons:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    return reason_counts


def get_input_path(cli_input):
    if cli_input:
        return cli_input

    data_dir = os.getenv("DATA_DIR", "/tmp")
    return os.path.join(data_dir, "preprocessed_news.csv")


def get_output_dir(cli_output_dir):
    if cli_output_dir:
        return cli_output_dir

    data_dir = os.getenv("DATA_DIR", "/tmp")
    return os.path.join(data_dir, "validation_results")


def save_validation_outputs(
    valid_df,
    quarantine_df,
    summary,
    output_dir,
):
    os.makedirs(output_dir, exist_ok=True)

    valid_output_path = os.path.join(output_dir, "valid_news.csv")
    quarantine_output_path = os.path.join(output_dir, "quarantine_news.csv")
    summary_output_path = os.path.join(output_dir, "validation_summary.json")

    valid_df.to_csv(valid_output_path, index=False)
    quarantine_df.to_csv(quarantine_output_path, index=False)

    with open(summary_output_path, "w", encoding="utf-8") as summary_file:
        json.dump(summary, summary_file, indent=2)

    print("Validation output files")
    print(f"valid news: {valid_output_path}")
    print(f"quarantine news: {quarantine_output_path}")
    print(f"validation summary: {summary_output_path}")


def validate_news_data(input_path, output_dir):
    if not os.path.exists(input_path):
        raise Exception(f"Input file does not exist: {input_path}")

    df = pd.read_csv(input_path)
    columns = list(df.columns)
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in columns]
    row_count = len(df)

    status = "PASSED"
    if missing_columns or row_count == 0:
        status = "FAILED"

    print("Validation summary")
    print(f"input path: {input_path}")
    print(f"row count: {row_count}")
    print(f"column list: {columns}")
    print(f"missing columns: {missing_columns}")
    print(f"status: {status}")

    if status == "FAILED":
        raise Exception("News data validation failed.")

    df["fail_reason"] = df.apply(validate_row, axis=1)
    valid_df = df[df["fail_reason"] == ""].copy()
    quarantine_df = df[df["fail_reason"] != ""].copy()
    valid_rows = len(valid_df)
    quarantine_rows = len(quarantine_df)
    fail_reason_counts = count_fail_reasons(df["fail_reason"])
    row_status = "PASSED"
    if valid_rows == 0:
        row_status = "FAILED"

    print("Row validation summary")
    print(f"total rows: {row_count}")
    print(f"valid rows: {valid_rows}")
    print(f"quarantine rows: {quarantine_rows}")
    print(f"fail_reason counts: {fail_reason_counts}")
    print(f"status: {row_status}")

    summary = {
        "input_path": input_path,
        "output_dir": output_dir,
        "total_rows": row_count,
        "valid_rows": valid_rows,
        "quarantine_rows": quarantine_rows,
        "fail_reason_counts": fail_reason_counts,
        "status": row_status,
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }

    save_validation_outputs(valid_df, quarantine_df, summary, output_dir)

    if valid_rows == 0:
        raise Exception("News data row validation failed: all rows are invalid.")


def main():
    parser = argparse.ArgumentParser(description="Validate preprocessed news CSV data.")
    parser.add_argument("--input", help="Path to preprocessed_news.csv")
    parser.add_argument("--output-dir", help="Directory for validation output files")
    args = parser.parse_args()

    input_path = get_input_path(args.input)
    output_dir = get_output_dir(args.output_dir)
    validate_news_data(input_path, output_dir)


if __name__ == "__main__":
    main()
