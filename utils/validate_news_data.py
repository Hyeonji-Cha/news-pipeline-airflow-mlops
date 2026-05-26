import argparse
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


def get_input_path(cli_input):
    if cli_input:
        return cli_input

    data_dir = os.getenv("DATA_DIR", "/tmp")
    return os.path.join(data_dir, "preprocessed_news.csv")


def validate_news_data(input_path):
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


def main():
    parser = argparse.ArgumentParser(description="Validate preprocessed news CSV data.")
    parser.add_argument("--input", help="Path to preprocessed_news.csv")
    args = parser.parse_args()

    input_path = get_input_path(args.input)
    validate_news_data(input_path)


if __name__ == "__main__":
    main()
