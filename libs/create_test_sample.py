import pandas as pd
from pathlib import Path
from libs_tidy.tidying import prepare_date_time

def create_test_sample(
        data_path: str,
        output_path: str,
        sample_size: int = 20_000,
        filter_year: int = 2024,
):
    data_path = Path(data_path)
    output_path = Path(output_path)

    df = pd.read_csv(data_path)

    df = prepare_date_time(df)

    df_year = df[df["created_date"].dt.year == filter_year]
    relevant_cols = ["created_date", "create_day", "create_month"]
    df_year = df_year[relevant_cols]

    n = min(sample_size, len(df_year))
    sample = df_year.sample(n=n, random_state=42)

    sample.to_parquet(output_path, index=False)

    return sample