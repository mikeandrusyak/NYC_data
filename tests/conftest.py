import pytest
import pandas as pd
from pathlib import Path


@pytest.fixture
def df():
    # Pfad zum Sample aus Git (klein & committed!)
    project_root = Path(__file__).resolve().parents[1]
    sample_path = project_root / "data" / "sample-test.parquet"

    if not sample_path.exists():
        raise FileNotFoundError(
            f"Sample file not found: {sample_path}. "
            "Generate it with create_test_sample() first."
        )

    df = pd.read_parquet(sample_path)

    # Sicherstellen, dass created_date richtig ist:
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")

    return df