import os
from pathlib import Path

from .libs.daw.pipeline import NYCDataPipeline

from .config import Settings, get_settings
from .libs.import_data.fetcher import get_dataset_stratified, save_dataset, get_median_rent_data
from .libs.import_data.utils import generate_month_ranges, generate_quarters

def cleanup_data_files(data_path: Path, run_import: bool) -> None:
    source_data = [
            "nyc_311_2024_2025_sample.csv",
            "medianAskingRent_All.csv"
        ]
    if  run_import:
        for csv_file in data_path.glob("*.csv"):
            os.remove(csv_file)
            print(f"Removed existing CSV file: {csv_file}")
    else:
        for csv_file in data_path.glob("*.csv"):
            if csv_file.name not in source_data:
                os.remove(csv_file)
                print(f"Removed existing CSV file: {csv_file}")

def ask_yes_no(prompt: str) -> bool:
    while True:
        answer = input(f"{prompt} [y/n]: ").strip().lower()
        if answer in ("y", "yes", "j", "ja"):
            return True
        if answer in ("n", "no", "nein"):
            return False
        print("Please enter only 'y' or 'n'.")


def run_import_pipe(SETTINGS: Settings) -> None:
    print("Running Import pipeline...")
    # 1. download median rent data
    get_median_rent_data(SETTINGS)
    # 2. generate date ranges
    quarters = generate_quarters(SETTINGS.DEFAULT_SINCE, SETTINGS.DEFAULT_UNTIL)
    months = generate_month_ranges(quarters)
    # 3. fetch the data 
    df_all_calls = get_dataset_stratified(months, SETTINGS, SETTINGS.SELECTED_COLUMNS)
    # 4. save the data
    save_dataset(df_all_calls, SETTINGS.BASE_DATA_PATH / "nyc_311_2024_2025_sample.csv")

def run_daw_pipe() -> None:
    print("Running DAW pipeline...")
    pipeline = NYCDataPipeline()
    # Load data
    df_nyc_311, df_median_rent, uhf_data, manual_map = pipeline.load_data()
    # Clean data
    df_nyc_311_cleaned, df_median_rent_cleaned = pipeline.clean_data(
    df_nyc_311, df_median_rent
    )

    # Transform data and create new features
    df_nyc_311_transformed, df_median_rent_transformed = pipeline.transform_data(
    df_nyc_311_cleaned, df_median_rent_cleaned, uhf_data, manual_map
    )

    final_dataset = pipeline.aggregate_and_integrate(
    df_nyc_311_transformed, df_median_rent_transformed
    )

    print(f"\nFinal dataset created: {final_dataset.shape}")

def main() -> None:
    SETTINGS: Settings = get_settings()
    run_import = ask_yes_no("Do you want to refresh source data?")
    cleanup_data_files(SETTINGS.BASE_DATA_PATH, run_import)
    if run_import:
        run_import_pipe(SETTINGS)
    run_daw_pipe()
    
if __name__ == "__main__":
    main()