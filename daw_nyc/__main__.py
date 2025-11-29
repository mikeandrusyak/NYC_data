import os
import argparse
import sys
from pathlib import Path

from .libs.daw.pipeline import NYCDataPipeline

from .config import Settings, get_settings
from .libs.import_data.fetcher import get_dataset_stratified, save_dataset, get_median_rent_data
from .libs.import_data.utils import generate_month_ranges, generate_quarters

__version__ = "0.1.0"

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


def run_import_pipe(settings: Settings, verbose: bool = False) -> None:
    """Run import pipeline to fetch fresh data."""
    if verbose:
        print("=" * 60)
        print("IMPORT PIPELINE")
        print("=" * 60)
    else:
        print("Running Import pipeline...")
    
    # 1. download median rent data
    if verbose:
        print("\n📥 Downloading median rent data...")
    get_median_rent_data(settings)
    
    # 2. generate date ranges
    if verbose:
        print(f"\n📅 Generating date ranges ({settings.DEFAULT_SINCE}-{settings.DEFAULT_UNTIL})...")
    quarters = generate_quarters(settings.DEFAULT_SINCE, settings.DEFAULT_UNTIL)
    months = generate_month_ranges(quarters)
    
    # 3. fetch the data 
    if verbose:
        print(f"\n🔍 Fetching NYC 311 data (target: {settings.TARGET_SAMPLE} per month)...")
    df_all_calls = get_dataset_stratified(months, settings, settings.SELECTED_COLUMNS)
    
    # 4. save the data
    output_path = settings.BASE_DATA_PATH / "nyc_311_2024_2025_sample.csv"
    if verbose:
        print(f"\n💾 Saving to {output_path}...")
    save_dataset(df_all_calls, output_path)
    
    if verbose:
        print(f"\n✅ Import complete: {len(df_all_calls)} records")

def run_daw_pipe(verbose: bool = False) -> None:
    """Run data wrangling pipeline."""
    if verbose:
        print("\n" + "=" * 60)
        print("DATA WRANGLING PIPELINE")
        print("=" * 60)
    else:
        print("Running DAW pipeline...")
    
    pipeline = NYCDataPipeline()
    
    # Load data
    if verbose:
        print("\n📂 Loading data...")
    df_nyc_311, df_median_rent, uhf_data, manual_map = pipeline.load_data()
    
    # Clean data
    if verbose:
        print("\n🧹 Cleaning data...")
    df_nyc_311_cleaned, df_median_rent_cleaned = pipeline.clean_data(
        df_nyc_311, df_median_rent
    )
    
    # Transform data and create new features
    if verbose:
        print("\n⚙️  Transforming data...")
    df_nyc_311_transformed, df_median_rent_transformed = pipeline.transform_data(
        df_nyc_311_cleaned, df_median_rent_cleaned, uhf_data, manual_map
    )
    
    # Aggregate and integrate
    if verbose:
        print("\n🔗 Aggregating and integrating datasets...")
    final_dataset = pipeline.aggregate_and_integrate(
        df_nyc_311_transformed, df_median_rent_transformed
    )
    
    if verbose:
        print(f"\n✅ Final dataset created: {final_dataset.shape}")

def main() -> None:
    """NYC Data Analysis & Wrangling Pipeline CLI."""
    parser = argparse.ArgumentParser(
        prog='daw-run',
        description='NYC Data Analysis & Wrangling Pipeline - Process NYC 311 and rent data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  daw-run                              # Interactive mode
  daw-run --import                     # Run import + processing
  daw-run --skip-import                # Skip import, process existing data
  daw-run --import-only                # Only download data
  daw-run --import --verbose           # Verbose output
  daw-run --import --target-sample 5000  # Custom sample size
  daw-run --since 2023 --until 2024    # Custom date range

For more information, see: README.md
        """
    )
    
    # Pipeline control
    pipeline_group = parser.add_mutually_exclusive_group()
    pipeline_group.add_argument(
        '--import',
        dest='run_import',
        action='store_true',
        help='Run import pipeline to fetch fresh data from APIs'
    )
    pipeline_group.add_argument(
        '--skip-import',
        action='store_true',
        help='Skip import pipeline, use existing data files'
    )
    parser.add_argument(
        '--import-only',
        action='store_true',
        help='Only run import pipeline, skip data processing'
    )
    
    # Data configuration
    parser.add_argument(
        '--target-sample',
        type=int,
        metavar='N',
        help='Override target sample size per month (default: from config)'
    )
    parser.add_argument(
        '--since',
        type=int,
        metavar='YEAR',
        help='Start year for data collection (default: from config)'
    )
    parser.add_argument(
        '--until',
        type=int,
        metavar='YEAR',
        help='End year for data collection (default: from config)'
    )
    
    # Behavior options
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output with detailed progress information'
    )
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Suppress non-essential output'
    )
    
    # Version
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {__version__}'
    )
    
    args = parser.parse_args()
    
    # Handle quiet mode
    if args.quiet and args.verbose:
        parser.error("--quiet and --verbose are mutually exclusive")
    
    # Load settings
    settings = get_settings()
    
    # Override settings from command line arguments
    if args.target_sample:
        os.environ['TARGET_SAMPLE'] = str(args.target_sample)
        settings = get_settings()  # Reload with new env var
    
    if args.since:
        os.environ['DEFAULT_SINCE'] = str(args.since)
        settings = get_settings()
    
    if args.until:
        os.environ['DEFAULT_UNTIL'] = str(args.until)
        settings = get_settings()
    
    # Determine what to run
    if args.skip_import:
        should_import = False
    elif args.run_import or args.import_only:
        should_import = True
    else:
        # Interactive mode (default)
        should_import = ask_yes_no("Do you want to refresh source data?")
    
    # Clean up old files if needed
    if not args.quiet:
        cleanup_data_files(settings.BASE_DATA_PATH, should_import)
    
    # Execute import pipeline
    if should_import:
        try:
            run_import_pipe(settings, verbose=args.verbose)
        except Exception as e:
            print(f"\n❌ Import pipeline failed: {e}", file=sys.stderr)
            if args.verbose:
                raise
            sys.exit(1)
    
    # Execute DAW pipeline (unless import-only)
    if not args.import_only:
        try:
            run_daw_pipe(verbose=args.verbose)
        except Exception as e:
            print(f"\n❌ DAW pipeline failed: {e}", file=sys.stderr)
            if args.verbose:
                raise
            sys.exit(1)
    
    if not args.quiet:
        print("\n" + "=" * 60)
        print("✅ PIPELINE COMPLETE")
        print("=" * 60)

if __name__ == "__main__":
    main()