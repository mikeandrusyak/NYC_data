from dataclasses import dataclass
from functools import lru_cache
from dotenv import load_dotenv
from pathlib import Path
from .utils import get_bool, get_float, get_int, get_list, get_str, get_path, validate


# .env laden (liegt standardmäßig neben config.py; passe an, wenn nötig)
ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=ENV_PATH if ENV_PATH.exists() else None, override=True)
PACKAGE_ROOT = Path(__file__).resolve().parent.parent.parent


# Default Constants 
DEFAULT_NYC_311_DATA_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
DEFAULT_MEDIAN_RENT_URL = "https://cdn-charts.streeteasy.com/rentals/All/medianAskingRent_All.zip?_ga=2.11354612.1524313251.1764007778-507322808.1759216457"
DEFAULT_SINCE_DEFAULT = 2024
DEFAULT_UNTIL_DEFAULT = 2025
TARGET_SAMPLE_DEFAULT = 10000
MAX_RETRIES_DEFAULT = 5
TIMEOUT_DEFAULT = 60
BASE_DELAY_DEFAULT = 2.0
GROUP_BY_DEFAULT = "borough"
GROUP_BY_VALUE_DEFAULT = ["BRONX","BROOKLYN","MANHATTAN","QUEENS","STATEN ISLAND","Unspecified"]
DEFAULT_SELECT_COLUMNS = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name", 
    "complaint_type", "descriptor", "location_type", "incident_zip", 
    "incident_address", "street_name", "cross_street_1", "cross_street_2",
    "intersection_street_1", "intersection_street_2", "address_type", "city", 
    "landmark", "facility_type", "status", "due_date", "resolution_description", 
    "resolution_action_updated_date", "community_board", "bbl", "borough", 
    "x_coordinate_state_plane", "y_coordinate_state_plane", "open_data_channel_type",
    "park_facility_name", "park_borough", "vehicle_type", "taxi_company_borough", 
    "taxi_pick_up_location", "bridge_highway_name", "bridge_highway_direction", 
    "road_ramp", "bridge_highway_segment", "latitude", "longitude", "location"
]
DAYS_IN_MONTH_DEFAULT = 16
SLEEP_FOR_SECONDS_DEFAULT = 0.3
PLOT_DIST_DEFAULT = False
BASE_DATA_PATH = PACKAGE_ROOT / "data"

@dataclass(frozen=True)
class Settings:
    URL_NYC_311: str
    URL_MEDIAN_RENT: str
    DEFAULT_SINCE: int
    DEFAULT_UNTIL: int
    TARGET_SAMPLE: int
    MAX_RETRIES: int
    TIMEOUT: float
    BASE_DELAY: float
    GROUP_BY: str
    GROUP_BY_VALUE: list[str]
    SELECTED_COLUMNS: list[str]
    DAYS_IN_MONTH: int
    SLEEP_FOR_SECONDS: float
    PLOT_DIST: bool
    BASE_DATA_PATH: str
        
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings(
        URL_NYC_311=get_str("BASE_URL", default=DEFAULT_NYC_311_DATA_URL),
        URL_MEDIAN_RENT=get_str("MEDIAN_RENT_URL", default=DEFAULT_MEDIAN_RENT_URL),
        DEFAULT_SINCE=get_int("DEFAULT_SINCE", default=DEFAULT_SINCE_DEFAULT),
        DEFAULT_UNTIL=get_int("DEFAULT_UNTIL", default=DEFAULT_UNTIL_DEFAULT),
        TARGET_SAMPLE=get_int("TARGET_SAMPLE", default=TARGET_SAMPLE_DEFAULT),
        MAX_RETRIES=get_int("MAX_RETRIES", default=MAX_RETRIES_DEFAULT),
        TIMEOUT=get_int("TIMEOUT", default=TIMEOUT_DEFAULT),
        BASE_DELAY=get_float("BASE_DELAY", default=BASE_DELAY_DEFAULT),
        GROUP_BY=get_str("GROUP_BY", default=GROUP_BY_DEFAULT),
        GROUP_BY_VALUE=get_list("GROUP_BY_VALUE", default=GROUP_BY_VALUE_DEFAULT),
        SELECTED_COLUMNS=get_list("SELECTED_COLUMNS", default=DEFAULT_SELECT_COLUMNS),
        DAYS_IN_MONTH=get_int("DAYS_IN_MONTH", default=DAYS_IN_MONTH_DEFAULT),
        SLEEP_FOR_SECONDS=get_float("SLEEP_FOR_SECONDS", default=SLEEP_FOR_SECONDS_DEFAULT),
        PLOT_DIST=get_bool("PLOT_DIST", default=PLOT_DIST_DEFAULT),
        BASE_DATA_PATH=get_path("BASE_DATA_PATH", default=str(BASE_DATA_PATH))
    )
    validate(settings)
    return settings