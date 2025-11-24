from typing import Iterable, List, Optional
from pathlib import Path
import os


def _clean(value: Optional[str]) -> Optional[str]:
    """Entfernt Inline-Kommentare (nach '#') und trimmt Whitespace."""
    if value is None:
        return None
    return value.split("#", 1)[0].strip()

def get_str(key: str, default: Optional[str] = None) -> Optional[str]:
    raw = os.getenv(key)
    val = _clean(raw)
    return val if (val is not None and val != "") else default

def get_int(key: str, default: Optional[int] = None) -> int:
    val = get_str(key)
    return int(val) if val is not None else default

def get_float(key: str, default: Optional[float] = None) -> float:
    val = get_str(key)
    return float(val) if val is not None else default

def get_bool(key: str, default: Optional[bool] = None) -> bool:
    val = get_str(key)
    if val is None:
        return default
    return val.lower() in {"true", "1", "yes", "on", "y"}

def get_list(
    key: str,
    sep: str = ",",
    strip_items: bool = True,
    default: Optional[Iterable[str]] = ()
) -> List[str]:
    val = get_str(key)
    if val is None:
        return list(default)
    items = val.split(sep)
    if strip_items:
        items = [it.strip() for it in items]
    return [it for it in items if it != ""]

def get_path(key: str, default: Optional[str] = None) -> Path:
    raw = os.getenv(key)
    val = _clean(raw)

    path_str = val if (val is not None and val != "") else default
    if path_str is None:
        raise ValueError(f"Path for '{key}' is missing and no default provided.")

    return Path(path_str).expanduser().resolve()


def validate(SETTINGS) -> None:
        # Beispielhafte Minimal-Validierungen
    if not SETTINGS.URL_NYC_311.startswith("http") or not SETTINGS.URL_MEDIAN_RENT.startswith("http"):
        raise ValueError("URL_NYC_311 muss mit http/https beginnen.")
    if SETTINGS.DEFAULT_SINCE > SETTINGS.DEFAULT_UNTIL:
        raise ValueError("DEFAULT_SINCE darf nicht größer als DEFAULT_UNTIL sein.")
    if SETTINGS.TARGET_SAMPLE <= 0:
        raise ValueError("TARGET_SAMPLE muss > 0 sein.")
    if SETTINGS.MAX_RETRIES < 0:
        raise ValueError("MAX_RETRIES darf nicht negativ sein.")
    if SETTINGS.TIMEOUT <= 0:
        raise ValueError("TIMEOUT muss > 0 sein.")
    if SETTINGS.DAYS_IN_MONTH <= 0 or SETTINGS.DAYS_IN_MONTH > 31:
        raise ValueError("DAYS_IN_MONTH sollte zwischen 1 und 31 liegen.")