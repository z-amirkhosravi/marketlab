# config.py (or top of your script)
import os
import io, json, zipfile, re

from datetime import date, timedelta, datetime, timezone
from pathlib import Path

from urllib.parse import urljoin

import pandas as pd
import requests

import requests

import boto3
from botocore.exceptions import ClientError

from botocore.config import Config
import pandas as pd
import numpy as np

from marketlab.config import MarketlabConfig
from marketlab.data.arctic import get_arctic, get_lib

import arcticdb as adb
from dotenv import load_dotenv

load_dotenv()  # loads .env if present

# --- Massive / Polygon S3 config ---
MASSIVE_S3_ACCESS_KEY = os.environ["MASSIVE_S3_ACCESS_KEY"]
MASSIVE_S3_SECRET_KEY = os.environ["MASSIVE_S3_SECRET_KEY"]
MASSIVE_API_KEY = os.environ["MASSIVE_API_KEY"]

S3_ENDPOINT = "https://files.massive.com"
S3_BUCKET   = "flatfiles"
DAY_AGGS_PREFIX = "us_stocks_sip/day_aggs_v1"   # as per docs :contentReference[oaicite:3]{index=3}

# --- Local storage for flat files ---
LOCAL_BASE = Path("./massive_flatfiles/us_stocks_sip/day_aggs_v1")

# --- ArcticDB config ---
ARCTIC_URI  = "lmdb://./arcticdb_stock_store"
LIB_NAME    = "daily_ohlc_all"
SYMBOL_NAME = "us_stocks_day_aggs_v1"

from datetime import date, timedelta

MAX_YEARS_BACK = 5  # how many years the subscription allows

def compute_hist_start() -> date:
    """
    Compute the earliest date we should even *try* to download,
    based on how many years of history the account has.

    We use 366 days/year to be safe around leap years.
    """
    today = date.today()
    approx_days = MAX_YEARS_BACK * 366
    return today - timedelta(days=approx_days)

def get_s3_client(cfg: MarketlabConfig):
    session = boto3.Session(
        aws_access_key_id=cfg.massive_access_key,
        aws_secret_access_key=cfg.massive_secret_key,
    )
    s3 = session.client(
        "s3",
        endpoint_url=cfg.massive_endpoint,
        config=Config(signature_version="s3v4"),
    )
    return s3

def get_arctic_lib():
    ac = adb.Arctic(ARCTIC_URI)
    lib = ac.get_library(LIB_NAME, create_if_missing=True)
    return lib

def get_last_date_in_arctic(lib) -> date | None:
    if not lib.has_symbol(SYMBOL_NAME):
        return None

    df_tail = lib.tail(SYMBOL_NAME, 1).data
    if df_tail.empty:
        return None

    idx = df_tail.index[-1]
    # idx is (date, ticker) in a MultiIndex
    if isinstance(idx, tuple):
        last_date = pd.to_datetime(idx[0]).date()
    else:
        last_date = pd.to_datetime(idx).date()

    return last_date

def get_dates_to_update(lib, end_date: date) -> list[date]:
    """
    Return calendar dates we still need to download,
    from max(hist_start, last_seen+1) through end_date inclusive.
    """
    hist_start = compute_hist_start()
    last_date = get_last_date_in_arctic(lib)

    if last_date is None or last_date < hist_start:
        cur = hist_start
    else:
        cur = last_date + timedelta(days=1)

    if cur > end_date:
        return []

    dates = []
    while cur <= end_date:
        dates.append(cur)
        cur += timedelta(days=1)
    return dates

def s3_key_for_date(d: date) -> str:
    return f"{DAY_AGGS_PREFIX}/{d.year:04d}/{d.month:02d}/{d.isoformat()}.csv.gz"


def local_path_for_date(d: date) -> Path:
    return LOCAL_BASE / f"{d.year:04d}" / f"{d.month:02d}" / f"{d.isoformat()}.csv.gz"

def ensure_local_file_for_date(s3, bucket: str, d: date) -> Path | None:
    key = s3_key_for_date(d)
    local_path = local_path_for_date(d)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    if local_path.exists():
        print(f"{d}: using existing local file {local_path}")
        return local_path

    try:
        print(f"{d}: downloading {key} from S3 …")
        s3.download_file(bucket, key, str(local_path))
        return local_path
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        print(f"{d}: S3 error {code} while fetching {key}")
        # NoSuchKey/404 = weekend/holiday or not yet published.
        # 403 = outside your allowed history window.
        if code in ("NoSuchKey", "404", "403"):
            return None
        # Any other error is "real", so re-raise.
        raise


def update_all_days():

    # polygon/massive data
    cfg = MarketlabConfig()

    s3  = get_s3_client(cfg)
    arctic = get_arctic(cfg.arctic_uri)
    lib = get_lib(arctic, cfg.daily_lib)

    today = date.today()
    end_date = today - timedelta(days=1)

    dates = get_dates_to_update(lib, end_date)
    if not dates:
        print("No new files to download.")
        return

    print(f"Updating {len(dates)} day(s): {dates[0]} → {dates[-1]}")

    for d in dates:
        path = ensure_local_file_for_date(s3, d)
        if path is None:
            continue
        df = load_daily_df_from_file(path)
        append_daily_df_to_arctic(lib, df)


def _download_bytes(url: str, timeout: int = 60) -> bytes:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def _extract_first_csv_from_zip(zbytes: bytes) -> tuple[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise ValueError("No CSV found inside zip")
        name = names[0]
        return name, zf.read(name)


_DATE6 = re.compile(r"^\d{6}$")   # YYYYMM
_DATE8 = re.compile(r"^\d{8}$")   # YYYYMMDD

if __name__ == "__main__":
    update_all_days()

