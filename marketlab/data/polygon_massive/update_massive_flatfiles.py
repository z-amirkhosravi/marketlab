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

def get_s3_client():
    session = boto3.Session(
        aws_access_key_id=MASSIVE_S3_ACCESS_KEY,
        aws_secret_access_key=MASSIVE_S3_SECRET_KEY,
    )
    s3 = session.client(
        "s3",
        endpoint_url="https://files.massive.com",
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

def ensure_local_file_for_date(s3, d: date) -> Path | None:
    key = s3_key_for_date(d)
    local_path = local_path_for_date(d)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    if local_path.exists():
        print(f"{d}: using existing local file {local_path}")
        return local_path

    try:
        print(f"{d}: downloading {key} from S3 …")
        s3.download_file(S3_BUCKET, key, str(local_path))
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

def load_daily_df_from_file(path):

    # Read ticker as string; don't auto-convert blanks/"NA"/etc. to NaN
    df = pd.read_csv(path, compression="gzip", keep_default_na=False)

    # needed because some objects are being read as string[python] which arcticdb rejects:
    df["ticker"] = df["ticker"].astype("object")
    # strip whitespace:
    df["ticker"] = df["ticker"].str.strip()

    # Drop rows with missing/blank ticker
    df = df[df["ticker"] != ""]

    # If you want: assert no blanks remain
    # assert (df["ticker"] != "").all()

    # Convert window_start (ns) to date
    df["date"] = pd.to_datetime(df["window_start"], unit="ns").dt.normalize()

    df.set_index(["date", "ticker"], inplace=True)
    df.sort_index(inplace=True)

    cols = ["open", "high", "low", "close", "volume", "transactions", "window_start"]
    df = df[[c for c in cols if c in df.columns]]

    return df

def arctic_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert pandas extension dtypes (especially string[python]/string[pyarrow])
    into types ArcticDB can normalize reliably.
    """
    out = df.copy()

    # Convert extension string dtype columns -> object
    for col in out.columns:
        if pd.api.types.is_string_dtype(out[col].dtype):
            out[col] = out[col].astype("object")

    # Also ensure index levels are not pandas StringDtype
    if isinstance(out.index, pd.MultiIndex):
        levels = []
        for lvl in out.index.levels:
            if hasattr(lvl, "dtype") and str(lvl.dtype).startswith("string"):
                levels.append(lvl.astype("object"))
            else:
                levels.append(lvl)
        # Rebuild MultiIndex with safe levels
        out.index = pd.MultiIndex.from_arrays(
            [out.index.get_level_values(i).astype("object") if str(out.index.get_level_values(i).dtype).startswith("string")
             else out.index.get_level_values(i)
             for i in range(out.index.nlevels)],
            names=out.index.names,
        )
    else:
        # Single index case
        if hasattr(out.index, "dtype") and str(out.index.dtype).startswith("string"):
            out.index = out.index.astype("object")

    return out

def append_daily_df_to_arctic(lib, df: pd.DataFrame):
    if df.empty:
        return
    
    df = arctic_safe(df)

    # Ensure index is MultiIndex / DatetimeIndex
    if not isinstance(df.index, pd.MultiIndex):
        raise ValueError("Expected MultiIndex (date, ticker) index.")

    # Write or append
    if lib.has_symbol(SYMBOL_NAME):
        lib.append(SYMBOL_NAME, df)
    else:
        lib.write(SYMBOL_NAME, df)

    # For logging
    last_idx = df.index[-1]
    last_date = pd.to_datetime(last_idx[0]).date() if isinstance(last_idx, tuple) else pd.to_datetime(last_idx).date()
    print(f"Appended {len(df)} rows, last date in chunk: {last_date}")

def update_all_days():
    # Ken French data:

    # try:
    #     update_ken_french_factors("factors/ken_french")
    #     print("Ken French factors updated.")
    # except Exception as e:
    #     print("WARNING: Failed to update Ken French factors:", e)

    # polygon/massive data
    s3  = get_s3_client()
    lib = get_arctic_lib()

    today = date.today()
    end_date = today - timedelta(days=1)

    dates = get_dates_to_update(lib, end_date)
    if not dates:
        print("ArcticDB is already up to date (or before hist_start).")
        return

    print(f"Updating {len(dates)} day(s): {dates[0]} → {dates[-1]}")

    for d in dates:
        path = ensure_local_file_for_date(s3, d)
        if path is None:
            continue
        df = load_daily_df_from_file(path)
        append_daily_df_to_arctic(lib, df)



# Ken French data downloader:

KF_LIBRARY_PAGE = (
    "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html"
)

TARGET_ZIPS = {
    "ff3_daily": {
        "filename": "F-F_Research_Data_Factors_daily_CSV.zip",
        "header_tokens": {"Mkt-RF"},
        "parquet": "ff3_daily.parquet",
    },
    "mom_daily": {
        "filename": "F-F_Momentum_Factor_daily_CSV.zip",
        "header_tokens": {"Mom", "UMD"},
        "parquet": "mom_daily.parquet",
    },
}


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


def _discover_zip_urls() -> dict[str, str]:
    """
    Scrape Ken French data library page and locate the ZIP URLs we need.
    """
    html = _download_bytes(KF_LIBRARY_PAGE).decode("utf-8", errors="replace")
    hrefs = re.findall(r'href\s*=\s*"([^"]+)"', html, flags=re.IGNORECASE)

    found = {}
    for key, spec in TARGET_ZIPS.items():
        fname = spec["filename"]
        match = next((h for h in hrefs if fname in h), None)
        if match is None:
            raise RuntimeError(f"Could not find link for {fname}")
        found[key] = urljoin(KF_LIBRARY_PAGE, match)

    return found


def update_ken_french_factors(out_dir: str = "factors/ken_french") -> dict:
    """
    Download, parse, and store FF3 daily and Momentum daily factors.

    Uses robust header-token parsing and date filtering.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    urls = _discover_zip_urls()

    meta = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "library_page": KF_LIBRARY_PAGE,
        "sources": {},
    }

    results = {}

    for key, spec in TARGET_ZIPS.items():
        url = urls[key]
        zip_name = spec["filename"]

        zpath = out / zip_name
        cpath = out / zip_name.replace(".zip", ".csv")
        ppath = out / spec["parquet"]

        # Download ZIP
        zbytes = _download_bytes(url)
        zpath.write_bytes(zbytes)

        # Extract CSV
        csv_inside, csv_bytes = _extract_first_csv_from_zip(zbytes)
        cpath.write_bytes(csv_bytes)

        # Parse via robust parser
        df = parse_kf_csv(
            csv_bytes,
            header_tokens=spec["header_tokens"],
        )

        df.to_parquet(ppath)
        results[key] = df

        meta["sources"][key] = {
            "url": url,
            "zip_saved_as": str(zpath),
            "csv_inside_zip": csv_inside,
            "csv_saved_as": str(cpath),
            "parquet_saved_as": str(ppath),
            "rows": int(len(df)),
            "columns": list(df.columns),
            "start": df.index.min().date().isoformat() if len(df) else None,
            "end": df.index.max().date().isoformat() if len(df) else None,
        }

    (out / "metadata.json").write_text(json.dumps(meta, indent=2))
    return {"data": results, "meta": meta}


# import re
# from urllib.parse import urljoin

# KF_LIBRARY_PAGE = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html"

# # What we want to download (by filename fragment)
# TARGET_ZIPS = {
#     "ff3_daily": "F-F_Research_Data_Factors_daily_CSV.zip",
#     "mom_daily": "F-F_Momentum_Factor_daily_CSV.zip",
# }

# def _download_bytes(url: str, timeout: int = 60) -> bytes:
#     r = requests.get(url, timeout=timeout)
#     r.raise_for_status()
#     return r.content

# def _extract_first_csv_from_zip(zbytes: bytes) -> tuple[str, bytes]:
#     with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
#         names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
#         if not names:
#             raise ValueError("No CSV found inside zip")
#         name = names[0]
#         return name, zf.read(name)

_DATE6 = re.compile(r"^\d{6}$")   # YYYYMM
_DATE8 = re.compile(r"^\d{8}$")   # YYYYMMDD

def parse_kf_csv(csv_bytes: bytes, header_tokens: set[str]) -> pd.DataFrame:
    """
    Parse Ken French CSV (daily or monthly) robustly.

    - Finds header row by presence of any token in header_tokens (e.g. "Mkt-RF" or "Mom")
    - Filters out footer lines by keeping only rows where first column is YYYYMM or YYYYMMDD
    - Converts percent -> decimal
    - Converts -99.99 / -999 (and variants) to NaN
    """
    text = csv_bytes.decode("latin-1", errors="replace")
    lines = text.splitlines()

    header_i = None
    for i, ln in enumerate(lines):
        if any(tok in ln for tok in header_tokens):
            header_i = i
            break
    if header_i is None:
        raise ValueError(f"Could not find header row containing any of: {sorted(header_tokens)}")

    data_text = "\n".join(lines[header_i:])
    df = pd.read_csv(io.StringIO(data_text))

    date_col = df.columns[0]
    s = df[date_col].astype(str).str.strip()

    is_date = s.map(lambda x: bool(_DATE6.match(x) or _DATE8.match(x)))
    df = df[is_date].copy()
    s = s[is_date]

    # Parse index (daily vs monthly)
    if (s.str.len() == 6).all():
        idx = pd.to_datetime(s, format="%Y%m")
    else:
        # daily
        idx = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
        # (Shouldn't be needed for MOM, but safe)
        if idx.isna().any():
            idx2 = pd.to_datetime(s, format="%Y%m", errors="coerce")
            idx = idx.fillna(idx2)

    df.index = idx
    df = df.drop(columns=[date_col]).sort_index()

    # Convert all columns to numeric; interpret sentinels as NaN; percent -> decimal
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Replace KF missing sentinels (documented in MOM header)
    df = df.replace({-99.99: np.nan, -999: np.nan, -999.0: np.nan, -99.99: np.nan})

    # Percent -> decimal
    df = df / 100.0

    # Drop rows that are all NaN after cleaning
    df = df.dropna(how="all")

    return df


if __name__ == "__main__":
    update_all_days()

