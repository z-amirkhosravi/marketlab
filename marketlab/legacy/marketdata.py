# marketdata.py
from datetime import date
from dataclasses import dataclass

import requests

import os

import arcticdb as adb
import pandas as pd


from pathlib import Path
from datetime import datetime, timedelta, timezone

ARCTIC_URI  = "lmdb://./arcticdb_stock_store"
LIB_NAME    = "daily_ohlc_all"
SYMBOL_NAME = "us_stocks_day_aggs_v1"

BASE = "https://api.massive.com" 

EXCLUDE_SYMBOLS = {
    "ZVZZT",
    "ZTEST",
    "TEST",
    "TESTA",
    "TESTB",
}

SEC_CACHE_DIR = Path("sec_master_cache")
SEC_CACHE_DIR.mkdir(exist_ok=True)

MASSIVE_API_KEY='XXX'

_ac = None
_lib = None

def _get_lib():
    global _ac, _lib
    if _lib is None:
        _ac = adb.Arctic(ARCTIC_URI)
        _lib = _ac.get_library(LIB_NAME)
    return _lib

def get_all() -> pd.DataFrame:
    """Full daily dataset: MultiIndex (date, ticker)."""
    lib = _get_lib()
    return lib.read(SYMBOL_NAME).data

def get_all_filtered():
    df = get_all()
    tickers = df.index.get_level_values("ticker")
    return df[~tickers.isin(EXCLUDE_SYMBOLS)]

def get_ohlc(ticker: str, start: str | date | None = None,
             end: str | date | None = None) -> pd.DataFrame:
    """
    Return OHLCV for a single ticker, optional date range.
    Index: DatetimeIndex (date).
    """
    df_all = get_all()
    df = df_all.xs(ticker, level="ticker")  # select one ticker

    if start is not None or end is not None:
        df = df.loc[start:end]

    return df

def get_universe_on(date_str: str) -> pd.DataFrame:
    """
    All tickers for a given date.
    Index: ticker.
    """
    df_all = get_all()
    day = df_all.loc[date_str]   # slice MultiIndex on date
    return day.reset_index(level="ticker").set_index("ticker")

def build_features(df):
    # df is MultiIndex (date, ticker) with columns close, volume, ...
    close = df["close"].unstack("ticker").sort_index()
    vol   = df["volume"].unstack("ticker").sort_index()

    dollar_vol = close * vol
    adv20 = dollar_vol.rolling(20, min_periods=20).mean()

    ret = close.pct_change(fill_method=None)
    mom_12_1 = close.shift(21) / close.shift(252) - 1  # R(-252, -21)

    return {
        "close": close,
        "dollar_vol": dollar_vol,
        "adv20": adv20,
        "ret": ret,
        "mom_12_1": mom_12_1,
    }

@dataclass(frozen=True)
class DayFeatures:
    close: pd.Series
    adv20: pd.Series

def get_day_features(features, dt) -> DayFeatures:
    return DayFeatures(
        close=features["close"].loc[dt],
        adv20=features["adv20"].loc[dt],
    )

def tradable_universe(day: DayFeatures,
                      price_min=5.0,
                      adv20_min=1_000_000) -> pd.Series:
    """
    Returns a boolean Series indexed by ticker.
    """
    return (day.close >= price_min) & (day.adv20 >= adv20_min)

def download_tickers_for_date(dt, *, type_filter=None, api_key=MASSIVE_API_KEY):
    url = f"{BASE}/v3/reference/tickers"
    params = {
        "apiKey": api_key,
        "market": "stocks",
        "locale": "us",
        "date": dt,          # point-in-time universe :contentReference[oaicite:1]{index=1}
        "active": "true",    # actively traded on that date (default true) :contentReference[oaicite:2]{index=2}
        "limit": 1000,
    }
    if type_filter:
        params["type"] = type_filter  # e.g. "CS" :contentReference[oaicite:3]{index=3}

    rows = []
    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        rows.extend(j.get("results", []))
        nxt = j.get("next_url")       # pagination :contentReference[oaicite:4]{index=4}
        if not nxt:
            break
        url = nxt
        params = {"apiKey": api_key}  # next_url carries the other params

    sm = pd.DataFrame(rows)
    return sm

def get_allowed_common_stocks(dt) -> set[str]:
    p = SEC_CACHE_DIR / f"cs_{dt}.parquet"
    if p.exists():
        sm = pd.read_parquet(p)
    else:
        sm = download_tickers_for_date(dt, type_filter="CS")
        sm.to_parquet(p)

    # The endpoint returns 'ticker' in results; keep as a set
    return set(sm["ticker"].astype(str))

def get_security_master(
    cache_path="sec_master.parquet",
    max_age_days=7,
) -> pd.DataFrame:
    cache_path = Path(cache_path)

    def cache_is_fresh() -> bool:
        if not cache_path.exists():
            return False
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc)
        return (datetime.now(tz=timezone.utc) - mtime) < timedelta(days=max_age_days)

    if cache_is_fresh():
        if cache_path.suffix == ".parquet":
            return pd.read_parquet(cache_path)
        else:
            return pd.read_csv(cache_path, index_col=0)

    sm = download_security_master()  # your existing function
    # Keep only what you need + normalize
    sm = sm.copy()
    sm.index = sm.index.astype("object")
    if "type" in sm.columns:
        sm["type"] = sm["type"].astype("object")

    # Save
    if cache_path.suffix == ".parquet":
        sm.to_parquet(cache_path)
    else:
        sm.to_csv(cache_path)

    return sm
