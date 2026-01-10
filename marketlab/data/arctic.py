# marketlab/data/arctic.py
from __future__ import annotations

import pandas as pd
from arcticdb import Arctic
import arcticdb as adb

from marketlab.config import MarketlabConfig

def get_arctic(cfg: MarketlabConfig) -> Arctic:
    return adb.Arctic(cfg.arctic_uri)

def get_lib(arctic: Arctic, lib_name: str):
    return arctic.get_library(lib_name, create_if_missing=True)

def key_bars(timeframe: str, symbol: str) -> str:
    # canonical key naming
    return f"bars/{timeframe}/{symbol}"

def write_bars(lib, timeframe: str, symbol: str, df: pd.DataFrame, upsert: bool = True) -> None:
    k = key_bars(timeframe, symbol)
    if upsert:
        lib.write(k, df, prune_previous_versions=True)
    else:
        lib.append(k, df)

def read_bars(lib, timeframe: str, symbol: str) -> pd.DataFrame:
    k = key_bars(timeframe, symbol)
    return lib.read(k).data
