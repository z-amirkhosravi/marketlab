# marketlab/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path



try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True), override=True)
except Exception:
    pass

@dataclass(frozen=True)
class MarketlabConfig:
    # ArcticDB
    arctic_uri: str = os.getenv("MARKETLAB_ARCTIC_URI", "lmdb://./arcticdb_stock_store")
    daily_lib: str = os.getenv("MARKETLAB_ARCTIC_LIB_DAILY", "daily_ohlc_all")
    daily_symbol_set: str = os.getenv("MARKETLAB_DAILY_SYMBOL_SET", "us_stocks_day_aggs_v1")

    massive_cache_dir: Path = Path(
        os.getenv("MARKETLAB_MASSIVE_CACHE_DIR", "./marketlab/data/polygon_massive/massive_flatfiles")
    ).resolve()

    daily_symbol_set: str = os.getenv("MARKETLAB_DAILY_SYMBOL", "us_stocks_sip/day_aggs_v1")

    # Massive S3 creds (DO NOT COMMIT)
    massive_access_key: str | None = os.getenv("MASSIVE_S3_ACCESS_KEY")
    massive_secret_key: str | None = os.getenv("MASSIVE_S3_SECRET_KEY")
    massive_api_key: str | None = os.getenv("MASSIVE_API_KEY")
    massive_endpoint: str = os.getenv("MASSIVE_S3_ENDPOINT", "https://files.massive.ca")
    massive_bucket: str = os.getenv("MASSIVE_S3_BUCKET", "flatfiles")

    # Local cache dirs
    cache_dir: Path = Path(os.getenv("MARKETLAB_CACHE_DIR", "./massive_flatfiles")).resolve()
    kenfrench_dir: Path = Path(os.getenv("MARKETLAB_KENFRENCH_DIR", "./factors/ken_french")).resolve()

    # Misc
    max_years_back: int = int(os.getenv("MARKETLAB_MAX_YEARS_BACK", "5"))

    def require_massive_api_key(self) -> str:
        if not self.massive_api_key:
            raise RuntimeError("Missing MASSIVE_API_KEY")
        return self.massive_api_key

    def require_massive_s3_creds(self) -> tuple[str, str]:
        if not self.massive_access_key or not self.massive_secret_key:
            raise RuntimeError("Missing MASSIVE_S3_ACCESS_KEY / MASSIVE_S3_SECRET_KEY")
        return self.massive_access_key, self.massive_secret_key