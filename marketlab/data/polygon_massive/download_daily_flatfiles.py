from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import shutil

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from marketlab.config import MarketlabConfig


_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})\.csv\.gz$")


def local_path_for_date(cfg: MarketlabConfig, day: dt.date) -> Path:
    return (
        cfg.massive_cache_dir
        / cfg.daily_symbol_set
        / f"{day.year:04d}"
        / f"{day.month:02d}"
        / f"{day:%Y-%m-%d}.csv.gz"
    )


def s3_key_for_date(cfg: MarketlabConfig, day: dt.date) -> str:
    # Matches your local folder structure and the most common Massive layout
    return f"{cfg.daily_symbol_set}/{day.year:04d}/{day.month:02d}/{day:%Y-%m-%d}.csv.gz"


def find_latest_local_date(cfg: MarketlabConfig) -> dt.date | None:
    """
    Finds the max date present under:
      {massive_cache_dir}/{daily_symbol_set}/YYYY/MM/YYYY-MM-DD.csv.gz
    Efficient enough for daily use.
    """
    root = cfg.massive_cache_dir / cfg.daily_symbol_set
    if not root.exists():
        return None

    latest: dt.date | None = None

    # Walk year/month directories; this avoids scanning the whole tree if it’s big.
    # If you have a TON of data, you can limit to last 2 years, but usually fine.
    for year_dir in sorted([p for p in root.iterdir() if p.is_dir() and p.name.isdigit()]):
        for month_dir in sorted([p for p in year_dir.iterdir() if p.is_dir() and p.name.isdigit()]):
            for f in month_dir.glob("*.csv.gz"):
                m = _DATE_RE.search(f.name)
                if not m:
                    continue
                y, mo, d = map(int, m.groups())
                try:
                    day = dt.date(y, mo, d)
                except ValueError:
                    continue
                if latest is None or day > latest:
                    latest = day

    return latest


def iter_dates(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def make_s3_client(cfg: MarketlabConfig):
    access_key, secret_key = cfg.require_massive_s3_creds()
    # Most S3-compatible providers need signature_v4 and path-style may matter.
    botocfg = Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.massive_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=botocfg,
        region_name="us-east-1",  # harmless default that fixes some providers
    )
    return s3

@dataclass(frozen=True)
class DownloadResult:
    checked: int
    downloaded: int
    skipped_existing: int
    missing_remote: int
    downloaded_days: list[dt.date]

MISSING_CODES = {"NoSuchKey", "404", "NotFound", "403", "AccessDenied"}

def object_exists_via_list(s3, bucket: str, key: str) -> bool:
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return any(obj["Key"] == key for obj in resp.get("Contents", []))

def try_download_day(s3, bucket: str, key: str, local_path: Path) -> bool:
    """
    Returns True if downloaded, False if not available / not allowed / not published yet.
    Avoids download_file() because it does HeadObject.
    """
    # First check existence cheaply (LIST works for you)
    if not object_exists_via_list(s3, bucket, key):
        return False

    local_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = local_path.with_suffix(local_path.suffix + ".partial")

    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]  # StreamingBody
        with tmp.open("wb") as f:
            shutil.copyfileobj(body, f)
        tmp.replace(local_path)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in MISSING_CODES:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            return False
        raise

def download_missing_range(
    cfg: MarketlabConfig,
    *,
    bucket: str,
    start: dt.date,
    end: dt.date,
    overwrite: bool = False,
) -> DownloadResult:
    s3 = make_s3_client(cfg)

    checked = downloaded = skipped_existing = missing_remote = 0
    downloaded_days: list[dt.date] = []

    for day in iter_dates(start, end):
        checked += 1

        local_path = local_path_for_date(cfg, day)
        if local_path.exists() and not overwrite:
            skipped_existing += 1
            continue

        key = s3_key_for_date(cfg, day)
        ok = try_download_day(s3, bucket, key, local_path)

        if ok:
            downloaded += 1
            downloaded_days.append(day)
        else:
            missing_remote += 1

    return DownloadResult(
        checked=checked,
        downloaded=downloaded,
        skipped_existing=skipped_existing,
        missing_remote=missing_remote,
        downloaded_days=downloaded_days
    )


def update_to_latest_available(
    cfg: MarketlabConfig,
    *,
    lookback_days: int = 10,
    overwrite: bool = False,
) -> DownloadResult:
    """
    Daily “update” behavior:
    - finds latest local date
    - checks a small window forward (and a short lookback in case of gaps)
    - downloads whatever exists remotely and is missing locally
    """
    latest = find_latest_local_date(cfg)
    today = dt.date.today()

    if latest is None:
        # If you have nothing locally, default to cfg.max_years_back
        start = today - dt.timedelta(days=cfg.max_years_back * 366)
    else:
        # Check from (latest - lookback) to today, so gaps get filled
        start = latest - dt.timedelta(days=lookback_days)

    end = today

    return download_missing_range(cfg, bucket=cfg.massive_bucket, start=start, end=end, overwrite=overwrite)
