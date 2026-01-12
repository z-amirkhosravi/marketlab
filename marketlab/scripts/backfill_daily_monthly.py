from __future__ import annotations

import argparse
import datetime as dt
from collections import defaultdict

import pandas as pd

from marketlab.config import MarketlabConfig
from marketlab.data.arctic import get_arctic, get_lib, key_bars
from marketlab.data.polygon_massive.ingest_daily_from_cache import flatfile_path, is_day_ingested, mark_day_ingested

def parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

def month_range(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    while cur <= end:
        yield cur.year, cur.month
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)

def days_in_month(year: int, month: int, start: dt.date, end: dt.date):
    first = dt.date(year, month, 1)
    nextm = dt.date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    last = nextm - dt.timedelta(days=1)

    lo = max(first, start)
    hi = min(last, end)

    d = lo
    while d <= hi:
        yield d
        d += dt.timedelta(days=1)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = p.parse_args()

    cfg = MarketlabConfig()
    start = parse_date(args.start)
    end = parse_date(args.end)

    arctic = get_arctic(cfg.arctic_uri)
    lib = get_lib(arctic, cfg.daily_lib)

    for year, month in month_range(start, end):
        # symbol -> list of rows
        rows = defaultdict(list)
        days_buffered = []
        days_found = 0
        total_rows_read = 0

        for day in days_in_month(year, month, start, end):
            path = flatfile_path(cfg, day)
            if not path.exists():
                continue
            days_found += 1

            if is_day_ingested(lib, cfg, day):
                continue

            df = pd.read_csv(path, compression="gzip")

            required = {"ticker", "volume", "open", "close", "high", "low", "window_start"}
            missing = required - set(df.columns)
            if missing:
                raise ValueError(f"Missing columns {sorted(missing)} in {path}")
            
            ts = pd.to_datetime(df["window_start"], unit="ns", utc=True).dt.tz_convert("UTC")
            df = df.assign(ts=ts)

            # unpack by position (fast + robust)
            for ticker, volume, open_, close, high, low, window_start, transactions, ts_val in df[
                ["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions", "ts"]
                ].itertuples(index=False, name=None):
                rows[ticker].append((ts_val, open_, high, low, close, volume))

            # ts = pd.to_datetime(df["window_start"], unit="ns", utc=True)
            # df = df.assign(_ts=ts)

            # for r in df.itertuples(index=False):
            #     rows[r.ticker].append((r._ts, r.open, r.high, r.low, r.close, r.volume))

            total_rows_read += len(df)
            days_buffered.append(day)

        if not days_buffered:
            print({"month": f"{year:04d}-{month:02d}", "days_found": days_found, "skipped": True})
            continue

        # write per symbol once
        symbols_written = 0
        rows_appended = 0
        for sym, lst in rows.items():
            if not lst:
                continue
            out = pd.DataFrame(lst, columns=["timestamp", "open", "high", "low", "close", "volume"])
            out = out.set_index("timestamp").sort_index()

            lib.append(key_bars("1d", sym), out)
            symbols_written += 1
            rows_appended += len(out)

        # mark manifest after successful writes
        for d in days_buffered:
            mark_day_ingested(lib, cfg, d)

        print({
            "month": f"{year:04d}-{month:02d}",
            "days_found": days_found,
            "days_ingested": len(days_buffered),
            "symbols_written": symbols_written,
            "rows_read": total_rows_read,
            "rows_appended": rows_appended,
        })

if __name__ == "__main__":
    main()
