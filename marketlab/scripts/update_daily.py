from __future__ import annotations
import argparse
import datetime as dt

from marketlab.config import MarketlabConfig
from marketlab.data.polygon_massive.download_daily_flatfiles import update_to_latest_available, find_latest_local_date, iter_dates
from marketlab.data.polygon_massive.ingest_daily_from_cache import ingest_day, flatfile_path

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lookback-days", type=int, default=10)
    args = p.parse_args()

    cfg = MarketlabConfig()

    # 1) download missing
    dl = update_to_latest_available(cfg, lookback_days=args.lookback_days)
    print("download:", dl)

    # 2) ingest anything local in the window that isn’t ingested yet
    latest = find_latest_local_date(cfg)
    today = dt.date.today()
    if latest is None:
        start = today - dt.timedelta(days=cfg.max_years_back * 366)
    else:
        start = latest - dt.timedelta(days=args.lookback_days)
    end = today

    for day in iter_dates(start, end):
        path = flatfile_path(cfg, day)
        if not path.exists():
            continue
        # ingest_day will skip instantly if manifest says already ingested
        info = ingest_day(cfg, day, append=True)
        if info.get("skipped"):
            continue
        print("ingest:", info)

    # from marketlab.data.arctic import get_arctic, get_lib, read_bars
    # lib = get_lib(get_arctic(cfg.arctic_uri), cfg.daily_lib)
    # spy = read_bars(lib, "1d", "SPY")
    # print("SPY last:", spy.index.max(), spy["close"].iloc[-1])

if __name__ == "__main__":
    main()
