from __future__ import annotations
import argparse
import datetime as dt

from marketlab.config import MarketlabConfig
from marketlab.data.polygon_massive.ingest_daily_from_cache import ingest_day

def parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

def daterange(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--rewrite", action="store_true", help="rewrite symbols instead of append")
    args = p.parse_args()

    cfg = MarketlabConfig()
    start = parse_date(args.start)
    end = parse_date(args.end)

    

    for day in daterange(start, end):
        info = ingest_day(cfg, day, append=not args.rewrite)
        print(info)

if __name__ == "__main__":
    main()