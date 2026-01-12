from __future__ import annotations
import argparse

from marketlab.config import MarketlabConfig
from marketlab.data.polygon_massive.download_daily_flatfiles import update_to_latest_available

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lookback-days", type=int, default=10)
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()

    cfg = MarketlabConfig()
    res = update_to_latest_available(
        cfg,
        lookback_days=args.lookback_days,
        overwrite=args.overwrite,
    )
    print(res)

if __name__ == "__main__":
    main()
