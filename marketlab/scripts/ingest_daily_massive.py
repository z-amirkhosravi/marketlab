from marketlab.config import MarketlabConfig
from marketlab.data.polygon_massive.ingest_daily import run_legacy_daily_ingest

def main():
    cfg = MarketlabConfig()
    run_legacy_daily_ingest(cfg)

if __name__ == "__main__":
    main()
