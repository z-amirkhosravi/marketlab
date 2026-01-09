# marketlab/data/polygon_massive/ingest_daily.py
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from marketlab.config import MarketlabConfig

def run_legacy_daily_ingest(cfg: MarketlabConfig) -> None:
    """
    Calls the legacy script as a subprocess to avoid import-time env crashes.
    Later we can refactor legacy into importable functions.
    """
    legacy_path = Path(__file__).resolve().parents[2] / "legacy" / "update_massive_flatfiles.py"
    if not legacy_path.exists():
        raise FileNotFoundError(f"Legacy ingest script not found at: {legacy_path}")

    env = dict(**os.environ)
    # Ensure required env vars are present
    api_key = cfg.require_massive_api_key()
    ak, sk = cfg.require_massive_s3_creds()
    env["MASSIVE_S3_ACCESS_KEY"] = ak
    env["MASSIVE_S3_SECRET_KEY"] = sk
    env["MASSIVE_API_KEY"] = api_key
    env["MARKETLAB_ARCTIC_URI"] = cfg.arctic_uri
    env["MARKETLAB_ARCTIC_LIB_DAILY"] = cfg.daily_lib

    subprocess.check_call([sys.executable, str(legacy_path)], env=env)
