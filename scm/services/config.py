"""
設定ローダー
- 本番: Unity Catalog Volume の config.json
- ローカル開発: scm/config.local.json
"""
import json
import os
from functools import lru_cache
from pathlib import Path

_VOLUME_BASE = "/Volumes/{catalog}/{schema}/scm_data"
_LOCAL_PATH  = Path(__file__).parent.parent / "config.local.json"


@lru_cache(maxsize=1)
def load_config() -> dict:
    catalog = os.environ.get("SCM_CATALOG", "")
    schema  = os.environ.get("SCM_SCHEMA",  "")

    if catalog and schema:
        try:
            volume_path = _VOLUME_BASE.format(catalog=catalog, schema=schema)
            config_path = f"{volume_path}/config.json"
            with open(config_path.replace("/Volumes", "/dbfs/Volumes"), "r", encoding="utf-8") as f:
                cfg = json.load(f)
            cfg["_source"] = "volume"
            return cfg
        except Exception:
            pass

    if _LOCAL_PATH.exists():
        with open(_LOCAL_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        cfg["_source"] = "local"
        return cfg

    return {
        "_source":           "demo",
        "genie_space_id":    None,
        "warehouse_id":      None,
        "catalog":           None,
        "schema":            None,
        "procurement_table": None,
        "warehouse_table":   None,
        "lead_time_table":   None,
        "logistics_table":   None,
        "forecast_table":    None,
    }


def is_databricks_mode() -> bool:
    cfg = load_config()
    return cfg.get("_source") in ("volume", "local") and cfg.get("warehouse_id")


def is_demo_mode() -> bool:
    return load_config().get("_source") == "demo"
