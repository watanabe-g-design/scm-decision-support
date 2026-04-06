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
        # Databricks Apps では /Volumes/... を直接読める
        # (古い /dbfs/Volumes/... は Apps コンテナに存在しないことがあるので両方試す)
        volume_path = _VOLUME_BASE.format(catalog=catalog, schema=schema)
        for cand in (
            f"{volume_path}/config.json",
            f"/dbfs{volume_path}/config.json",
        ):
            try:
                with open(cand, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                cfg["_source"] = "volume"
                cfg["_loaded_from"] = cand
                # 環境変数の方を優先 (config.json 内の値が空でも環境変数があれば動かす)
                cfg.setdefault("catalog", catalog)
                cfg.setdefault("schema",  schema)
                return cfg
            except FileNotFoundError:
                continue
            except Exception as e:
                # 読めたが JSON が壊れている等
                cfg = {
                    "_source":      "volume",
                    "_loaded_from": cand,
                    "_load_error":  str(e),
                    "catalog":      catalog,
                    "schema":       schema,
                }
                return cfg

        # Volume から読めなかった場合でも環境変数があれば databricks モードとして動かす
        # (config.json が無くても warehouse_id 不要のテーブルアクセスは可能)
        return {
            "_source":  "env",
            "catalog":  catalog,
            "schema":   schema,
            "warehouse_id":   None,
            "genie_space_id": None,
        }

    if _LOCAL_PATH.exists():
        with open(_LOCAL_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        cfg["_source"] = "local"
        return cfg

    return {
        "_source":        "demo",
        "genie_space_id": None,
        "warehouse_id":   None,
        "catalog":        None,
        "schema":         None,
    }


def is_databricks_mode() -> bool:
    cfg = load_config()
    return cfg.get("_source") in ("volume", "local", "env") and cfg.get("warehouse_id")


def is_demo_mode() -> bool:
    return load_config().get("_source") == "demo"
