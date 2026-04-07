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
    """設定読み込み (優先度順):
    1. 環境変数 (Databricks Apps の app.yaml で設定)
    2. Volume 上の config.json (環境変数で足りない値を補う)
    3. ローカル開発用 config.local.json
    4. デモモード (CSVから読む)
    """
    catalog        = os.environ.get("SCM_CATALOG", "")
    schema         = os.environ.get("SCM_SCHEMA",  "")
    warehouse_id   = os.environ.get("SCM_WAREHOUSE_ID",   "").strip()
    genie_space_id = os.environ.get("SCM_GENIE_SPACE_ID", "").strip()

    # HTTP path をそのまま貼られていても末尾だけ抽出
    if "/" in warehouse_id:
        warehouse_id = warehouse_id.rstrip("/").rsplit("/", 1)[-1]
    if "/" in genie_space_id:
        genie_space_id = genie_space_id.rstrip("/").rsplit("/", 1)[-1]

    if catalog and schema:
        cfg = {
            "_source":        "env",
            "catalog":        catalog,
            "schema":         schema,
            "warehouse_id":   warehouse_id or None,
            "genie_space_id": genie_space_id or None,
        }

        # Volume の config.json があれば、env で未設定の項目だけ補完する
        # (Apps コンテナで /Volumes/ が読めないこともあるが、その場合はそのまま env だけで動く)
        volume_path = _VOLUME_BASE.format(catalog=catalog, schema=schema)
        for cand in (
            f"{volume_path}/config.json",
            f"/dbfs{volume_path}/config.json",
        ):
            try:
                with open(cand, "r", encoding="utf-8") as f:
                    vol_cfg = json.load(f)
                cfg["_loaded_from"] = cand
                if not cfg["warehouse_id"] and vol_cfg.get("warehouse_id"):
                    cfg["warehouse_id"] = vol_cfg["warehouse_id"]
                if not cfg["genie_space_id"] and vol_cfg.get("genie_space_id"):
                    cfg["genie_space_id"] = vol_cfg["genie_space_id"]
                break
            except FileNotFoundError:
                continue
            except Exception as e:
                cfg["_volume_load_error"] = str(e)
                break

        return cfg

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
