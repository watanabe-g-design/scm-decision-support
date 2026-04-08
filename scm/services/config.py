"""
設定ローダー (Databricks Apps 専用)
================================
- Apps 環境変数 (SCM_CATALOG, SCM_SCHEMA, SCM_WAREHOUSE_ID, SCM_GENIE_SPACE_ID) から読み込む
- Volume 上の config.json があれば、env で未設定の項目を補完
"""
import json
import os
from functools import lru_cache

_VOLUME_BASE = "/Volumes/{catalog}/{schema}/scm_data"


@lru_cache(maxsize=1)
def load_config() -> dict:
    """設定読み込み (環境変数を最優先)"""
    catalog        = os.environ.get("SCM_CATALOG", "").strip()
    schema         = os.environ.get("SCM_SCHEMA",  "").strip()
    warehouse_id   = os.environ.get("SCM_WAREHOUSE_ID",   "").strip()
    genie_space_id = os.environ.get("SCM_GENIE_SPACE_ID", "").strip()

    # HTTP path や URL をそのまま貼られていても末尾だけ抽出
    if "/" in warehouse_id:
        warehouse_id = warehouse_id.rstrip("/").rsplit("/", 1)[-1]
    if "/" in genie_space_id:
        genie_space_id = genie_space_id.rstrip("/").rsplit("/", 1)[-1]

    if not catalog or not schema:
        raise RuntimeError(
            "SCM_CATALOG / SCM_SCHEMA 環境変数が未設定です。app.yaml で設定してください。"
        )

    cfg = {
        "catalog":        catalog,
        "schema":         schema,
        "warehouse_id":   warehouse_id or None,
        "genie_space_id": genie_space_id or None,
        "_source":        "env",
    }

    # Volume の config.json があれば不足項目を補完
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


def is_databricks_mode() -> bool:
    """常に True (デモモードは廃止済み)"""
    return True
