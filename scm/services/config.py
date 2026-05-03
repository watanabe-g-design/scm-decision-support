"""
設定ローダー (Databricks Apps 専用)
================================
- Apps 環境変数 (SCM_CATALOG, SCM_SCHEMA, SCM_WAREHOUSE_ID, SCM_GENIE_SPACE_ID) から読み込む
- Volume 上の config.json があれば、env で未設定の項目を補完
- AS_OF_DATE: デモの「今日」を統一管理 (デフォルト 2026-03-15)
"""
import json
import os
from datetime import date
from functools import lru_cache

_VOLUME_BASE = "/Volumes/{catalog}/{schema}/scm_data"

# ══════════════════════════════════════════════════════
# デモの「今日」を統一管理
# ══════════════════════════════════════════════════════
# データ分布:
#   FCST: 2023-01 〜 2026-10
#   受注: 2026-02-26 〜 2026-03-27 (受注日)
#   発注予定到着: 2026-02-13 〜 2026-10-15
# データ生成スクリプト (data_generation/gen_full.py) の TODAY = 2026-03-28 と整合させる
# → 過去実績 約3年強、未来予測 約7ヶ月、受注は全て発注済みとして表示される
_DEFAULT_AS_OF_DATE = date(2026, 3, 28)


def get_as_of_date() -> date:
    """デモの「今日」を返す。SCM_AS_OF_DATE 環境変数で上書き可能 (YYYY-MM-DD)"""
    env_val = os.environ.get("SCM_AS_OF_DATE", "").strip()
    if env_val:
        try:
            from datetime import datetime
            return datetime.strptime(env_val, "%Y-%m-%d").date()
        except ValueError:
            pass
    return _DEFAULT_AS_OF_DATE


def get_as_of_date_label_jp() -> str:
    """日本語表記の「今日」ラベルを返す。例: '2026年3月15日（日）'"""
    d = get_as_of_date()
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    return f"{d.year}年{d.month}月{d.day}日（{weekdays[d.weekday()]}）"


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
