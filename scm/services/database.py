"""
データ取得層 — Gold テーブル 10本への統一アクセス
"""
import pandas as pd
import streamlit as st
from pathlib import Path
from services.config import load_config, is_databricks_mode, is_demo_mode

_DATA = Path(__file__).parent.parent / "sample_data"


def _run_sql(sql):
    from databricks.sdk import WorkspaceClient
    cfg = load_config()
    w = WorkspaceClient()
    res = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=cfg["warehouse_id"],
        wait_timeout="30s",
    )
    if res.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {res.status.error}")

    schema_cols = res.manifest.schema.columns
    cols = [c.name for c in schema_cols]
    raw_rows = (res.result.data_array if res.result and res.result.data_array else []) or []

    # Statement Execution API は INLINE 形式では data_array を list[list[str]] で返す。
    # 旧バージョンの SDK では list[Row] で row.values を持つこともあるため両対応する。
    data = []
    for row in raw_rows:
        if hasattr(row, "values"):
            data.append([cell.str_value if hasattr(cell, "str_value") else cell for cell in row.values])
        else:
            data.append(list(row))

    df = pd.DataFrame(data, columns=cols)

    # Statement Execution API は値を全て文字列で返す。スキーマ情報を使って正しい dtype に変換する。
    # type_name は SDK バージョンによって enum/str のいずれもありうるので両対応。
    INT_TYPES   = {"INT", "INTEGER", "BIGINT", "LONG", "SHORT", "SMALLINT", "TINYINT", "BYTE"}
    FLOAT_TYPES = {"DOUBLE", "FLOAT", "DECIMAL", "REAL"}
    BOOL_TYPES  = {"BOOLEAN", "BOOL"}
    DATE_TYPES  = {"DATE"}
    TS_TYPES    = {"TIMESTAMP", "TIMESTAMP_NTZ"}

    for col in schema_cols:
        type_name = getattr(col, "type_name", None)
        if type_name is None:
            continue
        # enum の場合は .value or .name から文字列を取り出す
        if hasattr(type_name, "value"):
            type_str = str(type_name.value).upper()
        elif hasattr(type_name, "name"):
            type_str = str(type_name.name).upper()
        else:
            type_str = str(type_name).upper()

        cname = col.name
        if cname not in df.columns:
            continue

        try:
            if type_str in INT_TYPES:
                df[cname] = pd.to_numeric(df[cname], errors="coerce").astype("Int64")
            elif type_str in FLOAT_TYPES:
                df[cname] = pd.to_numeric(df[cname], errors="coerce")
            elif type_str in BOOL_TYPES:
                # API は "true"/"false" 文字列で返す
                df[cname] = df[cname].map(
                    lambda v: True if str(v).lower() == "true"
                    else (False if str(v).lower() == "false" else None)
                )
            elif type_str in DATE_TYPES:
                df[cname] = pd.to_datetime(df[cname], errors="coerce").dt.date
            elif type_str in TS_TYPES:
                df[cname] = pd.to_datetime(df[cname], errors="coerce")
        except Exception:
            # 変換に失敗してもアプリ全体を落とさず、文字列のまま残す
            pass

    return df


def _gold_table(name):
    cfg = load_config()
    return f"{cfg['catalog']}.{cfg['schema']}.{name}"


@st.cache_data(ttl=600)
def _build_gold_cache():
    from logic.gold_builder import build_all_gold
    return build_all_gold()


def _get_gold(name):
    if is_demo_mode():
        return _build_gold_cache()[name].copy()
    return _run_sql(f"SELECT * FROM {_gold_table(name)}")


# ── 公開API (10 Gold テーブル) ────────────────
def get_exec_summary():          return _get_gold("gold_exec_summary_daily")
def get_lt_snapshot():           return _get_gold("gold_lt_snapshot_current")
def get_lt_trend():              return _get_gold("gold_lt_trend_monthly")
def get_lt_escalation():         return _get_gold("gold_lt_escalation_items")
def get_order_commit_risk():     return _get_gold("gold_order_commit_risk")
def get_requirement_timeline():  return _get_gold("gold_requirement_timeline")
def get_balance_projection():    return _get_gold("gold_balance_projection_monthly")
def get_inventory_breach():      return _get_gold("gold_inventory_policy_breach")
def get_geo_warehouse():         return _get_gold("gold_geo_warehouse_status")
def get_pipeline_health():       return _get_gold("gold_data_pipeline_health")
def get_action_queue():          return _get_gold("gold_action_queue_daily")
def get_glossary():              return _get_gold("gold_business_glossary")
def get_metric_definitions():    return _get_gold("gold_metric_definition")
def get_genie_examples():        return _get_gold("gold_genie_semantic_examples")

# ── 補助 (Silver直接参照) ────────────────────
def get_shipment_routes():
    return pd.read_csv(_DATA / "shipment_routes.csv") if is_demo_mode() else _run_sql(f"SELECT * FROM {_gold_table('silver_shipment_routes')}")
