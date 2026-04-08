"""
データ取得層 — Gold/Silver テーブルへの統一アクセス
================================================
Databricks SQL Warehouse 経由で Unity Catalog の Gold/Silver テーブルを読む。
レスポンスはスキーマ情報を使って正しい dtype に変換した DataFrame として返す。
"""
import pandas as pd
import streamlit as st
from services.config import load_config


# ══════════════════════════════════════════════════════
# SQL 実行ヘルパー
# ══════════════════════════════════════════════════════
def _run_sql(sql: str) -> pd.DataFrame:
    """SQL を実行して結果を DataFrame で返す。dtype はスキーマ情報から推論。"""
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

    # SDK バージョン差異吸収: list[list[str]] と list[Row(.values)] の両方
    data = []
    for row in raw_rows:
        if hasattr(row, "values"):
            data.append([
                cell.str_value if hasattr(cell, "str_value") else cell
                for cell in row.values
            ])
        else:
            data.append(list(row))

    df = pd.DataFrame(data, columns=cols)

    # スキーマ情報を使って dtype を正しく変換
    INT_TYPES   = {"INT", "INTEGER", "BIGINT", "LONG", "SHORT", "SMALLINT", "TINYINT", "BYTE"}
    FLOAT_TYPES = {"DOUBLE", "FLOAT", "DECIMAL", "REAL"}
    BOOL_TYPES  = {"BOOLEAN", "BOOL"}
    DATE_TYPES  = {"DATE"}
    TS_TYPES    = {"TIMESTAMP", "TIMESTAMP_NTZ"}

    for col in schema_cols:
        type_name = getattr(col, "type_name", None)
        if type_name is None:
            continue
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
                df[cname] = df[cname].map(
                    lambda v: True if str(v).lower() == "true"
                    else (False if str(v).lower() == "false" else None)
                )
            elif type_str in DATE_TYPES:
                df[cname] = pd.to_datetime(df[cname], errors="coerce").dt.date
            elif type_str in TS_TYPES:
                df[cname] = pd.to_datetime(df[cname], errors="coerce")
        except Exception:
            pass

    return df


def _full_table(name: str) -> str:
    """カタログ.スキーマ.テーブル名 を返す"""
    cfg = load_config()
    return f"{cfg['catalog']}.{cfg['schema']}.{name}"


@st.cache_data(ttl=600)
def _load_table(name: str) -> pd.DataFrame:
    """テーブルを読んで 10 分キャッシュ"""
    return _run_sql(f"SELECT * FROM {_full_table(name)}")


# ══════════════════════════════════════════════════════
# 公開 API: Gold テーブル (14 本)
# ══════════════════════════════════════════════════════
def get_exec_summary():          return _load_table("gold_exec_summary_daily")
def get_lt_snapshot():           return _load_table("gold_lt_snapshot_current")
def get_lt_trend():              return _load_table("gold_lt_trend_monthly")
def get_lt_escalation():         return _load_table("gold_lt_escalation_items")
def get_order_commit_risk():     return _load_table("gold_order_commit_risk")
def get_requirement_timeline():  return _load_table("gold_requirement_timeline")
def get_balance_projection():    return _load_table("gold_balance_projection_monthly")
def get_inventory_breach():      return _load_table("gold_inventory_policy_breach")
def get_geo_warehouse():         return _load_table("gold_geo_warehouse_status")
def get_pipeline_health():       return _load_table("gold_data_pipeline_health")
def get_action_queue():          return _load_table("gold_action_queue_daily")
def get_glossary():              return _load_table("gold_business_glossary")
def get_metric_definitions():    return _load_table("gold_metric_definition")
def get_genie_examples():        return _load_table("gold_genie_semantic_examples")


# ══════════════════════════════════════════════════════
# 公開 API: Silver テーブル (UI のドロップダウン/結合用)
# ══════════════════════════════════════════════════════
def get_silver_components():            return _load_table("silver_components")
def get_silver_suppliers():             return _load_table("silver_suppliers")
def get_silver_warehouses():            return _load_table("silver_warehouses")
def get_silver_warehouse_components():  return _load_table("silver_warehouse_components")
def get_silver_inventory_current():     return _load_table("silver_inventory_current")
def get_silver_shipment_routes():       return _load_table("silver_shipment_routes")


# ── 後方互換: 旧 get_shipment_routes ──
def get_shipment_routes():
    return get_silver_shipment_routes()
