# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 SCM — Genie スペース作成 & config.json 更新
# MAGIC
# MAGIC Lakeflow Pipeline を実行して Bronze/Silver/Gold テーブルが作成された**後に**
# MAGIC 走らせるノートブックです。以下を実施します。
# MAGIC
# MAGIC 1. 14 個の Gold テーブルと 3 個の重要 Silver テーブルを Genie スペースに登録
# MAGIC 2. Volume 上の `config.json` に `genie_space_id` と `warehouse_id` を書き込み
# MAGIC
# MAGIC ## パラメータ
# MAGIC `warehouse_id` を必ず入力してください。Genie は SQL Warehouse がないと動作しません。
# MAGIC Serverless SQL Warehouse で OK です。

# COMMAND ----------

dbutils.widgets.text("catalog",      "supply_chain_management", "① カタログ名")
dbutils.widgets.text("schema",       "main",                    "② スキーマ名")
dbutils.widgets.text("warehouse_id", "",                         "③ SQLウェアハウスID (必須)")

CATALOG      = dbutils.widgets.get("catalog")
SCHEMA       = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
VOLUME_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/scm_data"

# HTTP path (/sql/1.0/warehouses/xxx) を貼り付けられた場合は末尾だけ抽出
if "/" in WAREHOUSE_ID:
    WAREHOUSE_ID = WAREHOUSE_ID.rstrip("/").rsplit("/", 1)[-1]

if not WAREHOUSE_ID:
    dbutils.notebook.exit("❌ warehouse_id を入力してください (SQL Warehouses → Connection details → HTTP path の末尾)")

print("=" * 60)
print("  Genie スペース作成")
print("=" * 60)
print(f"  カタログ     : {CATALOG}")
print(f"  スキーマ     : {SCHEMA}")
print(f"  Warehouse ID : {WAREHOUSE_ID}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md ## ステップ 1/3: パイプラインが作成したテーブルを確認

# COMMAND ----------

required = [
    "gold_exec_summary_daily", "gold_lt_snapshot_current", "gold_lt_trend_monthly",
    "gold_lt_escalation_items", "gold_order_commit_risk", "gold_requirement_timeline",
    "gold_balance_projection_monthly", "gold_inventory_policy_breach",
    "gold_geo_warehouse_status", "gold_data_pipeline_health",
    "gold_action_queue_daily", "gold_business_glossary",
    "gold_metric_definition", "gold_genie_semantic_examples",
]
existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`").collect()}

missing = [t for t in required if t not in existing]
if missing:
    print("❌ 以下のテーブルが存在しません。先に Lakeflow Pipeline を実行してください:")
    for t in missing:
        print(f"  - {t}")
    dbutils.notebook.exit("Pipeline not run yet")

print("✅ 14 個の Gold テーブルがすべて存在します")

# COMMAND ----------
# MAGIC %md ## ステップ 2/3: Genie スペース作成

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

genie_tables = [f"{CATALOG}.{SCHEMA}.{t}" for t in [
    # Core decision tables the app displays
    "gold_exec_summary_daily",
    "gold_order_commit_risk",
    "gold_lt_snapshot_current",
    "gold_lt_escalation_items",
    "gold_balance_projection_monthly",
    "gold_inventory_policy_breach",
    "gold_geo_warehouse_status",
    "gold_action_queue_daily",
    # Semantic helpers
    "gold_business_glossary",
    "gold_metric_definition",
    "gold_genie_semantic_examples",
    # Master data for drill-down
    "silver_components",
    "silver_warehouses",
    "silver_suppliers",
]]

print("🤖 Genie スペースを作成中...")
try:
    genie_space = w.genie.create_space(
        title="SCM Decision Support - 半導体商社 SCM アシスタント",
        description=(
            "半導体商社の在庫・発注・リードタイム・物流データに基づいて質問に回答します。\n"
            "例: 'Critical Orderは何件ですか？' '3ヶ月以内に在庫ゼロになる部品は？' "
            "'LTが延長傾向にある部品は？' '今週対応すべきアクションは？'"
        ),
        warehouse_id=WAREHOUSE_ID,
        table_identifiers=genie_tables,
    )
    GENIE_SPACE_ID = genie_space.space_id
    print(f"  ✅ Genie スペース作成完了")
    print(f"  Space ID: {GENIE_SPACE_ID}")
except Exception as e:
    GENIE_SPACE_ID = None
    print(f"  ⚠️ Genie スペース作成スキップ: {e}")
    print(f"  → 手動で Genie スペースを作成して space_id を config.json に書き込んでください")

# COMMAND ----------
# MAGIC %md ## ステップ 3/3: config.json を更新

# COMMAND ----------

import json

config = {
    "catalog":        CATALOG,
    "schema":         SCHEMA,
    "warehouse_id":   WAREHOUSE_ID,
    "genie_space_id": GENIE_SPACE_ID,
}

config_path = f"{VOLUME_PATH}/config.json"
dbutils.fs.put(config_path, json.dumps(config, ensure_ascii=False, indent=2), overwrite=True)

print(f"📄 config.json を更新しました: {config_path}")
print(json.dumps(config, ensure_ascii=False, indent=2))

print("\n" + "=" * 60)
print("  ✅ すべて完了! Databricks App を Redeploy してください")
print("=" * 60)
