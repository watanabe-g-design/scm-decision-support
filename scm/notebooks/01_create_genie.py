# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 SCM — Genie スペース登録 & config.json 更新
# MAGIC
# MAGIC Lakeflow Pipeline を実行して Bronze/Silver/Gold テーブルが作成された**後に**
# MAGIC 走らせるノートブックです。
# MAGIC
# MAGIC ## このノートブックの役割
# MAGIC 1. パイプライン成果物 (14 Gold テーブル) が揃っているか検証
# MAGIC 2. Genie スペースに登録すべきテーブル一覧を表示
# MAGIC 3. Genie Space ID を `config.json` に書き込み (warehouse_id も)
# MAGIC
# MAGIC ## ⚠️ Genie スペース作成について
# MAGIC Databricks SDK には **Genie スペースを作成する公開 API がまだ存在しません**。
# MAGIC そのため Genie スペース自体は**UIから手動作成**する必要があります。
# MAGIC このノートブックの「ステップ 2」で詳細手順と登録すべきテーブル一覧を表示します。
# MAGIC
# MAGIC ## 使い方 (2 パス実行)
# MAGIC **1回目** (`genie_space_id` 空欄):
# MAGIC   → Genie スペース作成手順とテーブル一覧が表示される
# MAGIC   → 画面の指示に従って UI で Genie スペースを作成
# MAGIC   → 作成した Genie スペースの URL から Space ID をコピー
# MAGIC
# MAGIC **2回目** (`genie_space_id` に貼り付けて Run All):
# MAGIC   → config.json に Space ID と warehouse_id が保存される
# MAGIC   → その後 `02_create_app` ノートブックで App をデプロイ

# COMMAND ----------

dbutils.widgets.text("catalog",         "supply_chain_management", "① カタログ名")
dbutils.widgets.text("schema",          "main",                    "② スキーマ名")
dbutils.widgets.text("warehouse_id",    "",                         "③ SQL Warehouse ID (必須)")
dbutils.widgets.text("genie_space_id",  "",                         "④ Genie Space ID (2回目の実行時に入力)")

CATALOG        = dbutils.widgets.get("catalog")
SCHEMA         = dbutils.widgets.get("schema")
WAREHOUSE_ID   = dbutils.widgets.get("warehouse_id").strip()
GENIE_SPACE_ID = dbutils.widgets.get("genie_space_id").strip()
VOLUME_PATH    = f"/Volumes/{CATALOG}/{SCHEMA}/scm_data"

# HTTP path (/sql/1.0/warehouses/xxx) を貼り付けられた場合は末尾だけ抽出
if "/" in WAREHOUSE_ID:
    WAREHOUSE_ID = WAREHOUSE_ID.rstrip("/").rsplit("/", 1)[-1]

# Genie space URL (https://.../genie/rooms/xxxx) を貼り付けられた場合も末尾だけ抽出
if GENIE_SPACE_ID and "/" in GENIE_SPACE_ID:
    GENIE_SPACE_ID = GENIE_SPACE_ID.rstrip("/").rsplit("/", 1)[-1]
if GENIE_SPACE_ID and "?" in GENIE_SPACE_ID:
    GENIE_SPACE_ID = GENIE_SPACE_ID.split("?", 1)[0]

if not WAREHOUSE_ID:
    dbutils.notebook.exit("❌ warehouse_id を入力してください")

print("=" * 60)
print("  Genie スペース登録")
print("=" * 60)
print(f"  カタログ        : {CATALOG}")
print(f"  スキーマ        : {SCHEMA}")
print(f"  Warehouse ID    : {WAREHOUSE_ID}")
print(f"  Genie Space ID  : {GENIE_SPACE_ID or '(未入力 → 手動作成ガイドを表示)'}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md ## ステップ 1/3: パイプライン成果物の検証

# COMMAND ----------

required_gold = [
    "gold_exec_summary_daily", "gold_lt_snapshot_current", "gold_lt_trend_monthly",
    "gold_lt_escalation_items", "gold_order_commit_risk", "gold_requirement_timeline",
    "gold_balance_projection_monthly", "gold_inventory_policy_breach",
    "gold_geo_warehouse_status", "gold_data_pipeline_health",
    "gold_action_queue_daily", "gold_business_glossary",
    "gold_metric_definition", "gold_genie_semantic_examples",
]
existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`").collect()}

missing = [t for t in required_gold if t not in existing]
if missing:
    print("❌ 以下の Gold テーブルが存在しません。先に Lakeflow Pipeline を実行してください:")
    for t in missing:
        print(f"  - {t}")
    dbutils.notebook.exit("Pipeline not run yet")

print("✅ 14 個の Gold テーブルがすべて存在します")

# COMMAND ----------
# MAGIC %md ## ステップ 2/3: Genie スペース作成 (手動)
# MAGIC
# MAGIC Genie スペース作成の公開 API がまだ無いため、以下の手順で**UI から手動作成**してください。

# COMMAND ----------

genie_tables = [
    # Decision-support Gold tables (what the app displays)
    f"{CATALOG}.{SCHEMA}.gold_exec_summary_daily",
    f"{CATALOG}.{SCHEMA}.gold_order_commit_risk",
    f"{CATALOG}.{SCHEMA}.gold_lt_snapshot_current",
    f"{CATALOG}.{SCHEMA}.gold_lt_escalation_items",
    f"{CATALOG}.{SCHEMA}.gold_balance_projection_monthly",
    f"{CATALOG}.{SCHEMA}.gold_inventory_policy_breach",
    f"{CATALOG}.{SCHEMA}.gold_geo_warehouse_status",
    f"{CATALOG}.{SCHEMA}.gold_action_queue_daily",
    # Semantic helpers for Genie's LLM
    f"{CATALOG}.{SCHEMA}.gold_business_glossary",
    f"{CATALOG}.{SCHEMA}.gold_metric_definition",
    f"{CATALOG}.{SCHEMA}.gold_genie_semantic_examples",
    # Master data for drill-down questions
    f"{CATALOG}.{SCHEMA}.silver_components",
    f"{CATALOG}.{SCHEMA}.silver_warehouses",
    f"{CATALOG}.{SCHEMA}.silver_suppliers",
]

if GENIE_SPACE_ID:
    print(f"✅ Genie Space ID が入力されています: {GENIE_SPACE_ID}")
    print("   ステップ 3 に進みます (config.json 更新)")
else:
    print("━" * 60)
    print("  📋 Genie スペースを UI で作成する手順")
    print("━" * 60)
    print("""
 1. Databricks 左サイドバー → Genie (💡アイコン)
     もしくはブラウザで直接: <ワークスペースURL>/genie

 2. 右上 "+ New" → "Genie space" をクリック

 3. 基本情報を入力:
     - Title       : SCM Decision Support - 半導体商社 SCM アシスタント
     - Description : 半導体商社の在庫・発注・リードタイム・物流データに基づいて
                     質問に回答します。
                     例: 'Critical Orderは何件ですか?'
                         '3ヶ月以内に在庫ゼロになる部品は?'
                         'LTが延長傾向にある部品は?'
                         '今週対応すべきアクションは?'

 4. SQL Warehouse を選択:""")
    print(f"     → Warehouse ID: {WAREHOUSE_ID}")
    print(f"""
 5. Tables タブで "Add tables" をクリックし、以下 {len(genie_tables)} 個を登録:
""")
    for t in genie_tables:
        print(f"     ✓ {t}")
    print("""
 6. 作成後、ブラウザの URL から Space ID をコピー:
     例: https://.../genie/rooms/01abcd1234efgh5678
                                    ^^^^^^^^^^^^^^^^
                                    ← この部分が Space ID

 7. このノートブックに戻り、④ Genie Space ID ウィジェットに貼り付けて
    再度 Run All を実行
""")
    print("━" * 60)
    print("  テーブルIDをコピペ用にフラット出力:")
    print("━" * 60)
    print("\n".join(genie_tables))
    print("\n⏸ genie_space_id を未入力のため、config.json は warehouse_id のみ更新します")

# COMMAND ----------
# MAGIC %md ## ステップ 3/3: config.json を更新

# COMMAND ----------

import json

config = {
    "catalog":        CATALOG,
    "schema":         SCHEMA,
    "warehouse_id":   WAREHOUSE_ID,
    "genie_space_id": GENIE_SPACE_ID or None,
}

config_path = f"{VOLUME_PATH}/config.json"
dbutils.fs.put(config_path, json.dumps(config, ensure_ascii=False, indent=2), overwrite=True)

print(f"📄 config.json を更新しました: {config_path}")
print(json.dumps(config, ensure_ascii=False, indent=2))

print("\n" + "=" * 60)
if GENIE_SPACE_ID:
    print("  ✅ Genie 登録完了。次は 02_create_app を実行してください")
else:
    print("  ⏸ 次のステップ:")
    print("     1. 上記の手順で Genie スペースを UI 作成")
    print("     2. Space ID をコピーしてこのノートブックの ④ 欄に貼り付け")
    print("     3. Run All を再実行")
print("=" * 60)
