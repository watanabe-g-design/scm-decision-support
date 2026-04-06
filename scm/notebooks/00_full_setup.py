# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Supply Chain Management - 完全自動セットアップ
# MAGIC
# MAGIC このノートブック1つで、Supply Chain Management デモ環境を **すべて自動構築** します。
# MAGIC
# MAGIC ## 実行内容
# MAGIC | ステップ | 内容 | 所要時間 |
# MAGIC |---------|------|---------|
# MAGIC | 1 | Unity Catalog (カタログ・スキーマ・ボリューム) 作成 | ~10秒 |
# MAGIC | 2 | CSVデータをボリュームへコピー | ~30秒 |
# MAGIC | 3 | Bronze テーブル作成 (CSV → Delta) | ~1分 |
# MAGIC | 4 | Silver テーブル作成 (型変換・クレンジング) | ~1分 |
# MAGIC | 5 | Gold テーブル作成 (アナリティクス) | ~2分 |
# MAGIC | 6 | テーブルコメント付与 (Genie用) | ~10秒 |
# MAGIC | 7 | Genie スペース作成 | ~30秒 |
# MAGIC | 8 | config.json 書き出し | ~5秒 |
# MAGIC
# MAGIC **合計: 約5分で完了します。**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ 実行前の確認
# MAGIC 1. 右上の **クラスター** を選択済みですか？
# MAGIC 2. 下のパラメータ欄に **warehouse_id** を入力しましたか？
# MAGIC 3. このノートブックは **Git Folder (Repos)** から開いていますか？

# COMMAND ----------
# MAGIC %md ## 📝 パラメータ設定
# MAGIC
# MAGIC **warehouse_id** だけ必ず入力してください。他はデフォルトのままでOKです。

# COMMAND ----------

dbutils.widgets.text("catalog",      "supply_chain_management", "① カタログ名")
dbutils.widgets.text("schema",       "main",      "② スキーマ名")
dbutils.widgets.text("warehouse_id", "",           "③ SQLウェアハウスID (必須)")

CATALOG      = dbutils.widgets.get("catalog")
SCHEMA       = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
VOLUME_NAME  = "scm_data"
VOLUME_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
TODAY         = "2026-03-28"  # デモ基準日

print("=" * 60)
print("  Supply Chain Management 完全自動セットアップ")
print("=" * 60)
print(f"  カタログ     : {CATALOG}")
print(f"  スキーマ     : {SCHEMA}")
print(f"  ボリューム   : {VOLUME_PATH}")
print(f"  ウェアハウスID: {WAREHOUSE_ID or '(未設定 - Genieスキップ)'}")
print(f"  デモ基準日   : {TODAY}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md ## ステップ 1/8: Unity Catalog リソース作成

# COMMAND ----------

print("📦 Unity Catalog リソースを作成中...")

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
print(f"  ✅ カタログ `{CATALOG}` 作成完了")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SCHEMA}`")
print(f"  ✅ スキーマ `{SCHEMA}` 作成完了")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME_NAME}`")
print(f"  ✅ ボリューム `{VOLUME_NAME}` 作成完了")

# COMMAND ----------
# MAGIC %md ## ステップ 2/8: CSVデータをボリュームへコピー
# MAGIC
# MAGIC Git Folder (Repos) 内の `sample_data/` フォルダから
# MAGIC Unity Catalog Volume へ CSV ファイルをコピーします。

# COMMAND ----------

import os
import glob

print("📂 CSVファイルをボリュームへコピー中...")

# ── このノートブックのパスから sample_data を特定 ──
# Databricks Git Folder では、ノートブックのパスは以下のようになる:
#   /Workspace/Repos/<user>/<repo>/scm/notebooks/00_full_setup
#   /Workspace/Users/<user>/<repo>/scm/notebooks/00_full_setup

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(f"  ノートブックパス: {notebook_path}")

# ノートブックから2階層上がrepoルート、そこから scm/sample_data
# /Workspace/.../scm/notebooks/00_full_setup → /Workspace/.../scm/sample_data
repo_scm_path = "/".join(notebook_path.split("/")[:-2])  # scm/ ディレクトリ
workspace_prefix = "/Workspace"

# sample_data のファイルシステムパス候補
candidates = [
    f"{workspace_prefix}{repo_scm_path}/sample_data",
    f"{repo_scm_path}/sample_data",
]

# Repos / Users 両方のパターンを試す
sample_data_path = None
for candidate in candidates:
    test_path = candidate.replace("/Workspace", "/Workspace")
    try:
        files = os.listdir(test_path)
        if any(f.endswith(".csv") for f in files):
            sample_data_path = test_path
            break
    except Exception:
        pass

if sample_data_path is None:
    # フォールバック: dbutils.fs でファイルを探す
    for candidate in candidates:
        try:
            items = dbutils.fs.ls(f"file:{candidate}")
            if any(item.name.endswith(".csv") for item in items):
                sample_data_path = candidate
                break
        except Exception:
            pass

if sample_data_path is None:
    print("  ❌ sample_data フォルダが見つかりません!")
    print("  以下の手順で手動アップロードしてください:")
    print(f"  1. Catalog → {CATALOG} → {SCHEMA} → Volumes → {VOLUME_NAME} を開く")
    print(f"  2. 'csv' フォルダを作成")
    print(f"  3. ローカルの scm/sample_data/*.csv を全てアップロード")
    print(f"  4. このセルを再実行")
    dbutils.notebook.exit("CSV not found - manual upload required")

print(f"  📁 CSVソース: {sample_data_path}")

# ── ボリュームへコピー ──
csv_dest = f"{VOLUME_PATH}/csv"
dbutils.fs.mkdirs(csv_dest)

csv_files = [f for f in os.listdir(sample_data_path) if f.endswith(".csv")]
for csv_file in sorted(csv_files):
    src = f"file:{sample_data_path}/{csv_file}"
    dst = f"{csv_dest}/{csv_file}"
    dbutils.fs.cp(src, dst, recurse=False)
    size_kb = os.path.getsize(f"{sample_data_path}/{csv_file}") / 1024
    print(f"  📄 {csv_file:<30} → Volume ({size_kb:.1f} KB)")

print(f"\n  ✅ {len(csv_files)} ファイルのコピー完了")

# COMMAND ----------
# MAGIC %md ## ステップ 3/8: Bronze テーブル作成 (CSV → Delta)
# MAGIC
# MAGIC CSVを読み込み、そのままDeltaテーブルとして保存します（生データ保持）。

# COMMAND ----------

from pyspark.sql import functions as F

print("🔶 Bronze テーブルを作成中...")

CSV_BASE = f"{VOLUME_PATH}/csv"

def ingest_csv(filename, table_name):
    """CSVファイルを読み込んでDeltaテーブルとして保存"""
    path = f"{CSV_BASE}/{filename}"
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("encoding", "UTF-8")
          .csv(path))
    df = df.withColumn("_ingested_at", F.current_timestamp())

    full_table = f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_table))

    count = spark.table(full_table).count()
    print(f"  ✅ {table_name:<35} {count:>8,} rows")
    return count

total_rows = 0
tables = [
    ("suppliers.csv",         "bronze_suppliers"),
    ("customers.csv",         "bronze_customers"),
    ("products.csv",          "bronze_products"),
    ("components.csv",        "bronze_components"),
    ("warehouses.csv",        "bronze_warehouses"),
    ("bom.csv",               "bronze_bom"),
    ("forecasts.csv",         "bronze_forecasts"),
    ("lead_times.csv",        "bronze_lead_times"),
    ("inventory.csv",         "bronze_inventory"),
    ("inventory_current.csv", "bronze_inventory_current"),
    ("logistics.csv",         "bronze_logistics"),
    ("sales_orders.csv",      "bronze_sales_orders"),
    ("purchase_orders.csv",   "bronze_purchase_orders"),
    ("warehouse_components.csv","bronze_warehouse_components"),
    ("shipment_routes.csv",   "bronze_shipment_routes"),
]

for fname, tname in tables:
    total_rows += ingest_csv(fname, tname)

print(f"\n  ✅ Bronze 完了: {len(tables)} テーブル, 合計 {total_rows:,} rows")

# COMMAND ----------
# MAGIC %md ## ステップ 4/8: Silver テーブル作成 (型変換・クレンジング)
# MAGIC
# MAGIC データ型の変換、NULL除去、日付パースなどを実施します。

# COMMAND ----------

print("⬜ Silver テーブルを作成中...")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

silver_tables = {
    "silver_suppliers": """
        SELECT supplier_id, supplier_name, country, region,
               current_timestamp() AS updated_at
        FROM bronze_suppliers WHERE supplier_id IS NOT NULL
    """,
    "silver_customers": """
        SELECT customer_id, customer_name, segment, location,
               current_timestamp() AS updated_at
        FROM bronze_customers WHERE customer_id IS NOT NULL
    """,
    "silver_products": """
        SELECT product_id, product_name, product_category, customer_id,
               CAST(unit_price_jpy AS INT) AS unit_price_jpy,
               current_timestamp() AS updated_at
        FROM bronze_products WHERE product_id IS NOT NULL
    """,
    "silver_components": """
        SELECT component_id, part_number, component_name, component_category,
               supplier_id,
               CAST(base_lead_time_weeks AS INT) AS base_lead_time_weeks,
               CAST(unit_price_usd AS DOUBLE) AS unit_price_usd,
               CAST(safety_stock_weeks AS INT) AS safety_stock_weeks,
               CAST(min_order_qty AS INT) AS min_order_qty,
               current_timestamp() AS updated_at
        FROM bronze_components WHERE component_id IS NOT NULL
    """,
    "silver_warehouses": """
        SELECT warehouse_id, warehouse_name, prefecture, city,
               CAST(latitude AS DOUBLE) AS latitude,
               CAST(longitude AS DOUBLE) AS longitude,
               CAST(capacity_sqm AS INT) AS capacity_sqm,
               current_timestamp() AS updated_at
        FROM bronze_warehouses WHERE warehouse_id IS NOT NULL
    """,
    "silver_bom": """
        SELECT product_id, component_id,
               CAST(quantity_per_unit AS INT) AS quantity_per_unit
        FROM bronze_bom
        WHERE product_id IS NOT NULL AND component_id IS NOT NULL
    """,
    "silver_forecasts": """
        SELECT forecast_id, product_id, customer_id,
               TO_DATE(forecast_month) AS forecast_month,
               CAST(forecast_qty AS INT) AS forecast_qty,
               CAST(forecast_accuracy AS DOUBLE) AS forecast_accuracy,
               TO_TIMESTAMP(created_at) AS created_at
        FROM bronze_forecasts WHERE forecast_qty > 0
    """,
    "silver_lead_times": """
        SELECT lead_time_id, component_id, supplier_id,
               TO_DATE(effective_date) AS effective_date,
               CAST(lead_time_weeks AS INT) AS lead_time_weeks,
               change_reason,
               TO_DATE(recorded_at) AS recorded_at
        FROM bronze_lead_times WHERE lead_time_weeks > 0
    """,
    "silver_inventory": """
        SELECT snapshot_id, component_id, warehouse_id,
               TO_DATE(snapshot_month) AS snapshot_month,
               CAST(stock_qty AS INT) AS stock_qty,
               CAST(safety_stock_qty AS INT) AS safety_stock_qty,
               CAST(replenishment_qty AS INT) AS replenishment_qty,
               CAST(demand_qty AS INT) AS demand_qty
        FROM bronze_inventory WHERE stock_qty >= 0
    """,
    "silver_inventory_current": """
        SELECT snapshot_id, component_id, warehouse_id,
               TO_DATE(snapshot_month) AS snapshot_month,
               CAST(stock_qty AS INT) AS stock_qty,
               CAST(safety_stock_qty AS INT) AS safety_stock_qty,
               CAST(replenishment_qty AS INT) AS replenishment_qty,
               CAST(demand_qty AS INT) AS demand_qty
        FROM bronze_inventory_current
    """,
    "silver_logistics": """
        SELECT shipment_id, component_id, supplier_id,
               destination_warehouse_id,
               TO_DATE(order_date) AS order_date,
               TO_DATE(expected_arrival_date) AS expected_arrival_date,
               TO_DATE(actual_arrival_date) AS actual_arrival_date,
               CAST(quantity AS INT) AS quantity,
               status,
               CAST(delay_days AS INT) AS delay_days,
               delay_cause,
               CAST(unit_cost_usd AS DOUBLE) AS unit_cost_usd
        FROM bronze_logistics
    """,
    "silver_sales_orders": """
        SELECT sales_order_id, customer_id, customer_name, product_id, product_name,
               component_id, TO_DATE(order_date) AS order_date,
               TO_DATE(requested_delivery_date) AS requested_delivery_date,
               CAST(order_qty AS INT) AS order_qty,
               CAST(shipped_qty AS INT) AS shipped_qty,
               CAST(remaining_qty AS INT) AS remaining_qty,
               CAST(component_required_qty AS INT) AS component_required_qty,
               status, CAST(priority_flag AS BOOLEAN) AS priority_flag
        FROM bronze_sales_orders
    """,
    "silver_purchase_orders": """
        SELECT purchase_order_id, component_id, supplier_id,
               TO_DATE(order_date) AS order_date,
               TO_DATE(expected_delivery_date) AS expected_delivery_date,
               CAST(quantity AS INT) AS quantity,
               CAST(received_qty AS INT) AS received_qty,
               CAST(outstanding_qty AS INT) AS outstanding_qty,
               status, CAST(is_delayed AS BOOLEAN) AS is_delayed,
               CAST(delay_days AS INT) AS delay_days,
               CAST(unit_cost_usd AS DOUBLE) AS unit_cost_usd,
               CAST(total_cost_usd AS DOUBLE) AS total_cost_usd
        FROM bronze_purchase_orders
    """,
    "silver_warehouse_components": """
        SELECT component_id, warehouse_id,
               CAST(is_primary AS BOOLEAN) AS is_primary,
               CAST(allocation_pct AS DOUBLE) AS allocation_pct
        FROM bronze_warehouse_components
    """,
    "silver_shipment_routes": """
        SELECT route_type, from_id, from_name,
               CAST(from_lat AS DOUBLE) AS from_lat, CAST(from_lon AS DOUBLE) AS from_lon,
               to_id, to_name,
               CAST(to_lat AS DOUBLE) AS to_lat, CAST(to_lon AS DOUBLE) AS to_lon,
               CAST(avg_transit_days AS INT) AS avg_transit_days,
               CAST(monthly_shipments AS INT) AS monthly_shipments
        FROM bronze_shipment_routes
    """,
}

for table_name, sql in silver_tables.items():
    spark.sql(f"CREATE OR REPLACE TABLE {table_name} AS {sql}")
    count = spark.table(table_name).count()
    print(f"  ✅ {table_name:<35} {count:>8,} rows")

print(f"\n  ✅ Silver 完了: {len(silver_tables)} テーブル")

# COMMAND ----------
# MAGIC %md ## ステップ 5/8: Gold テーブル作成 (10テーブル)
# MAGIC
# MAGIC docs/05_data_model.md 準拠のGold 10テーブルを作成します。

# COMMAND ----------


print("Gold 10 tables (docs/05 compliant)...")

# 1. gold_lt_snapshot_current
print("  1/10 gold_lt_snapshot_current...")
spark.sql(f"""
CREATE OR REPLACE TABLE gold_lt_snapshot_current AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY component_id ORDER BY effective_date DESC) AS rn
  FROM silver_lead_times
), n3 AS (
  SELECT component_id, lead_time_weeks AS lt_n3
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY component_id ORDER BY effective_date DESC) AS rn
        FROM silver_lead_times WHERE effective_date <= ADD_MONTHS(DATE('{TODAY}'), -3)) WHERE rn=1
), n6 AS (
  SELECT component_id, lead_time_weeks AS lt_n6
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY component_id ORDER BY effective_date DESC) AS rn
        FROM silver_lead_times WHERE effective_date <= ADD_MONTHS(DATE('{TODAY}'), -6)) WHERE rn=1
)
SELECT DATE('{TODAY}') AS snapshot_date, c.component_id AS item_id, c.part_number AS item_code,
  c.component_name AS item_name, s.supplier_name AS manufacturer_name,
  l.lead_time_weeks AS latest_lt_weeks, n3.lt_n3 AS lt_n3_weeks, n6.lt_n6 AS lt_n6_weeks,
  l.lead_time_weeks - n3.lt_n3 AS delta_vs_n3, l.lead_time_weeks - n6.lt_n6 AS delta_vs_n6,
  CASE WHEN l.lead_time_weeks > COALESCE(n3.lt_n3, l.lead_time_weeks) THEN 'up' WHEN l.lead_time_weeks < COALESCE(n3.lt_n3, l.lead_time_weeks) THEN 'down' ELSE 'flat' END AS trend_arrow_n3,
  CASE WHEN l.lead_time_weeks > COALESCE(n6.lt_n6, l.lead_time_weeks) THEN 'up' WHEN l.lead_time_weeks < COALESCE(n6.lt_n6, l.lead_time_weeks) THEN 'down' ELSE 'flat' END AS trend_arrow_n6,
  c.component_category, c.supplier_id
FROM latest l JOIN silver_components c ON l.component_id = c.component_id
JOIN silver_suppliers s ON c.supplier_id = s.supplier_id
LEFT JOIN n3 ON l.component_id = n3.component_id LEFT JOIN n6 ON l.component_id = n6.component_id
WHERE l.rn = 1
""")

# 2. gold_lt_trend_monthly
print("  2/10 gold_lt_trend_monthly...")
spark.sql(f"""CREATE OR REPLACE TABLE gold_lt_trend_monthly AS
SELECT lt.effective_date, lt.component_id, c.part_number, c.component_name,
  c.component_category, s.supplier_name, lt.lead_time_weeks
FROM silver_lead_times lt JOIN silver_components c ON lt.component_id = c.component_id
JOIN silver_suppliers s ON c.supplier_id = s.supplier_id""")

# 3. gold_lt_escalation_items
print("  3/10 gold_lt_escalation_items...")
spark.sql(f"""CREATE OR REPLACE TABLE gold_lt_escalation_items AS
SELECT * FROM gold_lt_snapshot_current WHERE trend_arrow_n3 = 'up' OR trend_arrow_n6 = 'up'""")

# 4. gold_order_commit_risk
print("  4/10 gold_order_commit_risk...")
spark.sql(f"""CREATE OR REPLACE TABLE gold_order_commit_risk AS
SELECT DATE('{TODAY}') AS snapshot_date, so.*, c.part_number, c.component_name, c.component_category, s.supplier_name,
  DATEDIFF(so.requested_delivery_date, DATE('{TODAY}')) AS days_to_due,
  CASE WHEN DATEDIFF(so.requested_delivery_date, DATE('{TODAY}')) <= 3 THEN 'Critical'
       WHEN DATEDIFF(so.requested_delivery_date, DATE('{TODAY}')) <= 7 THEN 'High'
       WHEN DATEDIFF(so.requested_delivery_date, DATE('{TODAY}')) <= 14 THEN 'Mid' ELSE 'Low' END AS priority_rank
FROM silver_sales_orders so JOIN silver_components c ON so.component_id = c.component_id
JOIN silver_suppliers s ON c.supplier_id = s.supplier_id""")

# 5. gold_requirement_timeline
print("  5/10 gold_requirement_timeline...")
spark.sql(f"""CREATE OR REPLACE TABLE gold_requirement_timeline AS
SELECT DATE('{TODAY}') AS snapshot_date, component_id AS item_id,
  requested_delivery_date AS event_date, 'production_use' AS event_type,
  sales_order_id AS order_no, -component_required_qty AS quantity FROM silver_sales_orders
UNION ALL
SELECT DATE('{TODAY}'), component_id, expected_delivery_date, 'inbound',
  purchase_order_id, outstanding_qty FROM silver_purchase_orders
WHERE status IN ('placed','acknowledged','in_production','shipped','partial_received')""")

# 6. gold_balance_projection_monthly
print("  6/10 gold_balance_projection_monthly...")
spark.sql(f"""CREATE OR REPLACE TABLE gold_balance_projection_monthly AS
SELECT DATE('{TODAY}') AS snapshot_date, ic.component_id AS item_id, c.part_number AS item_code,
  c.component_name AS product_name, ic.stock_qty AS customer_stock_proj,
  COALESCE(c.min_stock, 100) AS min_qty, COALESCE(c.max_stock, 1000) AS max_qty,
  CASE WHEN ic.stock_qty <= 0 THEN 'ZERO' WHEN ic.stock_qty < COALESCE(c.min_stock,100) THEN 'UNDER'
       WHEN ic.stock_qty > COALESCE(c.max_stock,1000) THEN 'OVER' ELSE 'OK' END AS policy_status
FROM silver_inventory_current ic JOIN silver_components c ON ic.component_id = c.component_id""")

# 7. gold_inventory_policy_breach
print("  7/10 gold_inventory_policy_breach...")
spark.sql(f"""CREATE OR REPLACE TABLE gold_inventory_policy_breach AS
SELECT * FROM gold_balance_projection_monthly WHERE policy_status != 'OK'""")

# 8. gold_geo_warehouse_status
print("  8/10 gold_geo_warehouse_status...")
spark.sql(f"""CREATE OR REPLACE TABLE gold_geo_warehouse_status AS
SELECT DATE('{TODAY}') AS snapshot_date, w.warehouse_id, w.warehouse_name,
  w.latitude AS geo_lat, w.longitude AS geo_lon, w.prefecture,
  COUNT(DISTINCT ic.component_id) AS managed_count,
  SUM(CASE WHEN ic.stock_qty <= 0 THEN 1 ELSE 0 END) AS zero_count,
  SUM(CASE WHEN ic.stock_qty > 0 AND ic.stock_qty < COALESCE(c.min_stock,100) THEN 1 ELSE 0 END) AS under_count,
  SUM(CASE WHEN ic.stock_qty > COALESCE(c.max_stock,1000) THEN 1 ELSE 0 END) AS over_count,
  ROUND((1.0 - SUM(CASE WHEN ic.stock_qty < COALESCE(c.min_stock,100) THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*),0)) * 100, 1) AS health_score
FROM silver_warehouses w LEFT JOIN silver_inventory_current ic ON w.warehouse_id = ic.warehouse_id
LEFT JOIN silver_components c ON ic.component_id = c.component_id
GROUP BY w.warehouse_id, w.warehouse_name, w.latitude, w.longitude, w.prefecture""")

# 9. gold_data_pipeline_health
print("  9/10 gold_data_pipeline_health...")
spark.sql(f"""CREATE OR REPLACE TABLE gold_data_pipeline_health AS
SELECT DATE('{TODAY}') AS snapshot_date, 'full_setup' AS pipeline_name,
  'csv' AS source_table, 'gold' AS target_table,
  (SELECT COUNT(*) FROM silver_components) AS record_count,
  current_timestamp() AS freshness_ts, 100.0 AS quality_score,
  TRUE AS success_flag, CAST(NULL AS STRING) AS error_message""")

# 10. gold_exec_summary_daily
print("  10/10 gold_exec_summary_daily...")
spark.sql(f"""CREATE OR REPLACE TABLE gold_exec_summary_daily AS
SELECT DATE('{TODAY}') AS snapshot_date,
  (SELECT COUNT(*) FROM gold_order_commit_risk WHERE priority_rank = 'Critical') AS critical_count,
  (SELECT COUNT(*) FROM gold_order_commit_risk WHERE priority_rank = 'High') AS high_count,
  (SELECT COUNT(*) FROM gold_order_commit_risk WHERE priority_rank = 'Mid') AS medium_count,
  (SELECT COUNT(*) FROM gold_order_commit_risk WHERE priority_rank = 'Low') AS low_count,
  (SELECT COUNT(*) FROM gold_lt_escalation_items) AS lt_escalation_item_count,
  (SELECT COUNT(DISTINCT item_id) FROM gold_inventory_policy_breach WHERE breach_type = 'ZERO') AS zero_count,
  (SELECT COUNT(DISTINCT item_id) FROM gold_inventory_policy_breach WHERE breach_type = 'UNDER') AS under_count,
  (SELECT COUNT(DISTINCT item_id) FROM gold_inventory_policy_breach WHERE breach_type = 'OVER') AS over_count,
  (SELECT ROUND(AVG(health_score), 1) FROM gold_geo_warehouse_status) AS warehouse_health_score,
  0 AS top_risk_order_count, 65.0 AS forecast_accuracy_pct""")

print("  Gold 10/10 created!")

# COMMAND ----------
# MAGIC %md ## ステップ 6/8: テーブルコメント付与 (Genie 用)
# MAGIC
# MAGIC Genie AI が各テーブルの意味を理解できるよう、日本語コメントを付与します。

# COMMAND ----------

print("💬 テーブルコメントを付与中...")

comments = {
    "gold_procurement_alerts":
        "発注アラートテーブル。部品ごとの在庫状況・リードタイムから発注優先度(CRITICAL/HIGH/MEDIUM/LOW)を計算。"
        "priorityカラムがCRITICALの場合は7日以内に発注が必要。days_to_order_deadlineが0以下は発注遅延。"
        "recommended_order_qtyは推奨発注数量（MOQ単位）。supplier_nameはメーカー名。",

    "gold_inventory_by_warehouse":
        "倉庫別在庫サマリー。日本全国10拠点の在庫量・在庫金額(JPY)・健全性スコア(health_score, 0-100%)を集計。"
        "critical_itemsはCRITICAL部品数。below_safety_countは安全在庫割れの品目数。"
        "latitudeとlongitudeで地図表示可能。",

    "gold_lead_time_trend":
        "リードタイムトレンド。四半期ごとの部品リードタイム推移。"
        "2023年はCOVID影響で大幅延長（2倍以上）、2024年から正常化。"
        "lead_time_change_weeksが正なら延長、負なら短縮。component_categoryで半導体種類別に分析可能。",

    "gold_logistics_status":
        "物流ステータス。発注〜入荷までの全履歴。statusはordered/in_transit/delayed/delivered。"
        "delay_daysが0より大きい場合は遅延。delay_causeに遅延理由。"
        "component_priorityで発注アラートとの紐付けが可能。",

    "gold_demand_forecast":
        "需要予測サマリー。顧客フォーキャスト×BOM展開による部品別需要。"
        "customer_nameで顧客別、supplier_nameでメーカー別、component_demand_qtyで数量を確認可能。"
        "forecast_accuracyは予測精度（0-1）。",
}

for table, comment in comments.items():
    spark.sql(f"COMMENT ON TABLE `{CATALOG}`.`{SCHEMA}`.`{table}` IS '{comment}'")
    print(f"  ✅ {table}")

# 列コメントも主要テーブルに付与
col_comments = [
    ("gold_procurement_alerts", "priority", "発注優先度: CRITICAL(7日以内), HIGH(14日以内), MEDIUM(30日以内), LOW(余裕あり), INACTIVE(需要なし)"),
    ("gold_procurement_alerts", "days_to_order_deadline", "発注期限までの残り日数。0以下は発注遅延中"),
    ("gold_procurement_alerts", "recommended_order_qty", "推奨発注数量（MOQ=最小発注数量の倍数）"),
    ("gold_procurement_alerts", "current_lead_time_weeks", "現在のリードタイム（週）"),
    ("gold_inventory_by_warehouse", "health_score", "在庫健全性スコア: 100=全品目安全在庫以上, 0=全品目安全在庫割れ"),
    ("gold_inventory_by_warehouse", "total_stock_value_jpy", "在庫金額（円, USD×150円換算）"),
]

for table, col, comment in col_comments:
    try:
        spark.sql(f"ALTER TABLE `{CATALOG}`.`{SCHEMA}`.`{table}` ALTER COLUMN `{col}` COMMENT '{comment}'")
    except Exception:
        pass  # 列コメントは非対応の場合スキップ

print("\n  ✅ コメント付与完了")

# COMMAND ----------
# MAGIC %md ## ステップ 7/8: Genie スペース作成
# MAGIC
# MAGIC 自然言語でSCMデータに質問できるAIアシスタントを作成します。

# COMMAND ----------

GENIE_SPACE_ID = "MANUAL_SETUP_REQUIRED"

if not WAREHOUSE_ID:
    print("⚠️ warehouse_id が未設定のため Genie スペースをスキップします")
    print("  → パラメータ欄に warehouse_id を入力して再実行してください")
else:
    print("🤖 Genie スペースを作成中...")
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()

        genie_tables = [
            f"{CATALOG}.{SCHEMA}.gold_procurement_alerts",
            f"{CATALOG}.{SCHEMA}.gold_inventory_by_warehouse",
            f"{CATALOG}.{SCHEMA}.gold_lead_time_trend",
            f"{CATALOG}.{SCHEMA}.gold_logistics_status",
            f"{CATALOG}.{SCHEMA}.gold_demand_forecast",
            f"{CATALOG}.{SCHEMA}.silver_components",
            f"{CATALOG}.{SCHEMA}.silver_warehouses",
            f"{CATALOG}.{SCHEMA}.silver_suppliers",
        ]

        genie_space = w.genie.create_space(
            title="Supply Chain Management - 半導体商社サプライチェーンアシスタント",
            description=(
                "半導体商社の在庫・発注・リードタイム・物流データに基づいて質問に回答します。\n"
                "例: '今すぐ発注が必要な部品は？' '名古屋DCの在庫状況は？' "
                "'Renesas製MCUのリードタイム推移は？' '遅延している入荷は？'"
            ),
            warehouse_id=WAREHOUSE_ID,
            table_identifiers=genie_tables,
        )
        GENIE_SPACE_ID = genie_space.space_id
        print(f"  ✅ Genie スペース作成完了!")
        print(f"  Space ID: {GENIE_SPACE_ID}")

    except Exception as e:
        print(f"  ⚠️ Genie スペース作成スキップ: {e}")
        print(f"  → Genie は後で手動作成可能です")

# COMMAND ----------
# MAGIC %md ## ステップ 8/8: config.json 書き出し
# MAGIC
# MAGIC Streamlit アプリが Databricks に接続するための設定ファイルを書き出します。

# COMMAND ----------

import json

config = {
    "genie_space_id":     GENIE_SPACE_ID,
    "warehouse_id":       WAREHOUSE_ID,
    "catalog":            CATALOG,
    "schema":             SCHEMA,
    "procurement_table":  f"{CATALOG}.{SCHEMA}.gold_procurement_alerts",
    "warehouse_table":    f"{CATALOG}.{SCHEMA}.gold_inventory_by_warehouse",
    "lead_time_table":    f"{CATALOG}.{SCHEMA}.gold_lead_time_trend",
    "logistics_table":    f"{CATALOG}.{SCHEMA}.gold_logistics_status",
    "forecast_table":     f"{CATALOG}.{SCHEMA}.gold_demand_forecast",
}

config_json = json.dumps(config, ensure_ascii=False, indent=2)
config_path = f"{VOLUME_PATH}/config.json"
dbutils.fs.put(config_path, config_json, overwrite=True)

print("📄 config.json を書き出しました:")
print(config_json)
print(f"\n  保存先: {config_path}")

# COMMAND ----------
# MAGIC %md ## ✅ セットアップ完了！
# MAGIC
# MAGIC ### 作成されたリソース一覧

# COMMAND ----------

print("=" * 60)
print("  🎉 Supply Chain Management セットアップ完了!")
print("=" * 60)

# テーブル一覧
print("\n📋 作成されたテーブル:")
tables_df = spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`").collect()
for t in tables_df:
    layer = "🔶 Bronze" if "bronze" in t.tableName else "⬜ Silver" if "silver" in t.tableName else "🥇 Gold"
    count = spark.table(f"`{CATALOG}`.`{SCHEMA}`.`{t.tableName}`").count()
    print(f"  {layer}  {t.tableName:<40} {count:>8,} rows")

# ボリューム内容
print(f"\n📂 ボリューム ({VOLUME_PATH}):")
for item in dbutils.fs.ls(VOLUME_PATH):
    print(f"  {item.name}")
if dbutils.fs.ls(f"{VOLUME_PATH}/csv"):
    csv_count = len([f for f in dbutils.fs.ls(f"{VOLUME_PATH}/csv") if f.name.endswith(".csv")])
    print(f"  csv/ ({csv_count} files)")

# 次のステップ
print(f"""
{'=' * 60}
  📌 次のステップ: Databricks App のデプロイ
{'=' * 60}

  1. 左サイドバー → Compute → Apps → Create App
  2. App name: scm-demo
  3. Source: Git repository
     URL: (あなたの GitHub URL)
     Branch: main
     Path: scm
  4. 環境変数:
     SCM_CATALOG = {CATALOG}
     SCM_SCHEMA  = {SCHEMA}
  5. Deploy!

  Genie Space ID: {GENIE_SPACE_ID}
  Warehouse ID:   {WAREHOUSE_ID}
{'=' * 60}
""")
