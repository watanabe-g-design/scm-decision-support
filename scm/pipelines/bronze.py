"""
Lakeflow Declarative Pipeline — Bronze layer
============================================
Ingests 15 raw CSV files from the Unity Catalog Volume into Delta tables.
Minimal transformation: add `_ingested_at` timestamp, keep all source columns.

Source : /Volumes/{catalog}/{schema}/scm_data/csv/*.csv
Target : {catalog}.{schema}.bronze_*

Pipeline parameters (set in resources/scm_pipeline.yml):
  catalog, schema, volume_path
"""
import dlt
from pyspark.sql import functions as F

# ── Pipeline parameters ───────────────────────────────
VOLUME_PATH = spark.conf.get("scm.volume_path", "/Volumes/supply_chain_management/main/scm_data")
CSV_BASE = f"{VOLUME_PATH}/csv"


def _read_csv(filename: str):
    """Read a single CSV from the Volume with schema inference."""
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(f"{CSV_BASE}/{filename}")
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ── Master data ────────────────────────────────────────
@dlt.table(
    name="bronze_suppliers",
    comment="原始サプライヤーマスタ (CSV直取り込み)。supplier_id, supplier_name, country, region",
)
def bronze_suppliers():
    return _read_csv("suppliers.csv")


@dlt.table(
    name="bronze_customers",
    comment="原始顧客マスタ。customer_id, customer_name, segment, location",
)
def bronze_customers():
    return _read_csv("customers.csv")


@dlt.table(
    name="bronze_products",
    comment="原始製品マスタ。product_id, product_name, product_category, customer_id, unit_price_jpy",
)
def bronze_products():
    return _read_csv("products.csv")


@dlt.table(
    name="bronze_components",
    comment=(
        "原始部品マスタ。min_stock / max_stock を含む。"
        "component_id, part_number, component_name, component_category, supplier_id, "
        "base_lead_time_weeks, unit_price_usd, safety_stock_weeks, min_order_qty, min_stock, max_stock"
    ),
)
def bronze_components():
    return _read_csv("components.csv")


@dlt.table(
    name="bronze_warehouses",
    comment="原始倉庫マスタ。warehouse_id, warehouse_name, prefecture, city, latitude, longitude, capacity_sqm",
)
def bronze_warehouses():
    return _read_csv("warehouses.csv")


@dlt.table(
    name="bronze_bom",
    comment="BOM。product_id × component_id × quantity_per_unit",
)
def bronze_bom():
    return _read_csv("bom.csv")


@dlt.table(
    name="bronze_warehouse_components",
    comment="部品 × 倉庫の配置マスタ。is_primary, allocation_pct",
)
def bronze_warehouse_components():
    return _read_csv("warehouse_components.csv")


@dlt.table(
    name="bronze_shipment_routes",
    comment="出荷ルート (地図表示用)。route_type, from/to 座標, avg_transit_days",
)
def bronze_shipment_routes():
    return _read_csv("shipment_routes.csv")


# ── Transactional / time-series ───────────────────────
@dlt.table(
    name="bronze_forecasts",
    comment="需要予測。forecast_id, product_id, customer_id, forecast_month, forecast_qty, forecast_accuracy",
)
def bronze_forecasts():
    return _read_csv("forecasts.csv")


@dlt.table(
    name="bronze_lead_times",
    comment="リードタイム履歴。lead_time_id, component_id, supplier_id, effective_date, lead_time_weeks",
)
def bronze_lead_times():
    return _read_csv("lead_times.csv")


@dlt.table(
    name="bronze_inventory",
    comment="在庫履歴スナップショット (月次)。snapshot_id, component_id, warehouse_id, snapshot_month, stock_qty",
)
def bronze_inventory():
    return _read_csv("inventory.csv")


@dlt.table(
    name="bronze_inventory_current",
    comment="現時点在庫スナップショット (当月分)。意思決定の主軸となる顧客在庫データ",
)
def bronze_inventory_current():
    return _read_csv("inventory_current.csv")


@dlt.table(
    name="bronze_logistics",
    comment="物流実績。shipment_id, component_id, 出荷日, 到着予定日, 実到着日, status, delay_days, delay_cause",
)
def bronze_logistics():
    return _read_csv("logistics.csv")


@dlt.table(
    name="bronze_sales_orders",
    comment=(
        "受注明細。sales_order_id, customer_id, product_id, component_id, order_date, "
        "requested_delivery_date, response_date, earliest_ship_date, deadline_date, "
        "order_qty, shipped_qty, remaining_qty, component_required_qty, status, partial_available_qty"
    ),
)
def bronze_sales_orders():
    return _read_csv("sales_orders.csv")


@dlt.table(
    name="bronze_purchase_orders",
    comment=(
        "発注明細。purchase_order_id, component_id, supplier_id, order_date, expected_delivery_date, "
        "quantity, received_qty, outstanding_qty, status, is_delayed, delay_days"
    ),
)
def bronze_purchase_orders():
    return _read_csv("purchase_orders.csv")
