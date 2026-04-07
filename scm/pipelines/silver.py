"""
Lakeflow Declarative Pipeline — Silver layer
============================================
Cleansed, type-cast, enriched and validated tables.

Beyond type casting, this layer:
  - Denormalizes (joins supplier/component names into transactional tables)
  - Computes derived columns that multiple Gold tables reuse
    (lt_band, days_to_due, is_overdue, coverage_weeks, etc.)
  - Applies data-quality expectations (@dlt.expect_or_drop / @dlt.expect)
    so that bad rows are either dropped or flagged in Catalog Data Quality metrics.

Reads upstream `bronze_*` tables via `dlt.read("bronze_*")` which establishes
lineage in Unity Catalog automatically.
"""
import dlt
from pyspark.sql import functions as F

TODAY = spark.conf.get("scm.today_date", "2026-03-31")


# ══════════════════════════════════════════════════════
# Master data — cleansing + key validation
# ══════════════════════════════════════════════════════
@dlt.table(
    name="silver_suppliers",
    comment="クレンジング済みサプライヤー。supplier_id必須。",
)
@dlt.expect_or_drop("supplier_id_not_null", "supplier_id IS NOT NULL")
@dlt.expect("supplier_name_present", "supplier_name IS NOT NULL")
def silver_suppliers():
    return (
        dlt.read("bronze_suppliers")
        .select(
            F.col("supplier_id").cast("string").alias("supplier_id"),
            F.col("supplier_name").cast("string").alias("supplier_name"),
            F.col("country").cast("string").alias("country"),
            F.col("region").cast("string").alias("region"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["supplier_id"])
    )


@dlt.table(
    name="silver_customers",
    comment="クレンジング済み顧客。customer_id必須。",
)
@dlt.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
def silver_customers():
    return (
        dlt.read("bronze_customers")
        .select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("customer_name").cast("string").alias("customer_name"),
            F.col("segment").cast("string").alias("segment"),
            F.col("location").cast("string").alias("location"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["customer_id"])
    )


@dlt.table(
    name="silver_products",
    comment="クレンジング済み製品マスタ。unit_price_jpy を整数化。",
)
@dlt.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
def silver_products():
    return (
        dlt.read("bronze_products")
        .select(
            F.col("product_id").cast("string").alias("product_id"),
            F.col("product_name").cast("string").alias("product_name"),
            F.col("product_category").cast("string").alias("product_category"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("unit_price_jpy").cast("int").alias("unit_price_jpy"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["product_id"])
    )


@dlt.table(
    name="silver_components",
    comment=(
        "クレンジング済み部品マスタ + サプライヤー結合でsupplier_nameを付与。"
        "min_stock/max_stock を保持(Gold balance projection の必須列)。"
    ),
)
@dlt.expect_or_drop("component_id_not_null", "component_id IS NOT NULL")
@dlt.expect_or_drop("supplier_id_not_null", "supplier_id IS NOT NULL")
@dlt.expect("min_lt_positive", "base_lead_time_weeks > 0")
@dlt.expect("min_max_stock_sane", "min_stock IS NULL OR max_stock IS NULL OR max_stock >= min_stock")
def silver_components():
    comp = dlt.read("bronze_components").select(
        F.col("component_id").cast("string").alias("component_id"),
        F.col("part_number").cast("string").alias("part_number"),
        F.col("component_name").cast("string").alias("component_name"),
        F.col("component_category").cast("string").alias("component_category"),
        F.col("supplier_id").cast("string").alias("supplier_id"),
        F.col("base_lead_time_weeks").cast("int").alias("base_lead_time_weeks"),
        F.col("unit_price_usd").cast("double").alias("unit_price_usd"),
        F.col("safety_stock_weeks").cast("int").alias("safety_stock_weeks"),
        F.col("min_order_qty").cast("int").alias("min_order_qty"),
        F.col("min_stock").cast("int").alias("min_stock"),
        F.col("max_stock").cast("int").alias("max_stock"),
    )
    sup = dlt.read("silver_suppliers").select("supplier_id", "supplier_name")
    return (
        comp.join(sup, "supplier_id", "left")
        .withColumn("updated_at", F.current_timestamp())
        .dropDuplicates(["component_id"])
    )


@dlt.table(
    name="silver_warehouses",
    comment="倉庫マスタ。lat/lon/capacity を数値化。",
)
@dlt.expect_or_drop("warehouse_id_not_null", "warehouse_id IS NOT NULL")
@dlt.expect("lat_in_japan", "latitude BETWEEN 24 AND 46")
@dlt.expect("lon_in_japan", "longitude BETWEEN 122 AND 150")
def silver_warehouses():
    return (
        dlt.read("bronze_warehouses")
        .select(
            F.col("warehouse_id").cast("string").alias("warehouse_id"),
            F.col("warehouse_name").cast("string").alias("warehouse_name"),
            F.col("prefecture").cast("string").alias("prefecture"),
            F.col("city").cast("string").alias("city"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("capacity_sqm").cast("int").alias("capacity_sqm"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["warehouse_id"])
    )


@dlt.table(
    name="silver_bom",
    comment="BOM (製品→部品展開)。quantity_per_unit > 0 のみ保持。",
)
@dlt.expect_or_drop("bom_keys_present", "product_id IS NOT NULL AND component_id IS NOT NULL")
@dlt.expect_or_drop("bom_qty_positive", "quantity_per_unit > 0")
def silver_bom():
    return dlt.read("bronze_bom").select(
        F.col("product_id").cast("string").alias("product_id"),
        F.col("component_id").cast("string").alias("component_id"),
        F.col("quantity_per_unit").cast("int").alias("quantity_per_unit"),
    )


@dlt.table(
    name="silver_warehouse_components",
    comment="部品×倉庫の配置情報。is_primary, allocation_pct",
)
def silver_warehouse_components():
    return dlt.read("bronze_warehouse_components").select(
        F.col("component_id").cast("string").alias("component_id"),
        F.col("warehouse_id").cast("string").alias("warehouse_id"),
        F.col("is_primary").cast("boolean").alias("is_primary"),
        F.col("allocation_pct").cast("double").alias("allocation_pct"),
    )


@dlt.table(
    name="silver_shipment_routes",
    comment="地図表示用の出荷ルート。supplier→warehouse, warehouse→customer を含む。",
)
def silver_shipment_routes():
    return dlt.read("bronze_shipment_routes").select(
        F.col("route_type").cast("string").alias("route_type"),
        F.col("from_id").cast("string").alias("from_id"),
        F.col("from_name").cast("string").alias("from_name"),
        F.col("from_lat").cast("double").alias("from_lat"),
        F.col("from_lon").cast("double").alias("from_lon"),
        F.col("to_id").cast("string").alias("to_id"),
        F.col("to_name").cast("string").alias("to_name"),
        F.col("to_lat").cast("double").alias("to_lat"),
        F.col("to_lon").cast("double").alias("to_lon"),
        F.col("avg_transit_days").cast("int").alias("avg_transit_days"),
        F.col("monthly_shipments").cast("int").alias("monthly_shipments"),
    )


# ══════════════════════════════════════════════════════
# Transactional / time-series — parsing + enrichment
# ══════════════════════════════════════════════════════
@dlt.table(
    name="silver_forecasts",
    comment="需要予測。forecast_month をDATE型に、forecast_accuracy を0-1範囲で検証。",
)
@dlt.expect_or_drop("forecast_qty_non_negative", "forecast_qty >= 0")
@dlt.expect("accuracy_in_range", "forecast_accuracy IS NULL OR (forecast_accuracy BETWEEN 0 AND 1)")
def silver_forecasts():
    return dlt.read("bronze_forecasts").select(
        F.col("forecast_id").cast("string").alias("forecast_id"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.to_date(F.col("forecast_month")).alias("forecast_month"),
        F.col("forecast_qty").cast("int").alias("forecast_qty"),
        F.col("forecast_accuracy").cast("double").alias("forecast_accuracy"),
        F.to_timestamp(F.col("created_at")).alias("created_at"),
    )


@dlt.table(
    name="silver_lead_times",
    comment=(
        "リードタイム履歴 + lt_band 分類 (Short/Mid/Long/XLong) + 部品名結合。"
        "lead_time_weeks > 0 のみ保持。"
    ),
)
@dlt.expect_or_drop("lt_positive", "lead_time_weeks > 0")
@dlt.expect_or_drop("effective_date_valid", "effective_date IS NOT NULL")
def silver_lead_times():
    lt = dlt.read("bronze_lead_times").select(
        F.col("lead_time_id").cast("string").alias("lead_time_id"),
        F.col("component_id").cast("string").alias("component_id"),
        F.col("supplier_id").cast("string").alias("supplier_id"),
        F.to_date(F.col("effective_date")).alias("effective_date"),
        F.col("lead_time_weeks").cast("int").alias("lead_time_weeks"),
        F.col("change_reason").cast("string").alias("change_reason"),
        F.to_date(F.col("recorded_at")).alias("recorded_at"),
    )
    comp = dlt.read("silver_components").select(
        "component_id", "part_number", "component_name", "component_category"
    )
    sup = dlt.read("silver_suppliers").select("supplier_id", "supplier_name")
    return (
        lt.join(comp, "component_id", "left")
        .join(sup, "supplier_id", "left")
        .withColumn(
            "lt_band",
            F.when(F.col("lead_time_weeks") <= 13, F.lit("13週以内"))
            .when(F.col("lead_time_weeks") <= 26, F.lit("14週〜半年"))
            .when(F.col("lead_time_weeks") <= 52, F.lit("半年〜1年"))
            .when(F.col("lead_time_weeks") <= 78, F.lit("1年〜1.5年"))
            .otherwise(F.lit("1.5年〜2年")),
        )
    )


@dlt.table(
    name="silver_inventory",
    comment="月次在庫履歴。stock_qty >= 0 のみ。",
)
@dlt.expect_or_drop("stock_non_negative", "stock_qty >= 0")
def silver_inventory():
    return dlt.read("bronze_inventory").select(
        F.col("snapshot_id").cast("string").alias("snapshot_id"),
        F.col("component_id").cast("string").alias("component_id"),
        F.col("warehouse_id").cast("string").alias("warehouse_id"),
        F.to_date(F.col("snapshot_month")).alias("snapshot_month"),
        F.col("stock_qty").cast("int").alias("stock_qty"),
        F.col("safety_stock_qty").cast("int").alias("safety_stock_qty"),
        F.col("replenishment_qty").cast("int").alias("replenishment_qty"),
        F.col("demand_qty").cast("int").alias("demand_qty"),
    )


@dlt.table(
    name="silver_inventory_current",
    comment="現在在庫スナップショット。顧客在庫の基礎となる重要テーブル。",
)
@dlt.expect("component_id_present", "component_id IS NOT NULL")
def silver_inventory_current():
    return dlt.read("bronze_inventory_current").select(
        F.col("snapshot_id").cast("string").alias("snapshot_id"),
        F.col("component_id").cast("string").alias("component_id"),
        F.col("warehouse_id").cast("string").alias("warehouse_id"),
        F.to_date(F.col("snapshot_month")).alias("snapshot_month"),
        F.col("stock_qty").cast("int").alias("stock_qty"),
        F.col("safety_stock_qty").cast("int").alias("safety_stock_qty"),
        F.col("replenishment_qty").cast("int").alias("replenishment_qty"),
        F.col("demand_qty").cast("int").alias("demand_qty"),
    )


@dlt.table(
    name="silver_logistics",
    comment="物流実績。日付をDATE化、delay_days を数値化。",
)
def silver_logistics():
    return dlt.read("bronze_logistics").select(
        F.col("shipment_id").cast("string").alias("shipment_id"),
        F.col("component_id").cast("string").alias("component_id"),
        F.col("supplier_id").cast("string").alias("supplier_id"),
        F.col("destination_warehouse_id").cast("string").alias("destination_warehouse_id"),
        F.to_date(F.col("order_date")).alias("order_date"),
        F.to_date(F.col("expected_arrival_date")).alias("expected_arrival_date"),
        F.to_date(F.col("actual_arrival_date")).alias("actual_arrival_date"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("status").cast("string").alias("status"),
        F.col("delay_days").cast("int").alias("delay_days"),
        F.col("delay_cause").cast("string").alias("delay_cause"),
        F.col("unit_cost_usd").cast("double").alias("unit_cost_usd"),
    )


@dlt.table(
    name="silver_sales_orders",
    comment=(
        "受注明細 + days_to_due / is_overdue 派生列。"
        "response_date, earliest_ship_date, deadline_date, partial_available_qty "
        "など Gold が必要とするカラムを保持。"
    ),
)
@dlt.expect_or_drop("so_id_not_null", "sales_order_id IS NOT NULL")
@dlt.expect_or_drop("so_component_id_not_null", "component_id IS NOT NULL")
def silver_sales_orders():
    today = F.to_date(F.lit(TODAY))
    return dlt.read("bronze_sales_orders").select(
        F.col("sales_order_id").cast("string").alias("sales_order_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("customer_name").cast("string").alias("customer_name"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("component_id").cast("string").alias("component_id"),
        F.to_date(F.col("order_date")).alias("order_date"),
        F.to_date(F.col("requested_delivery_date")).alias("requested_delivery_date"),
        F.to_date(F.col("response_date")).alias("response_date"),
        F.to_date(F.col("earliest_ship_date")).alias("earliest_ship_date"),
        F.to_date(F.col("deadline_date")).alias("deadline_date"),
        F.col("order_qty").cast("int").alias("order_qty"),
        F.col("shipped_qty").cast("int").alias("shipped_qty"),
        F.col("remaining_qty").cast("int").alias("remaining_qty"),
        F.col("component_required_qty").cast("int").alias("component_required_qty"),
        F.col("partial_available_qty").cast("int").alias("partial_available_qty"),
        F.col("status").cast("string").alias("status"),
        F.col("priority_flag").cast("boolean").alias("priority_flag"),
    ).withColumn(
        "days_to_due", F.datediff(F.col("requested_delivery_date"), today)
    ).withColumn(
        "is_overdue", F.col("days_to_due") < 0
    )


@dlt.table(
    name="silver_purchase_orders",
    comment="発注明細 + is_delayed 再計算。outstanding_qty >= 0 のみ。",
)
@dlt.expect_or_drop("po_id_not_null", "purchase_order_id IS NOT NULL")
@dlt.expect_or_drop("outstanding_non_negative", "outstanding_qty >= 0")
def silver_purchase_orders():
    today = F.to_date(F.lit(TODAY))
    return dlt.read("bronze_purchase_orders").select(
        F.col("purchase_order_id").cast("string").alias("purchase_order_id"),
        F.col("component_id").cast("string").alias("component_id"),
        F.col("supplier_id").cast("string").alias("supplier_id"),
        F.to_date(F.col("order_date")).alias("order_date"),
        F.to_date(F.col("expected_delivery_date")).alias("expected_delivery_date"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("received_qty").cast("int").alias("received_qty"),
        F.col("outstanding_qty").cast("int").alias("outstanding_qty"),
        F.col("status").cast("string").alias("status"),
        F.col("is_delayed").cast("boolean").alias("is_delayed"),
        F.col("delay_days").cast("int").alias("delay_days"),
        F.col("unit_cost_usd").cast("double").alias("unit_cost_usd"),
        F.col("total_cost_usd").cast("double").alias("total_cost_usd"),
    ).withColumn(
        "days_to_expected", F.datediff(F.col("expected_delivery_date"), today)
    )
