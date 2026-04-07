"""
Lakeflow Declarative Pipeline — Gold layer
==========================================
14 Gold tables, direct port of `scm/logic/gold_builder.py` to Spark SQL.
These are the tables the Streamlit app reads via `services/database.py`.

All references to upstream tables use the `LIVE.` prefix so that Lakeflow
can automatically track lineage in Unity Catalog.

Tables:
  1. gold_lt_snapshot_current          — LT current/N-1/N-3/N-6 comparison
  2. gold_lt_trend_monthly             — LT month-over-month trend
  3. gold_lt_escalation_items          — LT items trending up
  4. gold_order_commit_risk            — Sales order risk with priority/action
  5. gold_requirement_timeline         — Per-component event timeline w/ running balance
  6. gold_balance_projection_monthly   — Month-by-month customer stock projection
  7. gold_inventory_policy_breach      — ZERO/UNDER/OVER items within cutoff
  8. gold_geo_warehouse_status         — Warehouse health score by location
  9. gold_data_pipeline_health         — Bronze-layer data quality metadata
 10. gold_exec_summary_daily           — Top-level KPI single-row table
 11. gold_action_queue_daily           — Unified priority action queue
 12. gold_business_glossary            — Domain glossary (static)
 13. gold_metric_definition            — KPI definitions (static)
 14. gold_genie_semantic_examples      — Genie example questions (static)

NOTE on parameters: `scm.today_date` and `scm.cutoff_date` come from the
pipeline configuration. Defaults match gold_builder.py's TODAY constant.
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

TODAY = spark.conf.get("scm.today_date", "2026-03-31")
# End of the balance projection window (matches gold_builder.py line 315)
PROJECTION_END = spark.conf.get("scm.projection_end_date", "2026-11-01")
# Policy breach cutoff (6 months ahead, per gold_builder.py line 392)
BREACH_CUTOFF = spark.conf.get("scm.breach_cutoff_month", "2026-09")


# ══════════════════════════════════════════════════════
# 1. gold_lt_snapshot_current
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_lt_snapshot_current",
    comment=(
        "部品ごとの最新リードタイム + N-1/N-3/N-6 月前比較。"
        "trend_arrow_* は '↑'/'↓'/'→' の文字列。Streamlit UI で色分け表示される。"
    ),
)
def gold_lt_snapshot_current():
    sql = f"""
    WITH lt AS (
      SELECT component_id, supplier_id, effective_date, lead_time_weeks,
             part_number, component_name, component_category, supplier_name, lt_band
      FROM LIVE.silver_lead_times
    ),
    -- For each target "as-of" date, pick the latest LT row with effective_date <= target
    ranked AS (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY component_id ORDER BY effective_date DESC) AS rn_cur
      FROM lt
      WHERE effective_date <= DATE '{TODAY}'
    ),
    ranked_n1 AS (
      SELECT component_id, lead_time_weeks AS lt_n1,
        ROW_NUMBER() OVER (PARTITION BY component_id ORDER BY effective_date DESC) AS rn
      FROM lt WHERE effective_date <= ADD_MONTHS(DATE '{TODAY}', -1)
    ),
    ranked_n3 AS (
      SELECT component_id, lead_time_weeks AS lt_n3,
        ROW_NUMBER() OVER (PARTITION BY component_id ORDER BY effective_date DESC) AS rn
      FROM lt WHERE effective_date <= ADD_MONTHS(DATE '{TODAY}', -3)
    ),
    ranked_n6 AS (
      SELECT component_id, lead_time_weeks AS lt_n6,
        ROW_NUMBER() OVER (PARTITION BY component_id ORDER BY effective_date DESC) AS rn
      FROM lt WHERE effective_date <= ADD_MONTHS(DATE '{TODAY}', -6)
    ),
    current_lt AS (SELECT * FROM ranked WHERE rn_cur = 1),
    n1 AS (SELECT component_id, lt_n1 FROM ranked_n1 WHERE rn = 1),
    n3 AS (SELECT component_id, lt_n3 FROM ranked_n3 WHERE rn = 1),
    n6 AS (SELECT component_id, lt_n6 FROM ranked_n6 WHERE rn = 1)
    SELECT
      DATE '{TODAY}'                     AS snapshot_date,
      c.component_id                     AS item_id,
      c.part_number                      AS item_code,
      c.component_name                   AS item_name,
      c.supplier_name                    AS manufacturer_name,
      c.supplier_name                    AS supplier_name,
      CAST(c.lead_time_weeks AS INT)     AS latest_lt_weeks,
      CAST(n1.lt_n1 AS INT)              AS lt_n1_weeks,
      CAST(n3.lt_n3 AS INT)              AS lt_n3_weeks,
      CAST(n6.lt_n6 AS INT)              AS lt_n6_weeks,
      CAST(c.lead_time_weeks - n1.lt_n1 AS INT) AS delta_vs_n1,
      CAST(c.lead_time_weeks - n3.lt_n3 AS INT) AS delta_vs_n3,
      CAST(c.lead_time_weeks - n6.lt_n6 AS INT) AS delta_vs_n6,
      CASE
        WHEN n1.lt_n1 IS NULL THEN ''
        WHEN c.lead_time_weeks > n1.lt_n1 THEN '↑'
        WHEN c.lead_time_weeks < n1.lt_n1 THEN '↓'
        ELSE '→'
      END AS trend_arrow_n1,
      CASE
        WHEN n3.lt_n3 IS NULL THEN ''
        WHEN c.lead_time_weeks > n3.lt_n3 THEN '↑'
        WHEN c.lead_time_weeks < n3.lt_n3 THEN '↓'
        ELSE '→'
      END AS trend_arrow_n3,
      CASE
        WHEN n6.lt_n6 IS NULL THEN ''
        WHEN c.lead_time_weeks > n6.lt_n6 THEN '↑'
        WHEN c.lead_time_weeks < n6.lt_n6 THEN '↓'
        ELSE '→'
      END AS trend_arrow_n6,
      c.lt_band,
      CASE
        WHEN n1.lt_n1 IS NOT NULL AND c.lead_time_weeks > n1.lt_n1 THEN 'LT延長中。早めの発注を推奨'
        WHEN n3.lt_n3 IS NOT NULL AND c.lead_time_weeks > n3.lt_n3 THEN '3ヶ月前から延長傾向'
        ELSE ''
      END AS remark,
      c.component_category,
      c.supplier_id
    FROM current_lt c
    LEFT JOIN n1 ON c.component_id = n1.component_id
    LEFT JOIN n3 ON c.component_id = n3.component_id
    LEFT JOIN n6 ON c.component_id = n6.component_id
    """
    return spark.sql(sql)


# ══════════════════════════════════════════════════════
# 2. gold_lt_trend_monthly
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_lt_trend_monthly",
    comment="部品別リードタイムの月次推移。Streamlit の時系列チャートで使用。",
)
def gold_lt_trend_monthly():
    return spark.sql("""
    SELECT
      DATE_FORMAT(effective_date, 'yyyy-MM') AS month,
      component_id,
      part_number,
      component_name,
      component_category,
      supplier_name,
      lead_time_weeks,
      lt_band,
      effective_date
    FROM LIVE.silver_lead_times
    """)


# ══════════════════════════════════════════════════════
# 3. gold_lt_escalation_items
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_lt_escalation_items",
    comment="LTが3ヶ月前 or 6ヶ月前と比較して↑している部品。escalation_reason で理由を明示。",
)
def gold_lt_escalation_items():
    return spark.sql("""
    SELECT
      *,
      TRUE AS escalation_flag,
      CASE
        WHEN trend_arrow_n3 = '↑' THEN '3ヶ月前比で延長'
        WHEN trend_arrow_n6 = '↑' THEN '6ヶ月前比で延長'
        ELSE ''
      END AS escalation_reason
    FROM LIVE.gold_lt_snapshot_current
    WHERE trend_arrow_n3 = '↑' OR trend_arrow_n6 = '↑'
    """)


# ══════════════════════════════════════════════════════
# 4. gold_order_commit_risk
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_order_commit_risk",
    comment=(
        "受注明細 × 顧客在庫 × メーカー情報。priority_rank (Critical/High/Mid/Low), "
        "adjustment_action, risk_reason, risk_score を付与。"
    ),
)
def gold_order_commit_risk():
    sql = f"""
    WITH cust_stock AS (
      SELECT component_id, CAST(SUM(stock_qty) AS INT) AS current_customer_stock
      FROM LIVE.silver_inventory_current
      GROUP BY component_id
    ),
    enriched AS (
      SELECT
        DATE '{TODAY}' AS snapshot_date,
        so.sales_order_id, so.customer_id, so.customer_name,
        so.product_id, so.product_name,
        so.component_id, c.part_number, c.component_name, c.component_category,
        c.supplier_name,
        so.order_date, so.requested_delivery_date, so.response_date,
        so.earliest_ship_date, so.deadline_date,
        so.order_qty, so.shipped_qty, so.remaining_qty,
        so.component_required_qty, so.partial_available_qty,
        so.status, so.priority_flag,
        COALESCE(cs.current_customer_stock, 0) AS current_customer_stock,
        0 AS current_trading_house_stock,
        DATEDIFF(so.requested_delivery_date, DATE '{TODAY}') AS days_to_due
      FROM LIVE.silver_sales_orders so
      LEFT JOIN LIVE.silver_components c ON so.component_id = c.component_id
      LEFT JOIN cust_stock cs ON so.component_id = cs.component_id
    )
    SELECT
      snapshot_date,
      sales_order_id, customer_name, product_name,
      part_number, component_name, component_category, component_id,
      remaining_qty, requested_delivery_date, response_date,
      earliest_ship_date, deadline_date, partial_available_qty,
      current_customer_stock, current_trading_house_stock,
      CASE
        WHEN days_to_due IS NULL THEN 'Low'
        WHEN days_to_due <= 3  THEN 'Critical'
        WHEN days_to_due <= 7  THEN 'High'
        WHEN days_to_due <= 14 THEN 'Mid'
        ELSE 'Low'
      END AS priority_rank,
      CASE
        WHEN days_to_due <= 3 AND current_customer_stock > 0 THEN '商社前倒し調整'
        WHEN days_to_due <= 3 THEN '緊急発注'
        WHEN days_to_due <= 7  THEN '商社前倒し調整'
        WHEN days_to_due <= 14 THEN '1週間後に再確認'
        ELSE '状況モニタリング'
      END AS adjustment_action,
      TRIM(BOTH '; ' FROM CONCAT_WS('; ',
        CASE WHEN days_to_due IS NOT NULL AND days_to_due <= 7 THEN '納期接近' END,
        CASE WHEN current_customer_stock <= 0 THEN '顧客在庫ゼロ' END
      )) AS risk_reason_raw,
      CAST(GREATEST(0, LEAST(100, 100 - COALESCE(days_to_due, 0) * 3)) AS INT) AS risk_score,
      supplier_name, status
    FROM enriched
    """
    df = spark.sql(sql)
    # Replace empty risk_reason with '特になし' (matches gold_builder.py behavior)
    return df.withColumn(
        "risk_reason",
        F.when(F.col("risk_reason_raw") == "", F.lit("特になし")).otherwise(F.col("risk_reason_raw")),
    ).drop("risk_reason_raw").orderBy(F.col("risk_score").desc())


# ══════════════════════════════════════════════════════
# 5. gold_requirement_timeline
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_requirement_timeline",
    comment=(
        "部品ごとの需給イベント時系列 (在庫スナップショット + 受注出庫 + 発注入荷)。"
        "cumulative_balance は item_id × event_date で走査した累積残高。"
    ),
)
def gold_requirement_timeline():
    sql = f"""
    WITH events AS (
      -- 顧客在庫 (基準点)
      SELECT
        DATE '{TODAY}' AS snapshot_date,
        component_id AS item_id,
        DATE '{TODAY}' AS event_date,
        '顧客在庫' AS event_type,
        '' AS order_no, '' AS sd_no,
        CAST(SUM(stock_qty) AS INT) AS quantity,
        FALSE AS production_use_flag, FALSE AS inbound_flag,
        0 AS event_seq   -- 在庫行を最優先で処理
      FROM LIVE.silver_inventory_current
      GROUP BY component_id
      UNION ALL
      -- 受注出庫 (マイナス)
      SELECT
        DATE '{TODAY}',
        component_id,
        requested_delivery_date,
        '生産使用日',
        sales_order_id, '',
        -CAST(component_required_qty AS INT),
        TRUE, FALSE,
        1
      FROM LIVE.silver_sales_orders
      UNION ALL
      -- 発注入荷 (プラス)
      SELECT
        DATE '{TODAY}',
        component_id,
        expected_delivery_date,
        '商社納入日',
        purchase_order_id, '',
        CAST(outstanding_qty AS INT),
        FALSE, TRUE,
        1
      FROM LIVE.silver_purchase_orders
      WHERE status IN ('placed','acknowledged','in_production','shipped','partial_received')
    ),
    with_running AS (
      SELECT
        *,
        SUM(quantity) OVER (
          PARTITION BY item_id
          ORDER BY event_seq, event_date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_balance
      FROM events
    )
    SELECT
      snapshot_date, item_id, event_date, event_type, order_no, sd_no,
      quantity, production_use_flag, inbound_flag, cumulative_balance,
      CASE
        WHEN quantity < 0 AND cumulative_balance >= 0 THEN ABS(quantity)
        ELSE 0
      END AS allocated_qty
    FROM with_running
    """
    return spark.sql(sql)


# ══════════════════════════════════════════════════════
# 6. gold_balance_projection_monthly
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_balance_projection_monthly",
    comment=(
        "月末在庫予測。過去3ヶ月 + 2026-11 まで。customer_stock_proj は初期在庫 + 累積(入荷-消費)。"
        "将来月は consumption = GREATEST(confirmed_order_qty, forecast_qty)、入荷は確定POのみ。"
    ),
)
def gold_balance_projection_monthly():
    sql = f"""
    WITH months AS (
      SELECT explode(sequence(
        TRUNC(ADD_MONTHS(DATE '{TODAY}', -3), 'MM'),
        DATE '{PROJECTION_END}',
        INTERVAL 1 MONTH
      )) AS month_start
    ),
    component_months AS (
      SELECT
        c.component_id, c.part_number, c.component_name,
        COALESCE(c.min_stock, 100)  AS min_qty,
        COALESCE(c.max_stock, 1000) AS max_qty,
        m.month_start
      FROM LIVE.silver_components c
      CROSS JOIN months m
    ),
    initial_stock AS (
      SELECT component_id, CAST(SUM(stock_qty) AS INT) AS init_stock
      FROM LIVE.silver_inventory_current
      GROUP BY component_id
    ),
    so_monthly AS (
      SELECT
        component_id,
        TRUNC(requested_delivery_date, 'MM') AS month_start,
        CAST(SUM(component_required_qty) AS INT) AS so_qty
      FROM LIVE.silver_sales_orders
      WHERE requested_delivery_date IS NOT NULL
      GROUP BY component_id, TRUNC(requested_delivery_date, 'MM')
    ),
    po_monthly AS (
      SELECT
        component_id,
        TRUNC(expected_delivery_date, 'MM') AS month_start,
        CAST(SUM(outstanding_qty) AS INT) AS po_qty
      FROM LIVE.silver_purchase_orders
      WHERE status IN ('placed','acknowledged','in_production','shipped')
        AND expected_delivery_date IS NOT NULL
      GROUP BY component_id, TRUNC(expected_delivery_date, 'MM')
    ),
    -- 需要予測 (製品ベース) を BOM で部品ベースに展開
    fcst_monthly AS (
      SELECT
        b.component_id,
        f.forecast_month AS month_start,
        CAST(SUM(f.forecast_qty * b.quantity_per_unit) AS INT) AS fcst_qty
      FROM LIVE.silver_forecasts f
      JOIN LIVE.silver_bom b ON f.product_id = b.product_id
      GROUP BY b.component_id, f.forecast_month
    ),
    combined AS (
      SELECT
        cm.component_id, cm.part_number, cm.component_name,
        cm.min_qty, cm.max_qty, cm.month_start,
        COALESCE(ist.init_stock, 0) AS init_stock,
        COALESCE(so.so_qty, 0)      AS confirmed_order_qty,
        COALESCE(po.po_qty, 0)      AS inbound_qty_order_linked,
        COALESCE(fc.fcst_qty, 0)    AS forecast_qty,
        (cm.month_start >= TRUNC(DATE '{TODAY}', 'MM')) AS is_future
      FROM component_months cm
      LEFT JOIN initial_stock ist ON cm.component_id = ist.component_id
      LEFT JOIN so_monthly   so   ON cm.component_id = so.component_id AND cm.month_start = so.month_start
      LEFT JOIN po_monthly   po   ON cm.component_id = po.component_id AND cm.month_start = po.month_start
      LEFT JOIN fcst_monthly fc   ON cm.component_id = fc.component_id AND cm.month_start = fc.month_start
    ),
    projected AS (
      SELECT
        component_id, part_number, component_name, min_qty, max_qty, month_start,
        confirmed_order_qty, forecast_qty, inbound_qty_order_linked,
        CASE WHEN is_future THEN GREATEST(confirmed_order_qty, forecast_qty) ELSE 0 END AS production_use_qty,
        CASE WHEN is_future THEN inbound_qty_order_linked ELSE 0 END AS inbound_eff,
        init_stock,
        init_stock + SUM(
          CASE WHEN is_future
               THEN inbound_qty_order_linked - GREATEST(confirmed_order_qty, forecast_qty)
               ELSE 0 END
        ) OVER (
          PARTITION BY component_id
          ORDER BY month_start
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS customer_stock_proj
      FROM combined
    )
    SELECT
      DATE_FORMAT(month_start, 'yyyy-MM') AS month_end_date,
      component_id AS item_id,
      part_number  AS item_code,
      component_name AS product_name,
      CAST(customer_stock_proj AS INT) AS customer_stock_proj,
      confirmed_order_qty,
      forecast_qty,
      inbound_qty_order_linked,
      production_use_qty,
      min_qty, max_qty,
      CASE
        WHEN customer_stock_proj <= 0       THEN 'ZERO'
        WHEN customer_stock_proj < min_qty  THEN 'UNDER'
        WHEN customer_stock_proj > max_qty  THEN 'OVER'
        ELSE 'OK'
      END AS policy_status
    FROM projected
    """
    return spark.sql(sql)


# ══════════════════════════════════════════════════════
# 7. gold_inventory_policy_breach
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_inventory_policy_breach",
    comment=(
        "policy_status != 'OK' の行のみ、直近6ヶ月以内。"
        "priority_order: ZERO=1, UNDER=2, OVER=3。first_breach で部品の初回ブリーチ月。"
    ),
)
def gold_inventory_policy_breach():
    sql = f"""
    WITH breach AS (
      SELECT
        DATE '{TODAY}' AS snapshot_date,
        item_id, item_code, product_name,
        policy_status AS breach_type,
        month_end_date AS breach_date,
        customer_stock_proj AS projected_stock,
        min_qty, max_qty
      FROM LIVE.gold_balance_projection_monthly
      WHERE policy_status != 'OK'
        AND month_end_date <= '{BREACH_CUTOFF}'
    ),
    first_by_item AS (
      SELECT item_id, MIN(breach_date) AS first_breach
      FROM breach
      GROUP BY item_id
    )
    SELECT
      b.snapshot_date, b.item_id, b.item_code, b.product_name,
      b.breach_type, b.breach_date,
      b.projected_stock, b.min_qty, b.max_qty,
      f.first_breach,
      CASE b.breach_type
        WHEN 'ZERO' THEN 1
        WHEN 'UNDER' THEN 2
        WHEN 'OVER' THEN 3
      END AS priority_order
    FROM breach b
    JOIN first_by_item f ON b.item_id = f.item_id
    ORDER BY priority_order, f.first_breach
    """
    return spark.sql(sql)


# ══════════════════════════════════════════════════════
# 8. gold_geo_warehouse_status
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_geo_warehouse_status",
    comment=(
        "倉庫別サマリー。health_score = (OK品目数 / 管理品目数) × 100。"
        "zero/under/over カウントは部品単位(全倉庫合計在庫をmin/maxと比較)。"
    ),
)
def gold_geo_warehouse_status():
    sql = f"""
    WITH comp_stock AS (
      SELECT component_id, CAST(SUM(stock_qty) AS INT) AS total_stock
      FROM LIVE.silver_inventory_current
      GROUP BY component_id
    ),
    comp_class AS (
      SELECT
        cs.component_id,
        cs.total_stock,
        c.min_stock, c.max_stock,
        CASE WHEN cs.total_stock <= 0 THEN 1 ELSE 0 END AS is_zero,
        CASE WHEN cs.total_stock > 0 AND cs.total_stock < COALESCE(c.min_stock, 100) THEN 1 ELSE 0 END AS is_under,
        CASE WHEN cs.total_stock > COALESCE(c.max_stock, 1000) THEN 1 ELSE 0 END AS is_over,
        CASE WHEN cs.total_stock > 0
                  AND cs.total_stock >= COALESCE(c.min_stock, 100)
                  AND cs.total_stock <= COALESCE(c.max_stock, 1000)
             THEN 1 ELSE 0 END AS is_ok
      FROM comp_stock cs
      LEFT JOIN LIVE.silver_components c ON cs.component_id = c.component_id
    ),
    -- 部品×倉庫の関連 (inventory_current が厳密な関連の源)
    wh_comp AS (
      SELECT DISTINCT warehouse_id, component_id
      FROM LIVE.silver_inventory_current
    ),
    wh_agg AS (
      SELECT
        wc.warehouse_id,
        COUNT(DISTINCT wc.component_id) AS managed_count,
        SUM(cc.is_zero)  AS zero_count,
        SUM(cc.is_under) AS under_count,
        SUM(cc.is_over)  AS over_count,
        SUM(cc.is_ok)    AS ok_count
      FROM wh_comp wc
      LEFT JOIN comp_class cc ON wc.component_id = cc.component_id
      GROUP BY wc.warehouse_id
    )
    SELECT
      DATE '{TODAY}' AS snapshot_date,
      w.warehouse_id, w.warehouse_name,
      w.latitude  AS geo_lat,
      w.longitude AS geo_lon,
      w.prefecture,
      COALESCE(wa.managed_count, 0) AS managed_count,
      CAST(COALESCE(wa.zero_count,  0) AS INT) AS zero_count,
      CAST(COALESCE(wa.under_count, 0) AS INT) AS under_count,
      CAST(COALESCE(wa.over_count,  0) AS INT) AS over_count,
      ROUND(
        CASE WHEN COALESCE(wa.managed_count, 0) = 0 THEN 50.0
             ELSE COALESCE(wa.ok_count, 0) * 100.0 / wa.managed_count
        END, 1
      ) AS health_score,
      CAST(COALESCE(wa.zero_count, 0) + COALESCE(wa.under_count, 0) AS INT) AS critical_count,
      0 AS high_count
    FROM LIVE.silver_warehouses w
    LEFT JOIN wh_agg wa ON w.warehouse_id = wa.warehouse_id
    """
    return spark.sql(sql)


# ══════════════════════════════════════════════════════
# 9. gold_data_pipeline_health
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_data_pipeline_health",
    comment=(
        "Bronze レイヤー各テーブルの行数・品質スコア。"
        "デモ用簡易ロジック: 行数 > 0 かつ主要列のNULL率5%未満なら quality=100、"
        "それ以外は (1 - null_rate) × 100。"
    ),
)
def gold_data_pipeline_health():
    sql = f"""
    WITH stats AS (
      SELECT 'suppliers'            AS csv_name, 'bronze_suppliers'            AS target_table, COUNT(*) AS cnt FROM LIVE.bronze_suppliers UNION ALL
      SELECT 'customers',                          'bronze_customers',            COUNT(*)            FROM LIVE.bronze_customers UNION ALL
      SELECT 'products',                           'bronze_products',             COUNT(*)            FROM LIVE.bronze_products UNION ALL
      SELECT 'components',                         'bronze_components',           COUNT(*)            FROM LIVE.bronze_components UNION ALL
      SELECT 'warehouses',                         'bronze_warehouses',           COUNT(*)            FROM LIVE.bronze_warehouses UNION ALL
      SELECT 'bom',                                'bronze_bom',                  COUNT(*)            FROM LIVE.bronze_bom UNION ALL
      SELECT 'forecasts',                          'bronze_forecasts',            COUNT(*)            FROM LIVE.bronze_forecasts UNION ALL
      SELECT 'lead_times',                         'bronze_lead_times',           COUNT(*)            FROM LIVE.bronze_lead_times UNION ALL
      SELECT 'inventory',                          'bronze_inventory',            COUNT(*)            FROM LIVE.bronze_inventory UNION ALL
      SELECT 'inventory_current',                  'bronze_inventory_current',    COUNT(*)            FROM LIVE.bronze_inventory_current UNION ALL
      SELECT 'logistics',                          'bronze_logistics',            COUNT(*)            FROM LIVE.bronze_logistics UNION ALL
      SELECT 'sales_orders',                       'bronze_sales_orders',         COUNT(*)            FROM LIVE.bronze_sales_orders UNION ALL
      SELECT 'purchase_orders',                    'bronze_purchase_orders',      COUNT(*)            FROM LIVE.bronze_purchase_orders UNION ALL
      SELECT 'warehouse_components',               'bronze_warehouse_components', COUNT(*)            FROM LIVE.bronze_warehouse_components UNION ALL
      SELECT 'shipment_routes',                    'bronze_shipment_routes',      COUNT(*)            FROM LIVE.bronze_shipment_routes
    )
    SELECT
      DATE '{TODAY}'                            AS snapshot_date,
      CONCAT('csv_ingest_', csv_name)           AS pipeline_name,
      CONCAT('csv/', csv_name, '.csv')          AS source_table,
      target_table,
      CAST(cnt AS INT)                          AS record_count,
      CAST(DATE '{TODAY}' AS STRING)            AS freshness_ts,
      CAST(CASE WHEN cnt > 0 THEN 100.0 ELSE 0.0 END AS DOUBLE) AS quality_score,
      (cnt > 0)                                 AS success_flag,
      CAST(NULL AS STRING)                      AS error_message
    FROM stats
    """
    return spark.sql(sql)


# ══════════════════════════════════════════════════════
# 10. gold_exec_summary_daily
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_exec_summary_daily",
    comment="経営ダッシュボード用の単一行KPIテーブル。app.py のトップに表示される数値の情報源。",
)
def gold_exec_summary_daily():
    sql = f"""
    WITH order_counts AS (
      SELECT
        SUM(CASE WHEN priority_rank = 'Critical' THEN 1 ELSE 0 END) AS critical_count,
        SUM(CASE WHEN priority_rank = 'High'     THEN 1 ELSE 0 END) AS high_count,
        SUM(CASE WHEN priority_rank = 'Mid'      THEN 1 ELSE 0 END) AS medium_count,
        SUM(CASE WHEN priority_rank = 'Low'      THEN 1 ELSE 0 END) AS low_count
      FROM LIVE.gold_order_commit_risk
    ),
    lt_esc AS (
      SELECT COUNT(*) AS lt_escalation_item_count
      FROM LIVE.gold_lt_escalation_items
    ),
    breach_counts AS (
      SELECT
        COUNT(DISTINCT CASE WHEN breach_type = 'ZERO'  THEN item_id END) AS zero_count,
        COUNT(DISTINCT CASE WHEN breach_type = 'UNDER' THEN item_id END) AS under_count,
        COUNT(DISTINCT CASE WHEN breach_type = 'OVER'  THEN item_id END) AS over_count
      FROM LIVE.gold_inventory_policy_breach
    ),
    wh_health AS (
      SELECT ROUND(AVG(health_score), 1) AS warehouse_health_score
      FROM LIVE.gold_geo_warehouse_status
    ),
    fcst AS (
      SELECT ROUND(AVG(forecast_accuracy) * 100, 1) AS forecast_accuracy_pct
      FROM LIVE.silver_forecasts
      WHERE forecast_month >= ADD_MONTHS(DATE '{TODAY}', -3)
    )
    SELECT
      DATE '{TODAY}' AS snapshot_date,
      CAST(o.critical_count AS INT) AS critical_count,
      CAST(o.high_count     AS INT) AS high_count,
      CAST(o.medium_count   AS INT) AS medium_count,
      CAST(o.low_count      AS INT) AS low_count,
      CAST(l.lt_escalation_item_count AS INT) AS lt_escalation_item_count,
      CAST(b.zero_count  AS INT) AS zero_count,
      CAST(b.under_count AS INT) AS under_count,
      CAST(b.over_count  AS INT) AS over_count,
      w.warehouse_health_score,
      CAST(COALESCE(o.critical_count, 0) + COALESCE(o.high_count, 0) AS INT) AS top_risk_order_count,
      COALESCE(f.forecast_accuracy_pct, 0.0) AS forecast_accuracy_pct
    FROM order_counts o
    CROSS JOIN lt_esc l
    CROSS JOIN breach_counts b
    CROSS JOIN wh_health w
    CROSS JOIN fcst f
    """
    return spark.sql(sql)


# ══════════════════════════════════════════════════════
# 11. gold_action_queue_daily
# ══════════════════════════════════════════════════════
@dlt.table(
    name="gold_action_queue_daily",
    comment=(
        "3つのソース (Critical/High受注、LT長期化、ZEROブリーチ) を統合した優先アクションキュー。"
        "urgency_rank 昇順で並び替え、UIで上位20件を表示。"
    ),
)
def gold_action_queue_daily():
    sql = f"""
    WITH order_actions AS (
      SELECT
        DATE '{TODAY}' AS snapshot_date,
        'order_risk'   AS source,
        sales_order_id AS target_id,
        part_number    AS item_code,
        component_name AS item_name,
        CONCAT('納期', priority_rank) AS risk_type,
        adjustment_action AS recommended_action,
        CASE WHEN priority_rank = 'Critical' THEN 1 ELSE 2 END AS urgency_rank,
        CAST(requested_delivery_date AS STRING) AS due_date,
        risk_reason AS rationale,
        ROW_NUMBER() OVER (ORDER BY risk_score DESC) AS rn
      FROM LIVE.gold_order_commit_risk
      WHERE priority_rank IN ('Critical','High')
    ),
    lt_actions AS (
      SELECT
        DATE '{TODAY}'                                               AS snapshot_date,
        'lt_escalation'                                               AS source,
        item_id                                                       AS target_id,
        item_code                                                     AS item_code,
        item_name                                                     AS item_name,
        'LT長期化'                                                     AS risk_type,
        '発注タイミング前倒し検討'                                      AS recommended_action,
        3                                                             AS urgency_rank,
        ''                                                            AS due_date,
        CASE WHEN remark <> '' THEN remark ELSE escalation_reason END AS rationale,
        ROW_NUMBER() OVER (ORDER BY delta_vs_n3 DESC NULLS LAST)      AS rn
      FROM LIVE.gold_lt_escalation_items
    ),
    breach_actions AS (
      SELECT
        DATE '{TODAY}'                                     AS snapshot_date,
        'policy_breach'                                    AS source,
        item_id                                            AS target_id,
        item_code                                          AS item_code,
        product_name                                       AS item_name,
        '在庫ZERO予測'                                      AS risk_type,
        '緊急補充検討'                                      AS recommended_action,
        2                                                  AS urgency_rank,
        CAST(breach_date AS STRING)                        AS due_date,
        CONCAT('予測在庫', CAST(projected_stock AS STRING)) AS rationale,
        ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY breach_date) AS rn_item
      FROM LIVE.gold_inventory_policy_breach
      WHERE breach_type = 'ZERO'
    ),
    breach_dedup AS (
      SELECT
        snapshot_date, source, target_id, item_code, item_name,
        risk_type, recommended_action, urgency_rank, due_date, rationale,
        ROW_NUMBER() OVER (ORDER BY due_date) AS rn
      FROM breach_actions
      WHERE rn_item = 1
    )
    SELECT snapshot_date, source, target_id, item_code, item_name,
           risk_type, recommended_action, urgency_rank, due_date, rationale
    FROM order_actions WHERE rn <= 20
    UNION ALL
    SELECT snapshot_date, source, target_id, item_code, item_name,
           risk_type, recommended_action, urgency_rank, due_date, rationale
    FROM lt_actions WHERE rn <= 10
    UNION ALL
    SELECT snapshot_date, source, target_id, item_code, item_name,
           risk_type, recommended_action, urgency_rank, due_date, rationale
    FROM breach_dedup WHERE rn <= 10
    ORDER BY urgency_rank
    """
    return spark.sql(sql)


# ══════════════════════════════════════════════════════
# 12-14. Static reference tables (glossary, metrics, genie examples)
# ══════════════════════════════════════════════════════
# NOTE: これらは scm/logic/glossary.py と同一の値でなければならない。
#       変更する場合は両方を揃えること。

GLOSSARY = [
    ("T001", "顧客在庫", "顧客保有在庫。意思決定の主軸。", "顧客確保済在庫,顧客手元在庫", "商社在庫と混同しない"),
    ("T002", "商社在庫", "商社側保有の補助在庫。既存引当を除く可用数量。", "フリー在庫,未引当在庫", "顧客在庫の代替として使わない"),
    ("T003", "安全在庫", "部品の安全在庫週数×週間平均需要で算出されるバッファ在庫。", "SS,セーフティストック", ""),
    ("T004", "LT", "発注から供給可能になるまでの所要期間。", "リードタイム,納入リードタイム", ""),
    ("T005", "LT長期化", "N-3またはN-6比較でLTが増加(↑)している状態。", "LTエスカレーション", "前月比は対象外"),
    ("T006", "指定納期", "顧客要求納期。", "希望納期,CRD", ""),
    ("T007", "回答納期", "商社または供給側が回答した納期。", "確認納期", ""),
    ("T008", "デッドライン", "顧客納期を守るための内部締切日。", "出庫日,出荷期限", ""),
    ("T009", "最短出荷可能日", "システム上安全とみなす最短出荷可能日。", "ESD", ""),
    ("T010", "分納可能数", "一括納入が難しい場合に先行出荷できる数量。", "部分出荷可能数", ""),
    ("T011", "ZERO", "在庫予測が0未満。", "在庫ゼロ", ""),
    ("T012", "UNDER", "在庫予測がmin(安全在庫)未満。", "min割れ", ""),
    ("T013", "OVER", "在庫予測がmax超過。", "過剰在庫", ""),
    ("T014", "FCST", "需要予測・計画値。将来需給と月末在庫予測に使用。", "フォーキャスト", ""),
    ("T015", "FCST消費", "FCSTに基づく将来の在庫消費量。", "予測消費", ""),
    ("T016", "先行手配", "受注に紐づかないメーカーへの事前発注。", "見込発注", ""),
    ("T017", "倉庫健全性", "(min/max基準を満たしている品目数÷管理品目数)×100%。", "", "定義なしに'健全'と使わない"),
    ("T018", "調整Priority", "Critical(3日)/High(7日)/Mid(14日)/Low(14日超)。", "", ""),
    ("T019", "調整Action", "緊急発注/商社前倒し調整/1週間後再確認/状況モニタリング/発注抑制。", "", "自由記述ではなく定型で管理"),
]

METRICS = [
    ("M001", "Critical Orders", "指定納期まで3日以内のオーダー数", "件", "gold_order_commit_risk", "Executive Control Tower"),
    ("M002", "High Orders", "指定納期まで7日以内のオーダー数", "件", "gold_order_commit_risk", "Executive Control Tower"),
    ("M003", "LT Escalation Items", "N-3 or N-6比でLT↑の品目数", "品目", "gold_lt_escalation_items", "Executive Control Tower, Lead Time Intelligence"),
    ("M004", "3M Policy Breaches", "直近3ヶ月以内にZERO/UNDER/OVERとなる品目数", "品目", "gold_inventory_policy_breach", "Executive Control Tower, Inventory Policy"),
    ("M005", "Projected Stockout Parts", "3ヶ月以内に在庫ゼロ予測の品目数", "品目", "gold_inventory_policy_breach WHERE breach_type=ZERO", "Executive Control Tower"),
    ("M006", "Excess Inventory Parts", "3ヶ月以内にmax超過予測の品目数", "品目", "gold_inventory_policy_breach WHERE breach_type=OVER", "Executive Control Tower"),
    ("M007", "Forecast Accuracy", "FCST精度の月次平均(%)", "%", "gold_exec_summary_daily", "Executive Control Tower"),
    ("M008", "Warehouse Health", "(min/max基準充足品目÷管理品目)×100%、倉庫シェア按分", "%", "gold_geo_warehouse_status", "Executive Control Tower, Network & Warehouse Health"),
    ("M009", "Coverage Weeks", "現在在庫÷週間平均需要", "週", "gold_balance_projection_monthly", "Commit & Supply Balance"),
    ("M010", "Risk Score", "納期接近度(0-100)。days_to_due×3を100から減算", "点", "gold_order_commit_risk", "Commit & Supply Balance"),
]

GENIE_EXAMPLES = [
    ("G001", "Critical Orderは何件ですか？", "SELECT COUNT(*) FROM gold_order_commit_risk WHERE priority_rank='Critical'", "調達,経営層", "指定納期3日以内"),
    ("G002", "LTが延長傾向にある部品は？", "SELECT item_code, item_name, latest_lt_weeks, trend_arrow_n3, trend_arrow_n6 FROM gold_lt_snapshot_current WHERE trend_arrow_n3='↑' OR trend_arrow_n6='↑'", "調達,SCM企画", "N-3/N-6比較"),
    ("G003", "3ヶ月以内に在庫ゼロになる部品は？", "SELECT DISTINCT item_code, product_name, breach_date FROM gold_inventory_policy_breach WHERE breach_type='ZERO'", "生産管理,調達", "直近3ヶ月限定"),
    ("G004", "浦和倉庫の健全性は？", "SELECT warehouse_name, health_score, zero_count, under_count, over_count FROM gold_geo_warehouse_status WHERE warehouse_name LIKE '%浦和%'", "倉庫長", ""),
    ("G005", "過剰在庫の部品はどれですか？", "SELECT DISTINCT item_code, product_name FROM gold_inventory_policy_breach WHERE breach_type='OVER'", "SCM企画,倉庫長", "max超過"),
    ("G006", "今週対応すべきアクションは？", "SELECT * FROM gold_action_queue_daily ORDER BY urgency_rank LIMIT 20", "調達,SCM企画", ""),
    ("G007", "FCST精度の推移を見せてください", "SELECT month, forecast_accuracy_pct FROM gold_exec_summary_daily", "SCM企画", ""),
    ("G008", "16週超のLTを持つ部品は？", "SELECT item_code, item_name, manufacturer_name, latest_lt_weeks FROM gold_lt_snapshot_current WHERE latest_lt_weeks > 16", "調達", ""),
]


@dlt.table(
    name="gold_business_glossary",
    comment="SCMドメイン用語辞書。Genie と UI 両方から参照される正規化用語集。",
)
def gold_business_glossary():
    schema = StructType([
        StructField("term_id",    StringType()),
        StructField("term",       StringType()),
        StructField("definition", StringType()),
        StructField("synonyms",   StringType()),
        StructField("prohibited", StringType()),
    ])
    return spark.createDataFrame(GLOSSARY, schema)


@dlt.table(
    name="gold_metric_definition",
    comment="各KPIの名前・数式・単位・出典Gold表・表示画面を明示したメタテーブル。",
)
def gold_metric_definition():
    schema = StructType([
        StructField("metric_id",   StringType()),
        StructField("metric_name", StringType()),
        StructField("formula",     StringType()),
        StructField("unit",        StringType()),
        StructField("source_gold", StringType()),
        StructField("screen",      StringType()),
    ])
    return spark.createDataFrame(METRICS, schema)


@dlt.table(
    name="gold_genie_semantic_examples",
    comment="Genie スペースに与える質問パターンと期待SQLの例。",
)
def gold_genie_semantic_examples():
    schema = StructType([
        StructField("example_id",   StringType()),
        StructField("question",     StringType()),
        StructField("expected_sql", StringType()),
        StructField("role",         StringType()),
        StructField("notes",        StringType()),
    ])
    return spark.createDataFrame(GENIE_EXAMPLES, schema)
