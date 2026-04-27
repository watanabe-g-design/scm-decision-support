"""
リスク計算ロジック
=================
判定ロジックを関数化し、画面コードから分離する。
閾値は thresholds.py から読む。
"""
import pandas as pd
import numpy as np
from services.thresholds import (
    FORECAST_RISK, ORDER_DELIVERY_RISK, INVENTORY_STATUS, TODAY
)


# ══════════════════════════════════════════════════════
# A. フォーキャスト・発注リスク
# ══════════════════════════════════════════════════════
def calc_forecast_risk_status(days_remaining) -> str:
    """発注必要日までの残り日数からステータスを判定"""
    if pd.isna(days_remaining):
        return "Normal"
    d = int(days_remaining)
    if d <= FORECAST_RISK["critical_days"]:
        return "Critical"
    if d <= FORECAST_RISK["high_days"]:
        return "High"
    if d <= FORECAST_RISK["medium_days"]:
        return "Medium"
    return "Normal"


def build_forecast_risk_df(
    forecasts: pd.DataFrame,
    bom: pd.DataFrame,
    components: pd.DataFrame,
    products: pd.DataFrame,
    customers: pd.DataFrame,
    purchase_orders: pd.DataFrame,
) -> pd.DataFrame:
    """フォーキャスト × BOM × 部品マスタ を結合してリスクを計算"""
    today = pd.Timestamp(TODAY)

    # FCST → BOM 展開で部品単位にする
    fc = forecasts.copy()
    fc["forecast_month"] = pd.to_datetime(fc["forecast_month"], format="mixed", errors="coerce")

    # FCST × BOM → 部品需要
    fc_bom = fc.merge(bom, on="product_id", how="inner")
    fc_bom["component_demand_qty"] = (
        pd.to_numeric(fc_bom["forecast_qty"], errors="coerce").fillna(0)
        * pd.to_numeric(fc_bom["quantity_per_unit"], errors="coerce").fillna(1)
    ).astype(int)

    # 部品マスタ結合 (LT, 品番, 品名)
    comp = components[["component_id", "part_number", "component_name",
                        "component_category", "supplier_name",
                        "base_lead_time_weeks"]].copy()
    comp["base_lead_time_weeks"] = pd.to_numeric(comp["base_lead_time_weeks"], errors="coerce").fillna(12)
    fc_bom = fc_bom.merge(comp, on="component_id", how="left")

    # 製品マスタ → 顧客マスタ結合
    prod = products[["product_id", "product_name", "customer_id"]].copy()
    fc_bom = fc_bom.merge(prod, on="product_id", how="left")
    cust = customers[["customer_id", "customer_name"]].copy()
    fc_bom = fc_bom.merge(cust, on="customer_id", how="left")

    # 発注必要日 = forecast_month - LT
    fc_bom["order_required_date"] = (
        fc_bom["forecast_month"]
        - pd.to_timedelta(fc_bom["base_lead_time_weeks"] * 7, unit="D")
    )
    fc_bom["days_remaining"] = (fc_bom["order_required_date"] - today).dt.days

    # 発注済み数量 (PO の outstanding_qty を component_id 別に合計)
    po = purchase_orders.copy()
    po["outstanding_qty"] = pd.to_numeric(po["outstanding_qty"], errors="coerce").fillna(0)
    po_agg = po.groupby("component_id", as_index=False)["outstanding_qty"].sum()
    po_agg.rename(columns={"outstanding_qty": "ordered_qty"}, inplace=True)
    fc_bom = fc_bom.merge(po_agg, on="component_id", how="left")
    fc_bom["ordered_qty"] = fc_bom["ordered_qty"].fillna(0).astype(int)
    fc_bom["unordered_qty"] = (fc_bom["component_demand_qty"] - fc_bom["ordered_qty"]).clip(lower=0)

    # ステータス
    fc_bom["status"] = fc_bom["days_remaining"].apply(calc_forecast_risk_status)

    # 出力列の整理
    result = fc_bom[[
        "customer_name", "product_name", "part_number", "component_name",
        "component_category", "supplier_name",
        "forecast_month", "component_demand_qty",
        "base_lead_time_weeks", "order_required_date", "days_remaining",
        "ordered_qty", "unordered_qty", "status",
    ]].copy()
    result = result.sort_values(["days_remaining", "component_demand_qty"],
                                ascending=[True, False]).reset_index(drop=True)
    return result


# ══════════════════════════════════════════════════════
# B. 受注・納品リスク
# ══════════════════════════════════════════════════════
def calc_order_delivery_status(row) -> str:
    """受注・納品リスクステータスを判定"""
    days_to_delivery = row.get("days_to_delivery")
    resp_deadline_diff = row.get("response_deadline_diff")
    response_late = row.get("response_late", False)

    if pd.isna(days_to_delivery):
        return "Normal"

    d = int(days_to_delivery)
    rd = int(resp_deadline_diff) if not pd.isna(resp_deadline_diff) else 999

    th = ORDER_DELIVERY_RISK
    # Critical
    if d <= th["critical_delivery_days"] and rd <= th["critical_response_deadline_diff"]:
        return "Critical"
    # 指定納期が過ぎている
    if d < 0:
        return "Critical"
    # High
    if d <= th["high_delivery_days"] and rd <= th["high_response_deadline_diff"]:
        return "High"
    # Medium
    if d <= th["medium_delivery_days"] or response_late:
        return "Medium"
    return "Normal"


def build_order_delivery_risk_df(orders: pd.DataFrame) -> pd.DataFrame:
    """受注データに納品リスクステータスを付与"""
    today = pd.Timestamp(TODAY)
    df = orders.copy()

    # 日付変換
    for col in ["requested_delivery_date", "response_date", "deadline_date",
                "earliest_ship_date", "order_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")

    # 指定納期までの日数
    if "requested_delivery_date" in df.columns:
        df["days_to_delivery"] = (df["requested_delivery_date"] - today).dt.days
    else:
        df["days_to_delivery"] = np.nan

    # 回答納期とデッドラインの差分 (日数)
    if "response_date" in df.columns and "deadline_date" in df.columns:
        df["response_deadline_diff"] = (df["deadline_date"] - df["response_date"]).dt.days
    else:
        df["response_deadline_diff"] = np.nan

    # 回答納期が指定納期より遅いか
    if "response_date" in df.columns and "requested_delivery_date" in df.columns:
        df["response_late"] = df["response_date"] > df["requested_delivery_date"]
    else:
        df["response_late"] = False

    # ステータス判定
    df["delivery_status"] = df.apply(calc_order_delivery_status, axis=1)

    # 数値型変換
    for c in ["order_qty", "remaining_qty", "shipped_qty", "partial_available_qty"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    return df.sort_values(["days_to_delivery", "response_deadline_diff"],
                          ascending=[True, True]).reset_index(drop=True)


# ══════════════════════════════════════════════════════
# C. 在庫ステータス
# ══════════════════════════════════════════════════════
def calc_inventory_status(available_qty, safety_qty) -> str:
    """利用可能在庫からステータスを判定"""
    if pd.isna(available_qty):
        return "Normal"
    if available_qty < 0:
        return "Shortage"
    if available_qty < safety_qty:
        return "Low"
    return "Available"


def build_inventory_df(
    inventory_current: pd.DataFrame,
    components: pd.DataFrame,
    sales_orders: pd.DataFrame,
) -> pd.DataFrame:
    """在庫 + 引当 + 利用可能在庫を計算"""
    inv = inventory_current.copy()
    inv["stock_qty"] = pd.to_numeric(inv["stock_qty"], errors="coerce").fillna(0).astype(int)

    # 部品別在庫合計 (全倉庫)
    inv_agg = inv.groupby("component_id", as_index=False)["stock_qty"].sum()
    inv_agg.rename(columns={"stock_qty": "month_end_inventory"}, inplace=True)

    # 引当数量 = 未出荷受注の component_required_qty
    so = sales_orders.copy()
    so["component_required_qty"] = pd.to_numeric(so.get("component_required_qty", 0), errors="coerce").fillna(0)
    so_active = so[so["status"].isin(["open", "confirmed", "shipped_partial", "in_production"])]
    allocated = so_active.groupby("component_id", as_index=False)["component_required_qty"].sum()
    allocated.rename(columns={"component_required_qty": "allocated_qty"}, inplace=True)

    # 部品マスタ結合
    comp = components[["component_id", "part_number", "component_name",
                        "component_category", "supplier_name", "min_stock"]].copy()
    comp["min_stock"] = pd.to_numeric(comp["min_stock"], errors="coerce").fillna(
        INVENTORY_STATUS["default_safety_qty"]
    )

    df = comp.merge(inv_agg, on="component_id", how="left")
    df["month_end_inventory"] = df["month_end_inventory"].fillna(0).astype(int)
    df = df.merge(allocated, on="component_id", how="left")
    df["allocated_qty"] = df["allocated_qty"].fillna(0).astype(int)

    # 利用可能在庫
    df["available_inventory"] = df["month_end_inventory"] - df["allocated_qty"]
    df["spot_order_capacity"] = df["available_inventory"].clip(lower=0)

    # ステータス
    df["inventory_status"] = df.apply(
        lambda r: calc_inventory_status(r["available_inventory"], r["min_stock"]),
        axis=1,
    )

    return df.sort_values("available_inventory").reset_index(drop=True)


# ══════════════════════════════════════════════════════
# D. 月次需給バランス
# ══════════════════════════════════════════════════════
def build_monthly_balance_df(balance_projection: pd.DataFrame) -> pd.DataFrame:
    """gold_balance_projection_monthly をベースに需給バランスを構築"""
    df = balance_projection.copy()
    for c in ["customer_stock_proj", "confirmed_order_qty", "forecast_qty",
              "inbound_qty_order_linked", "production_use_qty", "min_qty", "max_qty"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # 供給可能数量 = 在庫予測 + 入荷予定 (既に customer_stock_proj に含まれている)
    # 不足/余剰 = 在庫予測 - min_qty (安全在庫割れの有無)
    df["supply_available"] = df["customer_stock_proj"]
    df["shortage_surplus"] = df["customer_stock_proj"] - df["min_qty"]
    df["balance_status"] = df["shortage_surplus"].apply(
        lambda x: "不足" if x < 0 else ("余剰" if x > 0 else "均衡")
    )
    return df


# ══════════════════════════════════════════════════════
# E. 入出庫リスト
# ══════════════════════════════════════════════════════
def build_inbound_outbound_df(requirement_timeline: pd.DataFrame) -> pd.DataFrame:
    """gold_requirement_timeline を入出庫リスト形式に変換"""
    df = requirement_timeline.copy()
    df["event_date"] = pd.to_datetime(df["event_date"], format="mixed", errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

    # 入庫/出庫区分
    df["direction"] = df["event_type"].apply(
        lambda t: "入庫" if "納入" in str(t) or "在庫" in str(t) else "出庫"
    )

    return df.sort_values("event_date").reset_index(drop=True)


# ══════════════════════════════════════════════════════
# Overview KPI 集約
# ══════════════════════════════════════════════════════
def build_overview_kpis(
    forecast_risk: pd.DataFrame,
    order_delivery: pd.DataFrame,
    inventory: pd.DataFrame,
    monthly_balance: pd.DataFrame,
) -> dict:
    """Overview 画面用の集約 KPI を計算"""
    return {
        # Forecast Risk
        "forecast_critical": int((forecast_risk["status"] == "Critical").sum()),
        "forecast_high":     int((forecast_risk["status"] == "High").sum()),
        "forecast_medium":   int((forecast_risk["status"] == "Medium").sum()),
        # Order Delivery Risk
        "delivery_critical": int((order_delivery["delivery_status"] == "Critical").sum()),
        "delivery_high":     int((order_delivery["delivery_status"] == "High").sum()),
        "delivery_medium":   int((order_delivery["delivery_status"] == "Medium").sum()),
        "delivery_overdue":  int((order_delivery["days_to_delivery"] < 0).sum()),
        # Inventory
        "inventory_shortage": int((inventory["inventory_status"] == "Shortage").sum()),
        "inventory_low":      int((inventory["inventory_status"] == "Low").sum()),
        # Monthly Balance
        "months_with_shortage": int((monthly_balance["balance_status"] == "不足").sum() if len(monthly_balance) > 0 else 0),
    }
