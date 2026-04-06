"""
Gold テーブル 10本 (docs/05_data_model.md 準拠)
顧客在庫を主役とした判断支援テーブル群

1. gold_exec_summary_daily         - 経営ダッシュボード用日次サマリー
2. gold_lt_snapshot_current        - 部品ごとの最新LTと比較値
3. gold_lt_trend_monthly           - 部品別/メーカー別LT月次推移
4. gold_lt_escalation_items        - LT長期化傾向アイテム一覧
5. gold_order_commit_risk          - オーダー単位の納期危険度
6. gold_requirement_timeline       - 所要量一覧の時系列イベント
7. gold_balance_projection_monthly - 月末在庫予測 (顧客在庫主役)
8. gold_inventory_policy_breach    - ZERO/UNDER/OVER判定
9. gold_geo_warehouse_status       - 地図表示用ステータス
10. gold_data_pipeline_health      - データ管理用パイプライン状況
"""
import pandas as pd
import numpy as np
from pathlib import Path
from logic.safety_stock import (calc_safety_stock, classify_inventory_status,
                                 classify_breach, classify_lt_band,
                                 project_monthly_stock, calc_coverage_weeks)
from logic.scoring import calc_criticality_score, score_to_priority

_DATA = Path(__file__).parent.parent / "sample_data"
TODAY = "2026-03-31"

def _load(name): return pd.read_csv(_DATA / name)


# ══════════════════════════════════════════
# 2. gold_lt_snapshot_current
# ══════════════════════════════════════════
def build_lt_snapshot_current() -> pd.DataFrame:
    lt = _load("lead_times.csv")
    comp = _load("components.csv")
    sups = _load("suppliers.csv")

    lt["effective_date"] = pd.to_datetime(lt["effective_date"], format="mixed")
    today = pd.Timestamp(TODAY)

    merged = lt.merge(comp[["component_id","part_number","component_name","component_category"]],
                      on="component_id", how="left")
    merged = merged.merge(sups[["supplier_id","supplier_name"]], on="supplier_id", how="left")

    sorted_dates = sorted(merged["effective_date"].unique())

    def get_lt_at(target):
        cands = [d for d in sorted_dates if d <= target]
        if not cands: return {}
        return merged[merged["effective_date"]==cands[-1]].set_index("component_id")["lead_time_weeks"].to_dict()

    current = get_lt_at(today)
    lt_n1 = get_lt_at(today - pd.DateOffset(months=1))
    lt_n3 = get_lt_at(today - pd.DateOffset(months=3))
    lt_n6 = get_lt_at(today - pd.DateOffset(months=6))

    latest = merged.sort_values("effective_date").groupby("component_id").last().reset_index()

    def arrow(cur, prev):
        if prev is None: return ""
        if cur > prev: return "↑"
        elif cur < prev: return "↓"
        return "→"

    rows = []
    for _, r in latest.iterrows():
        cid = r["component_id"]
        cur = current.get(cid, r["lead_time_weeks"])
        n1 = lt_n1.get(cid)
        n3 = lt_n3.get(cid)
        n6 = lt_n6.get(cid)

        d1 = (cur - n1) if n1 else None
        d3 = (cur - n3) if n3 else None
        d6 = (cur - n6) if n6 else None

        remark = ""
        if arrow(cur, n1) == "↑":
            remark = "LT延長中。早めの発注を推奨"
        elif arrow(cur, n3) == "↑":
            remark = "3ヶ月前から延長傾向"

        rows.append({
            "snapshot_date": TODAY, "item_id": cid,
            "item_code": r["part_number"], "item_name": r["component_name"],
            "manufacturer_name": r.get("supplier_name",""),
            "supplier_name": r.get("supplier_name",""),
            "latest_lt_weeks": int(cur),
            "lt_n1_weeks": int(n1) if n1 else None,
            "lt_n3_weeks": int(n3) if n3 else None,
            "lt_n6_weeks": int(n6) if n6 else None,
            "delta_vs_n1": int(d1) if d1 is not None else None,
            "delta_vs_n3": int(d3) if d3 is not None else None,
            "delta_vs_n6": int(d6) if d6 is not None else None,
            "trend_arrow_n1": arrow(cur, n1),
            "trend_arrow_n3": arrow(cur, n3),
            "trend_arrow_n6": arrow(cur, n6),
            "lt_band": classify_lt_band(cur),
            "remark": remark,
            "component_category": r.get("component_category",""),
            "supplier_id": r.get("supplier_id",""),
        })
    return pd.DataFrame(rows)


# ══════════════════════════════════════════
# 3. gold_lt_trend_monthly
# ══════════════════════════════════════════
def build_lt_trend_monthly() -> pd.DataFrame:
    lt = _load("lead_times.csv")
    comp = _load("components.csv")
    sups = _load("suppliers.csv")

    lt["effective_date"] = pd.to_datetime(lt["effective_date"], format="mixed")
    merged = lt.merge(comp[["component_id","part_number","component_name","component_category"]],
                      on="component_id", how="left")
    merged = merged.merge(sups[["supplier_id","supplier_name"]], on="supplier_id", how="left")

    merged["month"] = merged["effective_date"].dt.strftime("%Y-%m")
    merged["lt_band"] = merged["lead_time_weeks"].apply(classify_lt_band)

    return merged[["month","component_id","part_number","component_name",
                    "component_category","supplier_name","lead_time_weeks","lt_band",
                    "effective_date"]].copy()


# ══════════════════════════════════════════
# 4. gold_lt_escalation_items
# ══════════════════════════════════════════
def build_lt_escalation_items(lt_snapshot: pd.DataFrame) -> pd.DataFrame:
    """LT長期化 = 3ヶ月前比較 or 6ヶ月前比較で↑のアイテム"""
    esc = lt_snapshot[
        (lt_snapshot["trend_arrow_n3"]=="↑") |
        (lt_snapshot["trend_arrow_n6"]=="↑")
    ].copy()
    esc["escalation_flag"] = True
    esc["escalation_reason"] = esc.apply(lambda r:
        "3ヶ月前比で延長" if r["trend_arrow_n3"]=="↑"
        else "6ヶ月前比で延長", axis=1)
    return esc


# ══════════════════════════════════════════
# 5. gold_order_commit_risk
# ══════════════════════════════════════════
def build_order_commit_risk() -> pd.DataFrame:
    so = _load("sales_orders.csv")
    comp = _load("components.csv")
    sups = _load("suppliers.csv")
    inv = _load("inventory_current.csv")

    so["requested_delivery_date"] = pd.to_datetime(so["requested_delivery_date"], format="mixed")
    so["order_date"] = pd.to_datetime(so["order_date"], format="mixed")
    for col in ["response_date","earliest_ship_date","deadline_date"]:
        if col in so.columns:
            so[col] = pd.to_datetime(so[col], format="mixed", errors="coerce")

    today = pd.Timestamp(TODAY)

    # 顧客在庫 (ここでは在庫データを顧客在庫として扱う — docs準拠)
    cust_stock = inv.groupby("component_id", as_index=False)["stock_qty"].sum()
    cust_stock.rename(columns={"stock_qty":"current_customer_stock"}, inplace=True)

    # 商社在庫 = 0 (デモ用。実際は商社側データから取得)
    # 部品情報 + メーカー名
    comp_sup = comp.merge(sups[["supplier_id","supplier_name"]], on="supplier_id", how="left")
    so = so.merge(comp_sup[["component_id","part_number","component_name","component_category","supplier_name"]],
                  on="component_id", how="left")

    so = so.merge(cust_stock, on="component_id", how="left")
    so["current_customer_stock"] = so["current_customer_stock"].fillna(0).astype(int)
    so["current_trading_house_stock"] = 0  # 商社在庫は補助情報

    # Priority
    so["days_to_due"] = (so["requested_delivery_date"] - today).dt.days
    def prio(days):
        if pd.isna(days): return "Low"
        if days <= 3: return "Critical"
        if days <= 7: return "High"
        if days <= 14: return "Mid"
        return "Low"
    so["priority_rank"] = so["days_to_due"].apply(prio)

    # Risk score
    so["risk_score"] = so["days_to_due"].apply(lambda d: max(0, min(100, 100 - d*3)) if not pd.isna(d) else 0)

    # Action
    def action(row):
        if row["priority_rank"] == "Critical":
            return "商社前倒し調整" if row["current_customer_stock"] > 0 else "緊急発注"
        elif row["priority_rank"] == "High":
            return "商社前倒し調整"
        elif row["priority_rank"] == "Mid":
            return "1週間後に再確認"
        return "状況モニタリング"
    so["adjustment_action"] = so.apply(action, axis=1)

    # Risk reason
    def risk_reason(row):
        reasons = []
        if row["days_to_due"] is not None and row["days_to_due"] <= 7:
            reasons.append("納期接近")
        if row["current_customer_stock"] <= 0:
            reasons.append("顧客在庫ゼロ")
        return "; ".join(reasons) if reasons else "特になし"
    so["risk_reason"] = so.apply(risk_reason, axis=1)

    so["snapshot_date"] = TODAY

    cols = ["snapshot_date","sales_order_id","customer_name","product_name",
            "part_number","component_name","component_category","component_id",
            "remaining_qty","requested_delivery_date","response_date",
            "earliest_ship_date","deadline_date","partial_available_qty",
            "current_customer_stock","current_trading_house_stock",
            "priority_rank","adjustment_action","risk_reason","risk_score",
            "supplier_name","status"]
    available = [c for c in cols if c in so.columns]
    return so[available].sort_values("risk_score", ascending=False).reset_index(drop=True)


# ══════════════════════════════════════════
# 6. gold_requirement_timeline
# ══════════════════════════════════════════
def build_requirement_timeline() -> pd.DataFrame:
    """所要量一覧のイベント時系列 (全部品)"""
    inv = _load("inventory_current.csv")
    so = _load("sales_orders.csv")
    po = _load("purchase_orders.csv")
    fc = _load("forecasts.csv")
    bom = _load("bom.csv")

    so["requested_delivery_date"] = pd.to_datetime(so["requested_delivery_date"], format="mixed")
    po["expected_delivery_date"] = pd.to_datetime(po["expected_delivery_date"], format="mixed")
    fc["forecast_month"] = pd.to_datetime(fc["forecast_month"], format="mixed")
    today = pd.Timestamp(TODAY)

    stock_by_comp = inv.groupby("component_id", as_index=False)["stock_qty"].sum()

    rows = []
    for _, s in stock_by_comp.iterrows():
        cid = s["component_id"]

        # 在庫行
        rows.append({
            "snapshot_date":TODAY, "item_id":cid,
            "event_date":TODAY, "event_type":"顧客在庫",
            "order_no":"","sd_no":"",
            "quantity":int(s["stock_qty"]),
            "production_use_flag":False, "inbound_flag":False,
        })

        # 受注残 → 出荷 (マイナス)
        comp_so = so[so["component_id"]==cid]
        for _, o in comp_so.iterrows():
            rows.append({
                "snapshot_date":TODAY, "item_id":cid,
                "event_date":o["requested_delivery_date"].strftime("%Y-%m-%d"),
                "event_type":"生産使用日",
                "order_no":o.get("sales_order_id",""), "sd_no":"",
                "quantity":-int(o["component_required_qty"]),
                "production_use_flag":True, "inbound_flag":False,
            })

        # 発注残 → 入荷 (プラス)
        comp_po = po[po["component_id"]==cid]
        for _, p in comp_po.iterrows():
            if p["status"] in ["placed","acknowledged","in_production","shipped","partial_received"]:
                rows.append({
                    "snapshot_date":TODAY, "item_id":cid,
                    "event_date":p["expected_delivery_date"].strftime("%Y-%m-%d"),
                    "event_type":"商社納入日",
                    "order_no":p.get("purchase_order_id",""), "sd_no":"",
                    "quantity":int(p["outstanding_qty"]),
                    "production_use_flag":False, "inbound_flag":True,
                })

    df = pd.DataFrame(rows)
    df["event_date"] = pd.to_datetime(df["event_date"], format="mixed")

    # 累積計算
    result = []
    for cid in df["item_id"].unique():
        sub = df[df["item_id"]==cid].sort_values("event_date")
        cum = 0
        for _, r in sub.iterrows():
            if r["event_type"] == "顧客在庫":
                cum = r["quantity"]
            else:
                cum += r["quantity"]
            row = r.to_dict()
            row["cumulative_balance"] = cum
            row["allocated_qty"] = abs(r["quantity"]) if r["quantity"] < 0 and cum >= 0 else 0
            result.append(row)

    return pd.DataFrame(result)


# ══════════════════════════════════════════
# 7. gold_balance_projection_monthly
# ══════════════════════════════════════════
def build_balance_projection_monthly() -> pd.DataFrame:
    comp = _load("components.csv")
    inv = _load("inventory_current.csv")
    so = _load("sales_orders.csv")
    po = _load("purchase_orders.csv")
    fc = _load("forecasts.csv")
    bom = _load("bom.csv")

    fc["forecast_month"] = pd.to_datetime(fc["forecast_month"], format="mixed")
    so["requested_delivery_date"] = pd.to_datetime(so["requested_delivery_date"], format="mixed")
    po["expected_delivery_date"] = pd.to_datetime(po["expected_delivery_date"], format="mixed")

    today = pd.Timestamp(TODAY)
    # 過去3ヶ月 + 2026-11まで
    months = pd.date_range(today.replace(day=1) - pd.DateOffset(months=3), "2026-11-01", freq="MS")

    stock_total = inv.groupby("component_id", as_index=False)["stock_qty"].sum()

    rows = []
    for _, c in comp.iterrows():
        cid = c["component_id"]
        cur_stock = stock_total[stock_total["component_id"]==cid]["stock_qty"].sum()
        min_s = c.get("min_stock", 100)
        max_s = c.get("max_stock", 1000)

        running_stock = cur_stock
        for m in months:
            ms, me = m, m + pd.DateOffset(months=1)

            # 受注
            so_qty = int(so[(so["component_id"]==cid) &
                           (so["requested_delivery_date"]>=ms) &
                           (so["requested_delivery_date"]<me)]["component_required_qty"].sum())
            # 発注残入荷
            po_linked = int(po[(po["component_id"]==cid) &
                               (po["expected_delivery_date"]>=ms) &
                               (po["expected_delivery_date"]<me) &
                               (po["status"].isin(["placed","acknowledged","in_production","shipped"]))]["outstanding_qty"].sum())

            # FCST
            comp_bom = bom[bom["component_id"]==cid]
            fcst = 0
            if len(comp_bom) > 0:
                fc_m = fc[fc["forecast_month"]==m]
                if len(fc_m) > 0:
                    fcst = int((fc_m.merge(comp_bom, on="product_id")
                               .assign(d=lambda x: x["forecast_qty"]*x["quantity_per_unit"])["d"].sum()))

            # 過去月: 在庫変動なし (スナップショット = 現在在庫)
            # 当月以降: FCST消費 + PO入荷
            is_future = m >= today.replace(day=1)

            if is_future:
                # 消費 = max(受注, FCST)
                consumption = max(so_qty, fcst)
                # 入荷 = 確定PO のみ
                inbound = po_linked
                running_stock = running_stock + inbound - consumption
            else:
                # 過去月は変動なし (現在在庫がそのまま)
                consumption = 0
                inbound = 0

            # Policy status
            if running_stock <= 0: policy = "ZERO"
            elif running_stock < min_s: policy = "UNDER"
            elif running_stock > max_s: policy = "OVER"
            else: policy = "OK"

            rows.append({
                "month_end_date": m.strftime("%Y-%m"),
                "item_id": cid, "item_code": c["part_number"],
                "product_name": c["component_name"],
                "customer_stock_proj": int(running_stock),
                "confirmed_order_qty": so_qty,
                "forecast_qty": fcst,
                "inbound_qty_order_linked": po_linked,
                "production_use_qty": consumption,
                "min_qty": min_s, "max_qty": max_s,
                "policy_status": policy,
            })

    return pd.DataFrame(rows)


# ══════════════════════════════════════════
# 8. gold_inventory_policy_breach
# ══════════════════════════════════════════
def build_inventory_policy_breach(balance: pd.DataFrame) -> pd.DataFrame:
    """直近3ヶ月以内のブリーチのみ対象"""
    today = pd.Timestamp(TODAY)
    cutoff = (today + pd.DateOffset(months=6)).strftime("%Y-%m")  # UI側で3/6ヶ月切替

    rows = []
    for cid in balance["item_id"].unique():
        sub = balance[balance["item_id"]==cid].sort_values("month_end_date")
        for _, r in sub.iterrows():
            # 直近3ヶ月以内のみ
            if r["month_end_date"] > cutoff:
                continue
            if r["policy_status"] != "OK":
                rows.append({
                    "snapshot_date": TODAY,
                    "item_id": cid, "item_code": r["item_code"],
                    "product_name": r["product_name"],
                    "breach_type": r["policy_status"],
                    "breach_date": r["month_end_date"],
                    "projected_stock": r["customer_stock_proj"],
                    "min_qty": r["min_qty"], "max_qty": r["max_qty"],
                })
    df = pd.DataFrame(rows)
    if len(df) > 0:
        # 各部品の最初のbreach月でソート
        first = df.groupby("item_id")["breach_date"].min().reset_index()
        first.rename(columns={"breach_date":"first_breach"}, inplace=True)
        df = df.merge(first, on="item_id")
        df["priority_order"] = df["breach_type"].map({"ZERO":1,"UNDER":2,"OVER":3})
        df = df.sort_values(["priority_order","first_breach"])
    return df


# ══════════════════════════════════════════
# 9. gold_geo_warehouse_status
# ══════════════════════════════════════════
def build_geo_warehouse_status(balance: pd.DataFrame) -> pd.DataFrame:
    wh = _load("warehouses.csv")
    inv = _load("inventory_current.csv")
    comp = _load("components.csv")
    routes = _load("shipment_routes.csv")

    # 倉庫別集計: 安全在庫ベースで健全性を判定
    # 安全在庫 = safety_stock_weeks × (min_stock / safety_stock_weeks) ≈ min_stock
    # 過剰在庫 = max_stock超
    # 全体値をそのまま使う (部品ごとの合計在庫 vs min/max)
    comp_stock = inv.groupby("component_id", as_index=False)["stock_qty"].sum()
    comp_stock = comp_stock.merge(comp[["component_id","min_stock","max_stock"]], on="component_id", how="left")
    comp_stock["is_zero"] = comp_stock["stock_qty"] <= 0
    comp_stock["is_under"] = (comp_stock["stock_qty"] > 0) & (comp_stock["stock_qty"] < comp_stock["min_stock"])
    comp_stock["is_over"] = comp_stock["stock_qty"] > comp_stock["max_stock"]
    comp_stock["is_ok"] = (~comp_stock["is_zero"]) & (~comp_stock["is_under"]) & (~comp_stock["is_over"])

    # 倉庫に紐付け
    wc = _load("warehouse_components.csv")
    inv_wc = inv.merge(wc[["component_id","warehouse_id"]], on=["component_id","warehouse_id"], how="inner")
    inv_wc = inv_wc.merge(comp_stock[["component_id","is_zero","is_under","is_over","is_ok"]], on="component_id", how="left")

    wh_agg = inv_wc.groupby("warehouse_id", as_index=False).agg(
        managed_count=("component_id","nunique"),
        zero_count=("is_zero","sum"),
        under_count=("is_under","sum"),
        over_count=("is_over","sum"),
    )
    healthy = inv_wc.groupby("warehouse_id").apply(
        lambda g: g["is_ok"].sum() / max(len(g),1) * 100,
        include_groups=False
    ).reset_index(name="health_score").round(1)

    df = wh.merge(wh_agg, on="warehouse_id", how="left").merge(healthy, on="warehouse_id", how="left")
    df.rename(columns={"latitude":"geo_lat","longitude":"geo_lon"}, inplace=True)

    for c in ["managed_count","zero_count","under_count","over_count"]:
        df[c] = df[c].fillna(0).astype(int)
    df["health_score"] = df["health_score"].fillna(50)
    df["snapshot_date"] = TODAY

    # Critical/High (balance breach ベース)
    if len(balance) > 0:
        # 直近月のbreach件数
        latest_month = balance["month_end_date"].max()
        breach_latest = balance[balance["month_end_date"]==latest_month]
        # 簡易: zero+under = critical相当
        # TODO: 倉庫紐付けを厳密にする
        df["critical_count"] = df["zero_count"] + df["under_count"]
        df["high_count"] = 0
    else:
        df["critical_count"] = 0
        df["high_count"] = 0

    return df


# ══════════════════════════════════════════
# 10. gold_data_pipeline_health
# ══════════════════════════════════════════
def build_data_pipeline_health() -> pd.DataFrame:
    import os
    csv_map = {
        "suppliers":"bronze_suppliers","customers":"bronze_customers",
        "products":"bronze_products","components":"bronze_components",
        "warehouses":"bronze_warehouses","bom":"bronze_bom",
        "forecasts":"bronze_forecasts","lead_times":"bronze_lead_times",
        "inventory_current":"bronze_inventory_current",
        "logistics":"bronze_logistics","sales_orders":"bronze_sales_orders",
        "purchase_orders":"bronze_purchase_orders",
    }
    rows = []
    for csv_name, table_name in csv_map.items():
        path = _DATA / f"{csv_name}.csv"
        try:
            df = pd.read_csv(path)
            cnt = len(df)
            null_rate = df.isnull().sum().sum() / max(cnt * len(df.columns), 1)
            quality = round((1 - null_rate) * 100, 1)
        except Exception:
            cnt, quality = 0, 0
        rows.append({
            "snapshot_date":TODAY,"pipeline_name":f"csv_ingest_{csv_name}",
            "source_table":f"csv/{csv_name}.csv","target_table":table_name,
            "record_count":cnt,"freshness_ts":TODAY,
            "quality_score":quality,"success_flag":True,"error_message":None,
        })
    return pd.DataFrame(rows)


# ══════════════════════════════════════════
# 1. gold_exec_summary_daily
# ══════════════════════════════════════════
def build_exec_summary(lt_snap, breach, geo, balance) -> pd.DataFrame:
    fc = _load("forecasts.csv")
    fc["forecast_accuracy"] = pd.to_numeric(fc["forecast_accuracy"], errors="coerce")
    recent = fc[pd.to_datetime(fc["forecast_month"], format="mixed") >= pd.Timestamp(TODAY) - pd.DateOffset(months=3)]
    avg_acc = recent["forecast_accuracy"].mean() * 100 if len(recent) > 0 else 0

    lt_esc = len(lt_snap[(lt_snap["trend_arrow_n3"]=="↑") | (lt_snap["trend_arrow_n6"]=="↑")])

    orders = build_order_commit_risk()
    crit_orders = int((orders["priority_rank"]=="Critical").sum())
    high_orders = int((orders["priority_rank"]=="High").sum())

    zero_count = int(breach[breach["breach_type"]=="ZERO"]["item_id"].nunique()) if len(breach)>0 else 0
    under_count = int(breach[breach["breach_type"]=="UNDER"]["item_id"].nunique()) if len(breach)>0 else 0
    over_count = int(breach[breach["breach_type"]=="OVER"]["item_id"].nunique()) if len(breach)>0 else 0

    return pd.DataFrame([{
        "snapshot_date":TODAY,
        "critical_count": crit_orders,
        "high_count": high_orders,
        "medium_count": int((orders["priority_rank"]=="Mid").sum()),
        "low_count": int((orders["priority_rank"]=="Low").sum()),
        "lt_escalation_item_count": lt_esc,
        "zero_count": zero_count,
        "under_count": under_count,
        "over_count": over_count,
        "warehouse_health_score": round(geo["health_score"].mean(), 1),
        "top_risk_order_count": crit_orders + high_orders,
        "forecast_accuracy_pct": round(avg_acc, 1),
    }])


# ══════════════════════════════════════════
# build_all_gold
# ══════════════════════════════════════════
def build_action_queue_daily(orders: pd.DataFrame, lt_esc: pd.DataFrame,
                              breach: pd.DataFrame) -> pd.DataFrame:
    """gold_action_queue_daily — 今週の優先アクション統合キュー"""
    rows = []
    # Critical/High orders
    for _, r in orders[orders["priority_rank"].isin(["Critical","High"])].head(20).iterrows():
        rows.append({
            "snapshot_date": TODAY,
            "source": "order_risk",
            "target_id": r.get("sales_order_id",""),
            "item_code": r.get("part_number",""),
            "item_name": r.get("component_name",""),
            "risk_type": f"納期{r.get('priority_rank','')}",
            "recommended_action": r.get("adjustment_action",""),
            "urgency_rank": 1 if r.get("priority_rank")=="Critical" else 2,
            "due_date": str(r.get("requested_delivery_date","")),
            "rationale": r.get("risk_reason",""),
        })
    # LT escalation
    for _, r in lt_esc.head(10).iterrows():
        rows.append({
            "snapshot_date": TODAY,
            "source": "lt_escalation",
            "target_id": r.get("item_id",""),
            "item_code": r.get("item_code",""),
            "item_name": r.get("item_name",""),
            "risk_type": "LT長期化",
            "recommended_action": "発注タイミング前倒し検討",
            "urgency_rank": 3,
            "due_date": "",
            "rationale": r.get("remark","") or r.get("escalation_reason",""),
        })
    # Breach ZERO
    if len(breach) > 0:
        zero = breach[breach["breach_type"]=="ZERO"].drop_duplicates("item_id").head(10)
        for _, r in zero.iterrows():
            rows.append({
                "snapshot_date": TODAY,
                "source": "policy_breach",
                "target_id": r.get("item_id",""),
                "item_code": r.get("item_code",""),
                "item_name": r.get("product_name",""),
                "risk_type": "在庫ZERO予測",
                "recommended_action": "緊急補充検討",
                "urgency_rank": 2,
                "due_date": r.get("breach_date",""),
                "rationale": f"予測在庫{r.get('projected_stock',0):,}",
            })
    df = pd.DataFrame(rows)
    if len(df) > 0:
        df = df.sort_values("urgency_rank").reset_index(drop=True)
    return df


def build_all_gold() -> dict[str, pd.DataFrame]:
    from logic.glossary import build_glossary_df, build_metrics_df, build_genie_examples_df

    lt_snap = build_lt_snapshot_current()
    lt_trend = build_lt_trend_monthly()
    lt_esc = build_lt_escalation_items(lt_snap)
    orders = build_order_commit_risk()
    req_timeline = build_requirement_timeline()
    balance = build_balance_projection_monthly()
    breach = build_inventory_policy_breach(balance)
    geo = build_geo_warehouse_status(balance)
    pipeline = build_data_pipeline_health()
    summary = build_exec_summary(lt_snap, breach, geo, balance)
    action_q = build_action_queue_daily(orders, lt_esc, breach)
    glossary = build_glossary_df()
    metrics = build_metrics_df()
    genie_ex = build_genie_examples_df()

    return {
        "gold_exec_summary_daily": summary,
        "gold_lt_snapshot_current": lt_snap,
        "gold_lt_trend_monthly": lt_trend,
        "gold_lt_escalation_items": lt_esc,
        "gold_order_commit_risk": orders,
        "gold_requirement_timeline": req_timeline,
        "gold_balance_projection_monthly": balance,
        "gold_inventory_policy_breach": breach,
        "gold_geo_warehouse_status": geo,
        "gold_data_pipeline_health": pipeline,
        "gold_action_queue_daily": action_q,
        "gold_business_glossary": glossary,
        "gold_metric_definition": metrics,
        "gold_genie_semantic_examples": genie_ex,
    }
