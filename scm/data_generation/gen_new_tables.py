"""
SCM Demo - 新規テーブル生成スクリプト (Phase 8 改訂版)
=========================================================
Phase 8 改修ポイント:
  ✅ Critical (重) = 4月のみ6件に抑制 → デモで「5件程度」を実現
  ✅ High (中) = 4-5月の2ヶ月で5部材 × 2月 = 10件
  ✅ スマートPO生成: FCST連動で部材ごとにカバー率4グループ分類 → 65%健全性
  ✅ inventory_current もグループに合わせた初期在庫
  ✅ 希望納期: 月内ランダム (5〜25日) かつ TODAY 以降のみ
  ✅ マクニカフリー在庫: 新子安1拠点
  ✅ forecasts.csv: 2026-12まで延長済み

実行:
    python -m data_generation.gen_new_tables
"""
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

SEED = 4242
rng = random.Random(SEED)

OUT = Path(__file__).parent.parent / "sample_data"

TODAY = date(2026, 3, 28)
TARGET_CUSTOMER_ID = "CUS001"

PAST_WINDOW_DAYS   = 0
FUTURE_WINDOW_DAYS = 270   # 9ヶ月

MAX_DEMAND_QTY       = 6000
SHORTAGE_LARGE_MIN_QTY = 4000
NORMAL_DEMAND_MIN    = 30
NORMAL_DEMAND_MAX    = 400

DOOMED_CUST_CAP      = (10, 150)
PARTIAL_CUST_CAP     = (200, 800)

# Critical/High を生成する対象月
TARGET_MONTH   = date(2026, 4, 1)   # Critical (重) はこの月のみ
PARTIAL_MONTHS = [date(2026, 4, 1), date(2026, 5, 1)]  # High (中) は4-5月

MACNICA_WAREHOUSE_ID   = "WH_MAC_SHINKOYASU"
MACNICA_WAREHOUSE_NAME = "新子安ロジスティクスセンター"


# ════════════════════════════════════════════════════════════════
# 共通ユーティリティ
# ════════════════════════════════════════════════════════════════
def _random_date_in_month(month_start: date) -> date:
    day = rng.randint(5, 25)
    try:
        return month_start.replace(day=day)
    except ValueError:
        return month_start.replace(day=15)


# ════════════════════════════════════════════════════════════════
# BOM充足シナリオ選出
# ════════════════════════════════════════════════════════════════
def _pick_bom_scenarios(
    bom_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> tuple[str, list[str], set[str], set[str]]:
    """
    Critical/High シナリオのスケープゴート製品と部材を選出。

    Returns
    -------
    doomed_product_id : str   (全BOM部材をCritical化する製品)
    partial_product_ids : list[str]   (1部材だけHighにする製品 3件)
    doomed_comp_ids : set[str]   (Critical化する部材)
    partial_comp_ids : set[str]  (High化する部材 5件)
    """
    target_products = products_df[products_df["customer_id"] == TARGET_CUSTOMER_ID].copy()
    if target_products.empty:
        target_products = products_df.copy()

    bom_size = bom_df.groupby("product_id").size().reset_index(name="bom_count")
    target_products = target_products.merge(bom_size, on="product_id", how="left").fillna({"bom_count": 0})
    target_products["bom_count"] = target_products["bom_count"].astype(int)

    # Critical製品: BOM部材数5〜7が理想 (6件 ≈ ~5件)
    candidates = target_products[
        (target_products["bom_count"] >= 5) & (target_products["bom_count"] <= 7)
    ].copy()
    if candidates.empty:
        candidates = target_products[target_products["bom_count"] >= 4].copy()
    if candidates.empty:
        candidates = target_products.copy()

    candidates = candidates.sample(n=min(4, len(candidates)), random_state=SEED).reset_index(drop=True)
    if len(candidates) < 4:
        extra = target_products[~target_products["product_id"].isin(candidates["product_id"])].head(4 - len(candidates))
        candidates = pd.concat([candidates, extra], ignore_index=True)

    doomed_product_id = candidates.iloc[0]["product_id"]
    partial_product_ids = candidates.iloc[1:4]["product_id"].tolist()

    # doomed: 全BOM部材をCritical化
    doomed_bom = bom_df[bom_df["product_id"] == doomed_product_id]
    doomed_comp_ids = set(doomed_bom["component_id"].tolist())

    # partial: 各製品から1部材 (doomed_comp_idsと重複しないように)
    # 5部材 × 2ヶ月 = 10 High需要
    partial_comp_ids: set[str] = set()
    for pid in partial_product_ids:
        pbom = bom_df[
            (bom_df["product_id"] == pid) &
            (~bom_df["component_id"].isin(doomed_comp_ids)) &
            (~bom_df["component_id"].isin(partial_comp_ids))
        ]
        if pbom.empty:
            continue
        chosen = pbom.sample(n=1, random_state=SEED).iloc[0]["component_id"]
        partial_comp_ids.add(chosen)
    # 不足分を追加 (5部材に満たない場合)
    if len(partial_comp_ids) < 5:
        remaining = bom_df[
            (~bom_df["component_id"].isin(doomed_comp_ids)) &
            (~bom_df["component_id"].isin(partial_comp_ids))
        ]["component_id"].unique().tolist()
        for c in remaining:
            partial_comp_ids.add(c)
            if len(partial_comp_ids) >= 5:
                break

    return doomed_product_id, partial_product_ids, doomed_comp_ids, partial_comp_ids


# ════════════════════════════════════════════════════════════════
# 在庫グループ分類 (65%健全性のための4グループ)
# ════════════════════════════════════════════════════════════════
def _assign_inventory_groups(
    components_df: pd.DataFrame,
    doomed_cids: set[str],
    partial_cids: set[str],
) -> dict[str, str]:
    """
    90部材を4グループに分類。

    グループA (60%): OK → 月次PO ≈ 消費量の100-120%
    グループB (10%): OVER → 月次PO ≈ 消費量の160-200%
    グループC (15%): UNDER → 月次PO ≈ 消費量の50-65%
    グループD (15%): ZERO → 月次PO ≈ 消費量の10-20%
    doomed/partial → ZERO (Critical/Highシナリオ維持)

    Returns: {component_id: "A"/"B"/"C"/"D"}
    """
    all_comps = [c for c in components_df["component_id"].tolist()
                 if c not in doomed_cids and c not in partial_cids]
    # seedで固定シャッフル
    shuffled = list(all_comps)
    rng.shuffle(shuffled)
    n = len(shuffled)

    groups: dict[str, str] = {}
    for i, comp_id in enumerate(shuffled):
        ratio = i / n
        if ratio < 0.60:
            groups[comp_id] = "A"
        elif ratio < 0.70:
            groups[comp_id] = "B"
        elif ratio < 0.85:
            groups[comp_id] = "C"
        else:
            groups[comp_id] = "D"

    # doomed: Group D (PO最小 → total_cap が demand より小さい → Critical発動)
    for cid in doomed_cids:
        groups[cid] = "D"
    # partial: Group C (PO = 50-65% → max_single < demand ≤ total_cap → High発動)
    # ※ PO生成では partial_cids は除外せず、Group C の coverage 50-65% で生成する
    for cid in partial_cids:
        groups[cid] = "C"

    return groups


# ════════════════════════════════════════════════════════════════
# スマートPO生成 (FCST連動・65%健全性)
# ════════════════════════════════════════════════════════════════
def gen_purchase_orders_smart(
    components_df: pd.DataFrame,
    bom_df: pd.DataFrame,
    forecasts_df: pd.DataFrame,
    inventory_groups: dict[str, str],
    doomed_cids: set[str] | None = None,
    partial_cids: set[str] | None = None,
) -> pd.DataFrame:
    """
    FCST消費量をベースにグループ別のカバー率でPOを生成する。

    グループA: PO = 110-120% of monthly_consumption → OK維持
    グループB: PO = 160-200% → OVER傾向
    グループC: PO = 50-65%  → UNDER傾向
    グループD: PO = 10-20%  → ZERO傾向
    """
    # 全FCST × BOM展開で部材ごとの月次消費量を計算
    fc = forecasts_df.copy()
    fc["forecast_month"] = pd.to_datetime(fc["forecast_month"], format="mixed", errors="coerce").dt.date

    # 未来月のみを対象 (今日以降。過去の薄いデータで平均が下がるのを防ぐ)
    fc_future = fc[fc["forecast_month"] > TODAY].copy()
    if fc_future.empty:
        fc_future = fc.copy()

    comp_monthly = (
        fc_future.merge(bom_df[["product_id", "component_id", "quantity_per_unit"]], on="product_id")
        .assign(comp_qty=lambda x: x["forecast_qty"] * x["quantity_per_unit"])
        .groupby(["component_id", "forecast_month"])["comp_qty"].sum()
        .reset_index()
    )
    # ピーク値の80%を基準 (安定した高めのカバレッジ)
    peak_consumption = comp_monthly.groupby("component_id")["comp_qty"].max()
    avg_consumption = (peak_consumption * 0.8).to_dict()

    # 対象月: 2026-04 〜 2026-12
    months_future = []
    m = date(2026, 4, 1)
    while m <= date(2026, 12, 1):
        months_future.append(m)
        next_month = m.month + 1
        m = date(m.year + (next_month // 13), (next_month - 1) % 12 + 1, 1)

    # 部材×サプライヤーマッピング (components.csvからsupplier_idを取得)
    comp_supplier = components_df.set_index("component_id")[["supplier_id", "unit_price_usd", "base_lead_time_weeks"]].to_dict("index")

    # 目標: 健全性70% = Group A(53%) + B/C/Dの一部
    # A: 100-105%でinit=(min+max)/2→常にOK範囲に留まる
    # B: 115%でmax寄りinit→上昇傾向だが範囲内が多い
    # C: 75-80%でmin*1.2init→下降傾向 → 後半UNDER
    # D: 20-25%→ZERO
    coverage_params = {
        "A": (1.00, 1.05),   # ほぼ100%: initが(min+max)/2なら7ヶ月OK維持
        "B": (1.10, 1.20),   # 110-120%: max寄りinitから緩やかに上昇→後半OVER傾向
        "C": (0.85, 0.92),   # 85-92%: (min+max)/2 initから徐々に下降→後半のみUNDER/ZERO
        "D": (0.18, 0.28),   # ZERO傾向
    }

    rows = []
    po_id = 1

    # doomed / partial は別途専用PO生成 → ここでは除外
    scenario_no_smart_po = (doomed_cids or set()) | (partial_cids or set())

    for comp_id in components_df["component_id"].tolist():
        if comp_id in scenario_no_smart_po:
            continue

        grp = inventory_groups.get(comp_id, "D")
        lo, hi = coverage_params[grp]
        coverage = rng.uniform(lo, hi)
        avg_cons = avg_consumption.get(comp_id, 200)
        sup_info = comp_supplier.get(comp_id, {})
        supplier_id = sup_info.get("supplier_id", "SUP001")
        unit_price = float(sup_info.get("unit_price_usd", 10))
        lt_weeks = int(sup_info.get("base_lead_time_weeks", 20))

        for m in months_future:
            po_qty = max(0, int(avg_cons * coverage))
            if po_qty < 10:
                continue

            # 納期: 月初+1〜15日 (月初に届くようにする)
            delivery_day = rng.randint(1, 15)
            delivery_date = m.replace(day=delivery_day)
            # 発注日: 納期 - LT
            order_date = delivery_date - timedelta(weeks=lt_weeks)

            # 既に入荷済みの月 (TODAY より前)
            if delivery_date <= TODAY:
                status = "fully_received"
                received_qty = po_qty
                outstanding_qty = 0
            else:
                months_ahead = (m.year - TODAY.year) * 12 + m.month - TODAY.month
                if months_ahead <= 2:
                    status = rng.choice(["shipped", "in_production"])
                elif months_ahead <= 5:
                    status = rng.choice(["acknowledged", "in_production"])
                else:
                    status = "placed"
                received_qty = 0
                outstanding_qty = po_qty

            rows.append({
                "purchase_order_id":       f"PO{po_id:07d}",
                "component_id":            comp_id,
                "supplier_id":             supplier_id,
                "order_date":              order_date.isoformat(),
                "expected_delivery_date":  delivery_date.isoformat(),
                "quantity":                po_qty,
                "received_qty":            received_qty,
                "outstanding_qty":         outstanding_qty,
                "status":                  status,
                "is_delayed":              False,
                "delay_days":              0,
                "unit_cost_usd":           round(unit_price, 2),
                "total_cost_usd":          round(po_qty * unit_price, 2),
            })
            po_id += 1

    df = pd.DataFrame(rows)
    ok_cnt = (df["status"] == "fully_received").sum()
    outstanding_total = df["outstanding_qty"].sum()
    print(f"  → purchase_orders (smart): {len(df)} 行")
    print(f"     outstanding_qty合計: {outstanding_total:,}")
    print(f"     グループ別部材数: " + ", ".join(
        f"{g}={sum(1 for v in inventory_groups.values() if v == g)}"
        for g in ["A", "B", "C", "D"]
    ))
    return df


# ════════════════════════════════════════════════════════════════
# inventory_current 調整 (グループ別初期在庫)
# ════════════════════════════════════════════════════════════════
def adjust_inventory_for_scenarios(
    inv_df: pd.DataFrame,
    inventory_groups: dict[str, str],
    components_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    グループ別に初期在庫を調整して、月次予測の健全性を65%に近づける。

    A群: min_stock 〜 max_stock の範囲内 (OK開始)
    B群: max_stock 付近 (OVER傾向開始)
    C群: min_stock × 0.6-0.9 (UNDER傾向開始)
    D群: min_stock × 0.1-0.2 (ZERO傾向開始)
    """
    df = inv_df.copy()
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce").fillna(0).astype(int)

    comp_info = components_df.set_index("component_id")[["min_stock", "max_stock"]].to_dict("index")
    adjusted = 0

    for comp_id, grp in inventory_groups.items():
        info = comp_info.get(comp_id, {})
        min_s = int(info.get("min_stock", 500))
        max_s = int(info.get("max_stock", 2000))

        mask = df["component_id"] == comp_id
        n_rows = mask.sum()
        if n_rows == 0:
            continue

        if grp == "A":
            # (min+max)/2 付近 → 100%PO coverage で7ヶ月OK安定
            target_total = rng.randint(int((min_s + max_s) * 0.45), int((min_s + max_s) * 0.55))
        elif grp == "B":
            # max*0.75-0.90 → 110-120%PO で緩やかに上昇、後半OVER傾向
            target_total = rng.randint(int(max_s * 0.75), int(max_s * 0.90))
        elif grp == "C":
            # (min+max)/2 付近 → 88%PO で徐々に下降、5-6ヶ月はOK
            target_total = rng.randint(int((min_s + max_s) * 0.44), int((min_s + max_s) * 0.54))
        else:  # D (含 doomed)
            # min*0.1-0.2 → ZERO 傾向
            target_total = rng.randint(int(min_s * 0.10), int(min_s * 0.20))

        current_total = int(df.loc[mask, "stock_qty"].sum())
        if current_total == 0:
            if n_rows > 0:
                df.loc[mask, "stock_qty"] = target_total // n_rows
        else:
            ratio = target_total / max(current_total, 1)
            df.loc[mask, "stock_qty"] = (df.loc[mask, "stock_qty"] * ratio).astype(int).clip(lower=0)
        adjusted += 1

    print(f"  → inventory_current 調整: {adjusted} 部材のグループ別初期在庫設定完了")
    return df


# ════════════════════════════════════════════════════════════════
# マクニカフリー在庫 (新子安一極化)
# ════════════════════════════════════════════════════════════════
def gen_high_scenario_pos(
    partial_cids: set[str],
    inventory_df: pd.DataFrame,
    free_inv_df: pd.DataFrame,
    components_df: pd.DataFrame,
    po_id_start: int = 9000000,
) -> pd.DataFrame:
    """
    High (中) シナリオ専用POを生成。

    各 partial_comp に対し、以下を満たすPO1件を作成:
      - PO outstanding (po_qty) = max_single_route (PO が最大ルート)
      - cust + free + po_qty >= demand (combo_ok=1)
      - po_qty < demand (single_route_ok=0)
      - demand は gen_demand_plan_components で po_qty+50 〜 total_cap-50 に設定

    具体的な設計:
      - target_total = 3500-5500 (combo で満たす総量)
      - po_qty = target_total - cust_qty - free_qty (残りをPOで担う)
      - po_qty < MAX_DEMAND_QTY を保証
    """
    rows = []
    comp_info = components_df.set_index("component_id")[["supplier_id", "unit_price_usd"]].to_dict("index")

    # 4月・5月の2ヶ月分のPO (demand は2ヶ月発生するため)
    for month_offset, m in enumerate([date(2026, 4, 1), date(2026, 5, 1)]):
        delivery_day = rng.randint(5, 12)
        try:
            delivery_date = m.replace(day=delivery_day)
        except ValueError:
            delivery_date = m.replace(day=12)

        for i, comp_id in enumerate(sorted(partial_cids)):
            cust_qty = int(
                inventory_df[inventory_df["component_id"] == comp_id]["stock_qty"].sum()
            )
            free_qty = int(
                free_inv_df[free_inv_df["component_id"] == comp_id]["qty_available"].sum()
            )
            # target total_cap: 3000-5000 範囲
            target_total = rng.randint(3000, 5000)
            po_qty = max(0, target_total - cust_qty - free_qty)
            # PO は demand の上限 (MAX_DEMAND_QTY=6000) 未満に抑える
            po_qty = min(po_qty, MAX_DEMAND_QTY - 500)
            po_qty = max(po_qty, 500)  # 最低500

            info = comp_info.get(comp_id, {})
            supplier_id = info.get("supplier_id", "SUP001")
            unit_price = float(info.get("unit_price_usd", 10))

            order_date = delivery_date - timedelta(weeks=24)  # LT約24週

            po_id_val = po_id_start + i + month_offset * 100
            rows.append({
                "purchase_order_id":      f"PO_H{po_id_val:06d}",
                "component_id":           comp_id,
                "supplier_id":            supplier_id,
                "order_date":             order_date.isoformat(),
                "expected_delivery_date": delivery_date.isoformat(),
                "quantity":               po_qty,
                "received_qty":           0,
                "outstanding_qty":        po_qty,
                "status":                 "acknowledged",
                "is_delayed":             False,
                "delay_days":             0,
                "unit_cost_usd":          round(unit_price, 2),
                "total_cost_usd":         round(po_qty * unit_price, 2),
            })

    df = pd.DataFrame(rows)
    print(f"  → High シナリオ専用PO: {len(df)} 行 ({len(partial_cids)} 部材 × 2ヶ月)")
    return df


def gen_macnica_free_inventory(
    components_df: pd.DataFrame,
    doomed_comp_ids: set[str],
    partial_comp_ids: set[str],
) -> pd.DataFrame:
    rows = []
    free_id = 1
    target_components = components_df.sample(n=40, random_state=SEED).copy()

    for _, comp in target_components.iterrows():
        unit_price = float(comp["unit_price_usd"])
        comp_id = comp["component_id"]

        if comp_id in doomed_comp_ids:
            base_qty = rng.randint(10, 80)
        elif comp_id in partial_comp_ids:
            base_qty = rng.randint(50, 200)
        elif unit_price >= 20:
            base_qty = rng.randint(100, 400)
        elif unit_price >= 10:
            base_qty = rng.randint(200, 800)
        else:
            base_qty = rng.randint(300, 1200)

        qty = int(base_qty * rng.uniform(0.7, 1.0))
        rows.append({
            "free_inventory_id": f"FI{free_id:06d}",
            "component_id":      comp_id,
            "warehouse_id":      MACNICA_WAREHOUSE_ID,
            "qty_available":     qty,
            "as_of_date":        TODAY.isoformat(),
            "allocated_to":      TARGET_CUSTOMER_ID,
            "note":              "顧客向け引当在庫（マクニカ新子安ロジセンター保管）",
        })
        free_id += 1

    df = pd.DataFrame(rows)
    print(f"  → macnica_free_inventory: {len(df)} 行 (全て新子安)")
    return df


# ════════════════════════════════════════════════════════════════
# 需要計画 (Critical/High を精密制御)
# ════════════════════════════════════════════════════════════════
def gen_demand_plan_components(
    forecasts_df: pd.DataFrame,
    bom_df: pd.DataFrame,
    products_df: pd.DataFrame,
    components_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
    free_inv_df: pd.DataFrame,
    purchase_orders_df: pd.DataFrame,
    doomed_comp_ids: set[str],
    partial_comp_ids: set[str],
) -> pd.DataFrame:
    rows = []
    demand_id = 1

    cust_by_comp = (
        inventory_df.groupby("component_id", as_index=False)["stock_qty"].sum()
        .rename(columns={"stock_qty": "cust_qty"})
    )
    free_by_comp = (
        free_inv_df.groupby("component_id", as_index=False)["qty_available"].sum()
        .rename(columns={"qty_available": "free_qty"})
    )
    po_by_comp = (
        purchase_orders_df[purchase_orders_df["outstanding_qty"] > 0]
        .groupby("component_id", as_index=False)["outstanding_qty"].sum()
        .rename(columns={"outstanding_qty": "po_qty"})
    )

    capacity = components_df[["component_id"]].merge(cust_by_comp, on="component_id", how="left")
    capacity = capacity.merge(free_by_comp, on="component_id", how="left")
    capacity = capacity.merge(po_by_comp, on="component_id", how="left").fillna(0)
    capacity["total_cap"] = capacity["cust_qty"] + capacity["free_qty"] + capacity["po_qty"]
    capacity["max_single_route"] = capacity[["cust_qty", "free_qty", "po_qty"]].max(axis=1)
    cap_dict = capacity.set_index("component_id").to_dict("index")

    target_fcst = forecasts_df[forecasts_df["customer_id"] == TARGET_CUSTOMER_ID].copy()
    target_fcst["forecast_month"] = pd.to_datetime(
        target_fcst["forecast_month"], format="mixed", errors="coerce"
    ).dt.date

    cutoff_future = TODAY + timedelta(days=FUTURE_WINDOW_DAYS)
    active_fcst = target_fcst[
        (target_fcst["forecast_month"] > TODAY)
        & (target_fcst["forecast_month"] <= cutoff_future)
    ]

    for _, fc in active_fcst.iterrows():
        product_id = fc["product_id"]
        month_start = fc["forecast_month"]

        bom_for_product = bom_df[bom_df["product_id"] == product_id]
        for _, bom_row in bom_for_product.iterrows():
            comp_id = bom_row["component_id"]
            cap = cap_dict.get(comp_id, {})
            total_cap = int(cap.get("total_cap", 0))
            max_single = int(cap.get("max_single_route", 0))
            cust_only = int(cap.get("cust_qty", 0))

            is_target_month = (month_start.year == TARGET_MONTH.year and
                               month_start.month == TARGET_MONTH.month)
            is_partial_month = any(
                month_start.year == pm.year and month_start.month == pm.month
                for pm in PARTIAL_MONTHS
            )

            if comp_id in doomed_comp_ids:
                if is_target_month:
                    # Critical (重): 4月のみ、全ルートを超える需要
                    required_qty = min(
                        max(int(total_cap * rng.uniform(1.4, 1.9)) + 100, SHORTAGE_LARGE_MIN_QTY),
                        MAX_DEMAND_QTY,
                    )
                else:
                    # その他の月は通常需要 (充足させる)
                    required_qty = rng.randint(NORMAL_DEMAND_MIN, NORMAL_DEMAND_MAX)

            elif comp_id in partial_comp_ids:
                if is_partial_month:
                    # High (中): 4-5月の2ヶ月
                    if total_cap > max_single + 100:
                        lo = max_single + 50
                        hi = max(total_cap - 50, lo + 50)
                        required_qty = min(rng.randint(lo, hi), MAX_DEMAND_QTY)
                    else:
                        required_qty = min(
                            max(int(total_cap * 1.3) + 100, SHORTAGE_LARGE_MIN_QTY),
                            MAX_DEMAND_QTY,
                        )
                else:
                    # その他の月は通常需要
                    required_qty = rng.randint(NORMAL_DEMAND_MIN, NORMAL_DEMAND_MAX)

            else:
                # 通常需要 (OK)
                if cust_only > 500:
                    required_qty = rng.randint(
                        NORMAL_DEMAND_MIN,
                        min(NORMAL_DEMAND_MAX, max(cust_only // 10, NORMAL_DEMAND_MIN + 10)),
                    )
                else:
                    required_qty = rng.randint(NORMAL_DEMAND_MIN, NORMAL_DEMAND_MAX)

            req_date = _random_date_in_month(month_start)
            if req_date <= TODAY:
                req_date = TODAY + timedelta(days=rng.randint(3, 15))

            rows.append({
                "demand_id":      f"DM{demand_id:07d}",
                "component_id":   comp_id,
                "requested_date": req_date.isoformat(),
                "requested_qty":  int(max(required_qty, 1)),
                "source_type":    "FCST_AUTO",
                "product_id":     product_id,
                "customer_id":    TARGET_CUSTOMER_ID,
                "created_at":     (TODAY - timedelta(days=rng.randint(7, 30))).isoformat(),
                "note":           "製品FCSTから自動展開",
            })
            demand_id += 1

    # EMERGENCY_MANUAL: 突発的な緊急手配 (10件)
    emergency_samples = components_df.sample(n=10, random_state=SEED + 1)
    emergency_notes = [
        "親会社からの急な増産指示",
        "競合製品の不具合により受注急増",
        "既存ロットで品質問題、再手配",
        "新規プロジェクトの試作分",
        "客先からの緊急仕様変更",
        "在庫見込み誤差による補填",
        "代替部材調達失敗による手配",
        "量産立上げ前の追加検証用",
        "サンプル出荷分の補充",
        "新規受注対応の追加分",
    ]
    for i, (_, comp) in enumerate(emergency_samples.iterrows()):
        comp_id = comp["component_id"]
        days_ahead = rng.randint(5, 45)
        req_date = TODAY + timedelta(days=days_ahead)
        qty = rng.choice([50, 100, 150, 200, 300, 500])
        rows.append({
            "demand_id":      f"DM{demand_id:07d}",
            "component_id":   comp_id,
            "requested_date": req_date.isoformat(),
            "requested_qty":  qty,
            "source_type":    "EMERGENCY_MANUAL",
            "product_id":     None,
            "customer_id":    TARGET_CUSTOMER_ID,
            "created_at":     (TODAY - timedelta(days=rng.randint(0, 5))).isoformat(),
            "note":           emergency_notes[i % len(emergency_notes)],
        })
        demand_id += 1

    df = pd.DataFrame(rows)
    auto_n = (df["source_type"] == "FCST_AUTO").sum()
    emerg_n = (df["source_type"] == "EMERGENCY_MANUAL").sum()
    print(f"  → demand_plan_components: {len(df)} 行 (FCST_AUTO={auto_n}, EMERGENCY={emerg_n})")
    print(f"     Critical対象: {len(doomed_comp_ids)} 部材 (4月のみ)")
    print(f"     High対象:     {len(partial_comp_ids)} 部材 (4-5月)")
    print(f"     希望納期分布: {df['requested_date'].min()} 〜 {df['requested_date'].max()}")
    print(f"     需要数量分布: 中央値={int(df['requested_qty'].median())}, 最大={int(df['requested_qty'].max())}")
    return df


# ════════════════════════════════════════════════════════════════
# Forecasts 延長 (2026-12)
# ════════════════════════════════════════════════════════════════
def extend_forecasts_to_2026_12(fc_df: pd.DataFrame) -> pd.DataFrame:
    df = fc_df.copy()
    df["forecast_month"] = pd.to_datetime(df["forecast_month"], format="mixed", errors="coerce")

    target_end = pd.Timestamp("2026-12-01")
    current_end = df["forecast_month"].max()

    if current_end >= target_end:
        df["forecast_month"] = df["forecast_month"].dt.strftime("%Y-%m-%d")
        return df

    new_rows = []
    next_id = (
        df["forecast_id"].str.extract(r"FC(\d+)")[0].astype(int).max() + 1
        if "forecast_id" in df.columns else 1
    )
    for (pid, cid), grp in df.groupby(["product_id", "customer_id"]):
        last_known = grp["forecast_month"].max()
        last_qty = int(grp[grp["forecast_month"] == last_known]["forecast_qty"].iloc[0])
        m = last_known + pd.DateOffset(months=1)
        while m <= target_end:
            new_qty = max(50, int(last_qty * rng.uniform(0.85, 1.15)))
            acc = round(rng.uniform(0.70, 0.92), 4)
            new_rows.append({
                "forecast_id":       f"FC{next_id:06d}",
                "product_id":        pid,
                "customer_id":       cid,
                "forecast_month":    m.strftime("%Y-%m-%d"),
                "forecast_qty":      new_qty,
                "forecast_accuracy": acc,
                "created_at":        TODAY.isoformat(),
            })
            next_id += 1
            last_qty = new_qty
            m = m + pd.DateOffset(months=1)

    df["forecast_month"] = df["forecast_month"].dt.strftime("%Y-%m-%d")
    df_ext = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    print(f"  → forecasts.csv 延長: +{len(new_rows)} 行 (〜2026-12)")
    return df_ext


# ════════════════════════════════════════════════════════════════
# sales_orders 再生成 (Phase 10: 消費を9ヶ月に均等分散)
# ════════════════════════════════════════════════════════════════
def gen_sales_orders_distributed(
    products_df: pd.DataFrame,
    bom_df: pd.DataFrame,
    customers_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    受注残を9ヶ月 (4月〜12月 2026) に均等分散して再生成する。

    従来は TODAY+0〜60日 に集中していたため、在庫健全性スコアが急落していた。
    Phase 10: TODAY+4〜270日 に均等分散し、月次消費スパイクを解消する。
    """
    statuses = ["confirmed", "confirmed", "confirmed", "picking", "picking",
                "shipped_partial", "backorder", "backorder"]
    rows = []
    oid = 1

    for _ in range(120):
        # CUS001の製品を中心に選択
        prod_sample = products_df.sample(n=1, random_state=None).iloc[0]
        cust_rows = customers_df[customers_df["customer_id"] == prod_sample["customer_id"]]
        if cust_rows.empty:
            continue
        cust = cust_rows.iloc[0]

        order_date     = TODAY - timedelta(days=rng.randint(1, 30))
        # 4〜270日後に均等分散
        requested_date = TODAY + timedelta(days=rng.randint(4, 270))
        qty = rng.randint(5, 150) * 5

        status     = rng.choice(statuses)
        shipped_qty = 0
        if status == "shipped_partial":
            shipped_qty = int(qty * rng.uniform(0.3, 0.7))

        response_date = order_date + timedelta(days=rng.randint(1, 3))
        earliest_ship = requested_date - timedelta(days=rng.randint(5, 14))
        deadline_date = requested_date + timedelta(days=rng.randint(2, 7))
        partial_qty   = int(qty * rng.uniform(0.3, 0.7)) if status == "shipped_partial" else 0

        bom_for_prod = bom_df[bom_df["product_id"] == prod_sample["product_id"]]
        for _, b in bom_for_prod.iterrows():
            comp_qty = (qty - shipped_qty) * b["quantity_per_unit"]
            rows.append({
                "sales_order_id":          f"SO{oid:06d}",
                "customer_id":             cust["customer_id"],
                "customer_name":           cust["customer_name"],
                "product_id":              prod_sample["product_id"],
                "product_name":            prod_sample["product_name"],
                "component_id":            b["component_id"],
                "order_date":              order_date.isoformat(),
                "requested_delivery_date": requested_date.isoformat(),
                "response_date":           response_date.isoformat(),
                "earliest_ship_date":      earliest_ship.isoformat(),
                "deadline_date":           deadline_date.isoformat(),
                "order_qty":               qty,
                "shipped_qty":             shipped_qty,
                "remaining_qty":           qty - shipped_qty,
                "component_required_qty":  int(comp_qty),
                "partial_available_qty":   partial_qty,
                "status":                  status,
                "priority_flag":           requested_date <= TODAY + timedelta(days=7),
            })
        oid += 1

    df = pd.DataFrame(rows)
    print(f"  → sales_orders: {len(df)} 行 ({df['sales_order_id'].nunique()} 受注)")
    print(f"     requested_delivery_date: {df['requested_delivery_date'].min()} ~ {df['requested_delivery_date'].max()}")
    return df


# ════════════════════════════════════════════════════════════════
# 倉庫マスタにマクニカ新子安追加
# ════════════════════════════════════════════════════════════════
def add_macnica_warehouse(warehouses_df: pd.DataFrame) -> pd.DataFrame:
    if MACNICA_WAREHOUSE_ID in set(warehouses_df["warehouse_id"]):
        return warehouses_df
    new_row = {
        "warehouse_id":   MACNICA_WAREHOUSE_ID,
        "warehouse_name": MACNICA_WAREHOUSE_NAME,
        "prefecture":     "神奈川県",
        "city":           "横浜市鶴見区",
        "latitude":       35.5040,
        "longitude":      139.6789,
        "capacity_sqm":   8000,
    }
    df = pd.concat([warehouses_df, pd.DataFrame([new_row])], ignore_index=True)
    print(f"  → warehouses.csv: 新子安ロジスティクスセンター を追加 ({len(df)} 拠点)")
    return df


# ════════════════════════════════════════════════════════════════
# メイン
# ════════════════════════════════════════════════════════════════
def main():
    print(f"[Phase 10 データ再生成] 基準日={TODAY.isoformat()}, 顧客={TARGET_CUSTOMER_ID}")

    components_df = pd.read_csv(OUT / "components.csv",         encoding="utf-8-sig")
    warehouses_df = pd.read_csv(OUT / "warehouses.csv",         encoding="utf-8-sig")
    forecasts_df  = pd.read_csv(OUT / "forecasts.csv",          encoding="utf-8-sig")
    bom_df        = pd.read_csv(OUT / "bom.csv",                encoding="utf-8-sig")
    products_df   = pd.read_csv(OUT / "products.csv",           encoding="utf-8-sig")
    customers_df  = pd.read_csv(OUT / "customers.csv",          encoding="utf-8-sig")
    inventory_df  = pd.read_csv(OUT / "inventory_current.csv",  encoding="utf-8-sig")

    inventory_df["stock_qty"] = pd.to_numeric(inventory_df["stock_qty"], errors="coerce").fillna(0)

    # ⓪a forecasts 延長
    forecasts_df = extend_forecasts_to_2026_12(forecasts_df)
    forecasts_df.to_csv(OUT / "forecasts.csv", index=False, encoding="utf-8-sig")

    # ⓪b sales_orders 再生成 (Phase 10: 消費を9ヶ月に均等分散)
    sales_df = gen_sales_orders_distributed(products_df, bom_df, customers_df)
    sales_df.to_csv(OUT / "sales_orders.csv", index=False, encoding="utf-8-sig")

    # ① 倉庫マスタ
    warehouses_df = add_macnica_warehouse(warehouses_df)
    warehouses_df.to_csv(OUT / "warehouses.csv", index=False, encoding="utf-8-sig")

    # ② BOMシナリオ選出
    doomed_pid, partial_pids, doomed_cids, partial_cids = _pick_bom_scenarios(bom_df, products_df)
    print(f"  → BOM充足シナリオ:")
    print(f"     [Critical製品] {doomed_pid} (BOM {len(doomed_cids)} 部材 → 4月のみ重)")
    print(f"     [High製品]     {partial_pids} (計 {len(partial_cids)} 部材 → 4-5月のみ中)")

    # ③ 在庫グループ分類
    inv_groups = _assign_inventory_groups(components_df, doomed_cids, partial_cids)
    group_counts = {g: sum(1 for v in inv_groups.values() if v == g) for g in "ABCD"}
    print(f"  → 在庫グループ分類: {group_counts} (A=OK, B=OVER, C=UNDER, D=ZERO)")

    # ④ スマートPO生成 (FCST連動)
    po_smart = gen_purchase_orders_smart(
        components_df, bom_df, forecasts_df, inv_groups, doomed_cids, partial_cids
    )
    po_smart.to_csv(OUT / "purchase_orders.csv", index=False, encoding="utf-8-sig")

    # ⑤ inventory_current をグループ別初期在庫に調整
    inventory_df = adjust_inventory_for_scenarios(inventory_df, inv_groups, components_df)
    inventory_df.to_csv(OUT / "inventory_current.csv", index=False, encoding="utf-8-sig")

    # ⑥ フリー在庫 (新子安一極化)
    free_inv_df = gen_macnica_free_inventory(components_df, doomed_cids, partial_cids)
    free_inv_df.to_csv(OUT / "macnica_free_inventory.csv", index=False, encoding="utf-8-sig")

    # ⑦ Highシナリオ専用PO (partial_cids 用)
    po_high = gen_high_scenario_pos(
        partial_cids, inventory_df, free_inv_df, components_df
    )
    # スマートPO + Highシナリオ専用PO を結合
    po_all = pd.concat([po_smart, po_high], ignore_index=True)
    po_all.to_csv(OUT / "purchase_orders.csv", index=False, encoding="utf-8-sig")

    # ⑧ 需要計画生成 (最終PO capacityを使う)
    demand_df = gen_demand_plan_components(
        forecasts_df, bom_df, products_df, components_df,
        inventory_df, free_inv_df, po_all,
        doomed_cids, partial_cids,
    )
    demand_df.to_csv(OUT / "demand_plan_components.csv", index=False, encoding="utf-8-sig")
    # (purchase_orders.csv は po_all として⑦で保存済み)

    print("\n[完了] 出力ファイル:")
    for f in ["warehouses.csv", "purchase_orders.csv", "inventory_current.csv",
              "macnica_free_inventory.csv", "demand_plan_components.csv", "forecasts.csv",
              "sales_orders.csv"]:
        print(f"  - {OUT / f}")


if __name__ == "__main__":
    main()
