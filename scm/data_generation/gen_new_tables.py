"""
SCM Demo - 新規テーブル生成スクリプト (Phase 6 改訂版)
=========================================================
ユーザーフィードバック対応:
  ✅ 数量を現実的にスケールダウン (60K → 数百〜数千)
  ✅ 希望納期は月内ランダム (5〜25日)
  ✅ 「新規発注必要（LT考慮）」が確実に発生するデータ
  ✅ BOM充足を逆算: 生産困難=1製品, 部分不足=3製品 を保証
  ✅ purchase_orders の outstanding_qty を現実値にキャップ
  ✅ マクニカフリー在庫を新子安(=マクニカ倉庫)一極化

設計方針:
  - 「生産困難スケープゴート製品」(1製品): その全BOM部材を SHORTAGE_LARGE に
  - 「部分不足スケープゴート製品」(3製品): その1部材を SHORTAGE_LARGE に
  - これにより `gold_bom_fulfillment_status` が確実に 🔴1製品 + 🟡3製品 を生む

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

# 「今日」: services/config.py と整合
TODAY = date(2026, 3, 28)

# 対象顧客 (今回のデモは1社固定)
TARGET_CUSTOMER_ID = "CUS001"

# 期間
PAST_WINDOW_DAYS   = 30
FUTURE_WINDOW_DAYS = 180

# 数量上限 (現実的なスケール)
MAX_DEMAND_QTY = 6000           # 単一需要の上限
SHORTAGE_LARGE_MIN_QTY = 4000   # SHORTAGE_LARGE 需要の最低数量 (cap=3000+free+cust より大きい)
NORMAL_DEMAND_MIN = 30
NORMAL_DEMAND_MAX = 400         # 通常需要の上限

# シナリオ対象部材の顧客在庫キャップ (重/中シナリオを発動させるため)
DOOMED_CUST_CAP = (10, 200)    # 重: 10〜200個 (cust+free+PO < requested で「重」)
PARTIAL_CUST_CAP = (300, 1500) # 中: cust+free+PO がギリギリ充足できる

# 集中対象月 (BOM充足ビューで🔴🟡を確実に発生させたい月)
TARGET_MONTH = date(2026, 4, 1)   # 4月

# PO outstanding 上限 (1部材あたり)
PO_CAP_PER_COMP = 3000

# マクニカ「新子安ロジスティクスセンター」(物理的に1拠点)
MACNICA_WAREHOUSE_ID = "WH_MAC_SHINKOYASU"
MACNICA_WAREHOUSE_NAME = "新子安ロジスティクスセンター"


# ════════════════════════════════════════════════════════════════
# 1. マクニカフリー在庫 (新子安一極化)
# ════════════════════════════════════════════════════════════════
def gen_macnica_free_inventory(
    components_df: pd.DataFrame,
    doomed_comp_ids: set[str],
    partial_comp_ids: set[str],
) -> pd.DataFrame:
    """
    マクニカが顧客 CUS001 向けに引当済みのフリー在庫を生成。
    全在庫を新子安ロジスティクスセンターに集約 (マクニカ倉庫=新子安のみ)。

    フィードバック対応: 倉庫=顧客倉庫の概念とは別物。マクニカ在庫は1拠点(新子安)を経由する。
    """
    rows = []
    free_id = 1

    target_components = components_df.sample(n=40, random_state=SEED).copy()

    for _, comp in target_components.iterrows():
        unit_price = float(comp["unit_price_usd"])
        comp_id = comp["component_id"]

        # SHORTAGE_LARGE 部材は意図的に「少なめ」(顧客在庫+PO+フリーで不足を演出)
        if comp_id in doomed_comp_ids:
            base_qty = rng.randint(10, 80)       # 微量
        elif comp_id in partial_comp_ids:
            base_qty = rng.randint(50, 200)      # 少なめ
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
    print(f"  → macnica_free_inventory: {len(df)} 行 ({df['component_id'].nunique()} 部材, 全て新子安)")
    return df


# ════════════════════════════════════════════════════════════════
# 2. 倉庫マスタに新子安を追加 (顧客倉庫と区別)
# ════════════════════════════════════════════════════════════════
def add_macnica_warehouse(warehouses_df: pd.DataFrame) -> pd.DataFrame:
    """既存倉庫マスタにマクニカ新子安を追加。既存倉庫=顧客倉庫として残す。"""
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
# 3. 需要計画 (部材レベル) - BOM充足を逆算
# ════════════════════════════════════════════════════════════════
def _random_date_in_month(month_start: date) -> date:
    day = rng.randint(5, 25)
    try:
        return month_start.replace(day=day)
    except ValueError:
        return month_start.replace(day=15)


def _pick_bom_scenarios(
    bom_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> tuple[str, list[str], set[str], set[str]]:
    """
    BOM充足ビューで🔴1製品/🟡3製品 を確実に生むため、スケープゴート製品を逆算で選出。

    Returns
    -------
    doomed_product_id : str
        全部材を不足にする「生産困難スケープゴート」製品
    partial_product_ids : list[str]
        1部材だけ不足にする「部分不足スケープゴート」3製品
    doomed_comp_ids : set[str]
        SHORTAGE_LARGE にする部材集合
    partial_comp_ids : set[str]
        SHORTAGE_PARTIAL にする部材集合 (組合せで充足可)
    """
    target_products = products_df[products_df["customer_id"] == TARGET_CUSTOMER_ID].copy()
    if target_products.empty:
        target_products = products_df.copy()

    # BOM部材数を計算
    bom_size = bom_df.groupby("product_id").size().reset_index(name="bom_count")
    target_products = target_products.merge(bom_size, on="product_id", how="left").fillna({"bom_count": 0})
    target_products["bom_count"] = target_products["bom_count"].astype(int)

    # BOM5部材以上の製品を候補に
    candidates = target_products[target_products["bom_count"] >= 5].copy()
    if len(candidates) < 4:
        candidates = target_products[target_products["bom_count"] >= 3].copy()

    candidates = candidates.sample(n=min(4, len(candidates)), random_state=SEED).reset_index(drop=True)

    if len(candidates) < 4:
        # 候補が足りない場合のフォールバック
        candidates = target_products.head(4).copy().reset_index(drop=True)

    doomed_product_id = candidates.iloc[0]["product_id"]
    partial_product_ids = candidates.iloc[1:4]["product_id"].tolist()

    # 生産困難製品: その製品の全部材を SHORTAGE_LARGE 化
    doomed_bom = bom_df[bom_df["product_id"] == doomed_product_id]
    doomed_comp_ids = set(doomed_bom["component_id"].tolist())

    # 部分不足製品: 各製品から1部材を SHORTAGE_LARGE 化
    #   doomed_comp_ids と重複させないように選ぶ
    partial_comp_ids = set()
    for pid in partial_product_ids:
        pbom = bom_df[(bom_df["product_id"] == pid) & (~bom_df["component_id"].isin(doomed_comp_ids))]
        if pbom.empty:
            continue
        chosen = pbom.sample(n=1, random_state=SEED).iloc[0]["component_id"]
        partial_comp_ids.add(chosen)

    return doomed_product_id, partial_product_ids, doomed_comp_ids, partial_comp_ids


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
    """部材レベルの希望納期データを生成 (BOM充足の逆算結果を組込)。"""
    rows = []
    demand_id = 1

    # 部材ごとの「全ルート合計確保可能数」を事前計算
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

    # ── A. FCST_AUTO: 製品FCSTから自動展開 ──
    target_fcst = forecasts_df[forecasts_df["customer_id"] == TARGET_CUSTOMER_ID].copy()
    target_fcst["forecast_month"] = pd.to_datetime(
        target_fcst["forecast_month"], format="mixed", errors="coerce"
    ).dt.date

    cutoff_past = TODAY - timedelta(days=PAST_WINDOW_DAYS)
    cutoff_future = TODAY + timedelta(days=FUTURE_WINDOW_DAYS)
    active_fcst = target_fcst[
        (target_fcst["forecast_month"] >= cutoff_past)
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

            # 部材ごとに数量を決定
            if comp_id in doomed_comp_ids:
                # 「重」シナリオ: combo_ok=0 (cust+free+PO < requested_qty)
                base_target = max(int(total_cap * rng.uniform(1.3, 1.8)) + 100, SHORTAGE_LARGE_MIN_QTY)
                required_qty = min(base_target, MAX_DEMAND_QTY)
            elif comp_id in partial_comp_ids:
                # 「中」シナリオ: max_single < requested_qty <= total_cap (combo_ok=1, single_ok=0)
                if total_cap > max_single + 100:
                    lo = max_single + 50
                    hi = max(total_cap - 50, lo + 50)
                    required_qty = rng.randint(lo, hi)
                else:
                    # 単一ルートが支配的: 重シナリオへフォールバック
                    required_qty = max(int(total_cap * 1.3) + 100, SHORTAGE_LARGE_MIN_QTY)
                required_qty = min(required_qty, MAX_DEMAND_QTY)
            else:
                # 通常需要: 顧客在庫だけで賄える小規模
                if cust_only > 500:
                    required_qty = rng.randint(NORMAL_DEMAND_MIN, min(NORMAL_DEMAND_MAX, max(cust_only // 8, NORMAL_DEMAND_MIN + 10)))
                else:
                    required_qty = rng.randint(NORMAL_DEMAND_MIN, NORMAL_DEMAND_MAX)

            req_date = _random_date_in_month(month_start)
            if req_date < TODAY:
                req_date = TODAY + timedelta(days=rng.randint(7, 30))

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

    # ── B. EMERGENCY_MANUAL: 突発的な緊急手配 (10件) ──
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
        days_ahead = rng.randint(3, 45)
        req_date = TODAY + timedelta(days=days_ahead)

        cap = cap_dict.get(comp_id, {})
        max_single = int(cap.get("max_single_route", 100))
        # 緊急の半分は不足ボリュームに、ただし現実的なスケール
        if i < 5 and max_single > 0:
            qty = min(int(max_single * rng.uniform(1.1, 1.5)), MAX_DEMAND_QTY)
        else:
            qty = rng.choice([30, 50, 100, 150, 200, 300, 500])

        rows.append({
            "demand_id":      f"DM{demand_id:07d}",
            "component_id":   comp_id,
            "requested_date": req_date.isoformat(),
            "requested_qty":  int(max(qty, 1)),
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
    print(f"     SHORTAGE_LARGE対象: {len(doomed_comp_ids)} 部材, SHORTAGE_PARTIAL対象: {len(partial_comp_ids)} 部材")
    print(f"     希望納期分布: {df['requested_date'].min()} 〜 {df['requested_date'].max()}")
    print(f"     需要数量分布: 中央値={int(df['requested_qty'].median())}, 最大={int(df['requested_qty'].max())}")
    return df


# ════════════════════════════════════════════════════════════════
# 4. 既存POの outstanding_qty 現実化
# ════════════════════════════════════════════════════════════════
def adjust_inventory_for_scenarios(
    inv_df: pd.DataFrame,
    doomed_cids: set[str],
    partial_cids: set[str],
) -> pd.DataFrame:
    """
    シナリオ部材の顧客在庫(stock_qty) をキャップして、
    重/中の action_level が確実に発火するように調整する。
    """
    df = inv_df.copy()
    df["stock_qty"] = pd.to_numeric(df["stock_qty"], errors="coerce").fillna(0).astype(int)

    # 部材ごとの合計在庫を計算
    by_comp = df.groupby("component_id")["stock_qty"].sum()
    n_doomed_adjusted = 0
    n_partial_adjusted = 0

    for comp_id in doomed_cids:
        if comp_id in by_comp.index:
            target_total = rng.randint(DOOMED_CUST_CAP[0], DOOMED_CUST_CAP[1])
            current_total = int(by_comp[comp_id])
            if current_total > target_total:
                ratio = target_total / max(current_total, 1)
                mask = df["component_id"] == comp_id
                df.loc[mask, "stock_qty"] = (df.loc[mask, "stock_qty"] * ratio).astype(int).clip(lower=0)
                n_doomed_adjusted += 1

    for comp_id in partial_cids:
        if comp_id in by_comp.index:
            target_total = rng.randint(PARTIAL_CUST_CAP[0], PARTIAL_CUST_CAP[1])
            current_total = int(by_comp[comp_id])
            if current_total > target_total:
                ratio = target_total / max(current_total, 1)
                mask = df["component_id"] == comp_id
                df.loc[mask, "stock_qty"] = (df.loc[mask, "stock_qty"] * ratio).astype(int).clip(lower=0)
                n_partial_adjusted += 1

    print(f"  → inventory_current 調整: {n_doomed_adjusted} 重部材 / {n_partial_adjusted} 中部材 を低在庫化")
    return df


def realign_purchase_orders(po_df: pd.DataFrame) -> pd.DataFrame:
    """1部材あたりの合計outstanding を PO_CAP_PER_COMP 個に均等キャップ。"""
    df = po_df.copy()
    df["outstanding_qty"] = pd.to_numeric(df["outstanding_qty"], errors="coerce").fillna(0).astype(int)
    df["received_qty"] = pd.to_numeric(df["received_qty"], errors="coerce").fillna(0).astype(int)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

    by_comp = df.groupby("component_id")["outstanding_qty"].sum()
    for comp_id, total in by_comp.items():
        if total > PO_CAP_PER_COMP:
            ratio = PO_CAP_PER_COMP / total
            mask = (df["component_id"] == comp_id) & (df["outstanding_qty"] > 0)
            df.loc[mask, "outstanding_qty"] = (df.loc[mask, "outstanding_qty"] * ratio).astype(int)
            df.loc[mask, "received_qty"] = df.loc[mask, "quantity"] - df.loc[mask, "outstanding_qty"]
    print(f"  → purchase_orders 修正: 1部材あたり outstanding_qty 上限 {PO_CAP_PER_COMP} 個に調整")
    return df


# ════════════════════════════════════════════════════════════════
# メイン
# ════════════════════════════════════════════════════════════════
def main():
    print(f"[新規テーブル生成 Phase6] 基準日 (TODAY) = {TODAY.isoformat()}, 対象顧客 = {TARGET_CUSTOMER_ID}")

    components_df = pd.read_csv(OUT / "components.csv", encoding="utf-8-sig")
    warehouses_df = pd.read_csv(OUT / "warehouses.csv", encoding="utf-8-sig")
    forecasts_df  = pd.read_csv(OUT / "forecasts.csv",  encoding="utf-8-sig")
    bom_df        = pd.read_csv(OUT / "bom.csv",        encoding="utf-8-sig")
    products_df   = pd.read_csv(OUT / "products.csv",   encoding="utf-8-sig")
    inventory_df  = pd.read_csv(OUT / "inventory_current.csv", encoding="utf-8-sig")
    po_df         = pd.read_csv(OUT / "purchase_orders.csv",   encoding="utf-8-sig")

    inventory_df["stock_qty"] = pd.to_numeric(inventory_df["stock_qty"], errors="coerce").fillna(0)

    # ① 倉庫マスタにマクニカ新子安を追加
    warehouses_df = add_macnica_warehouse(warehouses_df)
    warehouses_df.to_csv(OUT / "warehouses.csv", index=False, encoding="utf-8-sig")

    # ② POを先に現実化
    po_fixed = realign_purchase_orders(po_df)
    po_fixed.to_csv(OUT / "purchase_orders.csv", index=False, encoding="utf-8-sig")

    # ③ BOM充足の逆算でスケープゴート製品/部材を選出
    doomed_pid, partial_pids, doomed_cids, partial_cids = _pick_bom_scenarios(bom_df, products_df)
    print(f"  → BOM充足シナリオ:")
    print(f"     [生産困難] 製品 {doomed_pid} (BOM {len(doomed_cids)} 部材を全て不足化)")
    print(f"     [部分不足] 製品 {partial_pids} (各製品1部材を不足化, 計 {len(partial_cids)} 部材)")

    # ④ 顧客在庫を調整 (シナリオ部材のみ低在庫化して action_level 発動を保証)
    inventory_df = adjust_inventory_for_scenarios(inventory_df, doomed_cids, partial_cids)
    inventory_df.to_csv(OUT / "inventory_current.csv", index=False, encoding="utf-8-sig")

    # ⑤ フリー在庫 (新子安一極化)
    free_inv_df = gen_macnica_free_inventory(components_df, doomed_cids, partial_cids)
    free_inv_df.to_csv(OUT / "macnica_free_inventory.csv", index=False, encoding="utf-8-sig")

    # ⑥ 需要計画
    demand_df = gen_demand_plan_components(
        forecasts_df, bom_df, products_df, components_df,
        inventory_df, free_inv_df, po_fixed,
        doomed_cids, partial_cids,
    )
    demand_df.to_csv(OUT / "demand_plan_components.csv", index=False, encoding="utf-8-sig")

    print("\n[完了] 出力ファイル:")
    print(f"  - {OUT / 'warehouses.csv'} (更新)")
    print(f"  - {OUT / 'purchase_orders.csv'} (修正)")
    print(f"  - {OUT / 'inventory_current.csv'} (シナリオ部材を低在庫化)")
    print(f"  - {OUT / 'macnica_free_inventory.csv'}")
    print(f"  - {OUT / 'demand_plan_components.csv'}")


if __name__ == "__main__":
    main()
