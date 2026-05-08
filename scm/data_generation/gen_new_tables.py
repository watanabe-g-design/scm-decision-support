"""
SCM Demo - 新規テーブル生成スクリプト (Phase 4 改訂版)
=========================================================
本デモAppの大幅再設計にともない、以下2本のSilverテーブルのソースCSVを生成する:

  1. macnica_free_inventory.csv
     - マクニカが顧客向けに引当済みの「フリー在庫」
     - 顧客側在庫 (inventory_current.csv) と区別する
     - 今回は顧客 CUS001 (ネクサス精機株式会社) 向けに固定

  2. demand_plan_components.csv
     - 部材レベルの希望納期データ
     - source_type = "FCST_AUTO":      製品FCST × BOM展開で自動生成
     - source_type = "EMERGENCY_MANUAL": 緊急の手動入力 (シミュレーション用)

Phase 4 改善:
  ✅ 希望納期を月内ランダム化 (5〜25日)
  ✅ 過去日付の需要を除外 (今日以降のみ)
  ✅ 数百件は維持しつつ「不足大10件」「一部不足20件」を意図的に発生
  ✅ 同一(部材, 月)を集約しつつ製品の多様性は維持

実行:
    python -m data_generation.gen_new_tables
"""
import random
from datetime import date, timedelta, datetime
from pathlib import Path

import pandas as pd

SEED = 4242
rng = random.Random(SEED)

OUT = Path(__file__).parent.parent / "sample_data"

# 「今日」: services/config.py と整合させる
TODAY = date(2026, 3, 28)

# 対象顧客 (今回のデモは1社固定)
TARGET_CUSTOMER_ID = "CUS001"

# 期間
PAST_WINDOW_DAYS   = 30   # 直近30日も含める (進行中の需要)
FUTURE_WINDOW_DAYS = 180  # 6ヶ月先まで


# ════════════════════════════════════════════════════════════════
# 1. マクニカフリー在庫
# ════════════════════════════════════════════════════════════════
def gen_macnica_free_inventory(components_df: pd.DataFrame, warehouses_df: pd.DataFrame) -> pd.DataFrame:
    """
    マクニカが顧客 CUS001 向けに引当済みのフリー在庫を生成。

    設計方針:
      - 全90部材中、約35部材のみ引当 (主要部材に絞る)
      - 1部材あたり1〜2倉庫に保管
      - 数量は控えめ (顧客側在庫より少なめ): 30〜800個程度
      - 高価格部材は数量を少なく、低価格部材は数量を多く
    """
    rows = []
    free_id = 1

    target_components = components_df.sample(n=35, random_state=SEED).copy()
    macnica_warehouses = warehouses_df["warehouse_id"].head(5).tolist()

    for _, comp in target_components.iterrows():
        n_wh = rng.choices([1, 2], weights=[0.7, 0.3])[0]
        selected_whs = rng.sample(macnica_warehouses, k=n_wh)

        unit_price = float(comp["unit_price_usd"])
        if unit_price >= 20:
            base_qty = rng.randint(30, 200)
        elif unit_price >= 10:
            base_qty = rng.randint(80, 400)
        else:
            base_qty = rng.randint(150, 800)

        for wh_id in selected_whs:
            qty = int(base_qty * rng.uniform(0.6, 1.0))
            rows.append({
                "free_inventory_id": f"FI{free_id:06d}",
                "component_id":      comp["component_id"],
                "warehouse_id":      wh_id,
                "qty_available":     qty,
                "as_of_date":        TODAY.isoformat(),
                "allocated_to":      TARGET_CUSTOMER_ID,
                "note":              "顧客向け引当在庫（マクニカ保有）",
            })
            free_id += 1

    df = pd.DataFrame(rows)
    print(f"  → macnica_free_inventory: {len(df)} 行 ({df['component_id'].nunique()} 部材, {df['warehouse_id'].nunique()} 倉庫)")
    return df


# ════════════════════════════════════════════════════════════════
# 2. 需要計画 (部材レベル) - Phase 4 改訂
# ════════════════════════════════════════════════════════════════
def _random_date_in_month(month_start: date) -> date:
    """月内ランダム日付 (5〜25日のいずれか)"""
    day = rng.randint(5, 25)
    try:
        return month_start.replace(day=day)
    except ValueError:
        return month_start.replace(day=15)


def gen_demand_plan_components(
    forecasts_df: pd.DataFrame,
    bom_df: pd.DataFrame,
    products_df: pd.DataFrame,
    components_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
    free_inv_df: pd.DataFrame,
    purchase_orders_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    部材レベルの希望納期データを生成。

    Phase 4 改善:
      - 過去日付の需要を排除 (PAST_WINDOW以内のみ含める)
      - 月内ランダムな希望納期
      - 不足を意図的に発生:
          * 不足大10件:  全ルート合計より大きい数量を要求
          * 一部不足20件: 単一ルートでは足りないが組合せで充足可能な数量
          * 充足30件:   余裕あり (顧客在庫だけで充足)
        → 残りはランダムだが充足傾向
    """
    rows = []
    demand_id = 1

    # 部材ごとの「全ルート合計確保可能数」を事前計算 (不足設計の基準)
    # ① 顧客在庫
    cust_by_comp = (
        inventory_df.groupby("component_id", as_index=False)["stock_qty"]
        .sum()
        .rename(columns={"stock_qty": "cust_qty"})
    )
    # ② フリー在庫
    free_by_comp = (
        free_inv_df.groupby("component_id", as_index=False)["qty_available"]
        .sum()
        .rename(columns={"qty_available": "free_qty"})
    )
    # ③ PO outstanding (現実化のためキャップ後の値を想定)
    po_by_comp = (
        purchase_orders_df[purchase_orders_df["outstanding_qty"] > 0]
        .groupby("component_id", as_index=False)["outstanding_qty"]
        .sum()
        .rename(columns={"outstanding_qty": "po_qty"})
    )

    capacity = components_df[["component_id"]].merge(cust_by_comp, on="component_id", how="left")
    capacity = capacity.merge(free_by_comp, on="component_id", how="left")
    capacity = capacity.merge(po_by_comp, on="component_id", how="left")
    capacity = capacity.fillna(0)
    capacity["total_cap"] = capacity["cust_qty"] + capacity["free_qty"] + capacity["po_qty"]
    capacity["max_single_route"] = capacity[["cust_qty", "free_qty", "po_qty"]].max(axis=1)

    cap_dict = capacity.set_index("component_id").to_dict("index")

    # 不足設計: 部材から3グループに分類
    all_comps = components_df["component_id"].tolist()
    rng.shuffle(all_comps)
    SHORTAGE_LARGE_COMPS = all_comps[:10]      # 不足大: 10件
    SHORTAGE_PARTIAL_COMPS = all_comps[10:30]  # 一部不足: 20件
    # 残りはランダム（充足傾向）

    # ── A. FCST_AUTO: 製品FCSTから自動展開 ──
    target_fcst = forecasts_df[forecasts_df["customer_id"] == TARGET_CUSTOMER_ID].copy()
    if not target_fcst.empty:
        target_fcst["forecast_month"] = pd.to_datetime(
            target_fcst["forecast_month"], format="mixed", errors="coerce"
        ).dt.date

        # 直近〜未来のFCSTのみ
        cutoff_past = TODAY - timedelta(days=PAST_WINDOW_DAYS)
        cutoff_future = TODAY + timedelta(days=FUTURE_WINDOW_DAYS)
        active_fcst = target_fcst[
            (target_fcst["forecast_month"] >= cutoff_past)
            & (target_fcst["forecast_month"] <= cutoff_future)
        ]

        for _, fc in active_fcst.iterrows():
            product_id = fc["product_id"]
            fcst_qty = int(fc["forecast_qty"])
            month_start = fc["forecast_month"]

            # BOM展開
            bom_for_product = bom_df[bom_df["product_id"] == product_id]
            for _, bom_row in bom_for_product.iterrows():
                comp_id = bom_row["component_id"]
                qty_per_unit = int(bom_row.get("quantity_per_unit", 1) or 1)
                base_required = fcst_qty * qty_per_unit

                # 不足設計に従って数量を調整
                cap = cap_dict.get(comp_id, {})
                total_cap = int(cap.get("total_cap", 0))
                max_single = int(cap.get("max_single_route", 0))

                if comp_id in SHORTAGE_LARGE_COMPS:
                    # 全ルート合計を超える需要を生成 (不足大)
                    required_qty = max(int(total_cap * rng.uniform(1.3, 2.0)), base_required, 500)
                elif comp_id in SHORTAGE_PARTIAL_COMPS:
                    # 単一ルートは足りないが組合せで充足できる数量
                    required_qty = int(max_single * rng.uniform(1.2, 1.8))
                    if required_qty < base_required:
                        required_qty = base_required
                    if required_qty > total_cap:
                        required_qty = max(int(total_cap * 0.85), 100)
                else:
                    # 充足傾向: 顧客在庫だけで賄える小さな数量
                    cust_only = int(cap.get("cust_qty", 0))
                    if cust_only > 100:
                        required_qty = int(cust_only * rng.uniform(0.05, 0.4))
                    else:
                        required_qty = rng.randint(20, 150)

                # 希望納期: 月内ランダム日付
                req_date = _random_date_in_month(month_start)
                # 過去日付なら今日以降にずらす
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
        # 緊急なので近い未来 (3〜45日先)
        days_ahead = rng.randint(3, 45)
        req_date = TODAY + timedelta(days=days_ahead)

        # 緊急の半分は不足を起こすボリュームに
        cap = cap_dict.get(comp_id, {})
        max_single = int(cap.get("max_single_route", 100))
        if i < 5:
            qty = int(max_single * rng.uniform(1.1, 1.5))  # 単一ルート不足
        else:
            qty = rng.choice([20, 50, 80, 100, 150, 200, 300])  # 通常規模

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
    print(f"  → demand_plan_components: {len(df)} 行 (FCST_AUTO={auto_n}, EMERGENCY_MANUAL={emerg_n})")
    print(f"     不足大対象: {len(SHORTAGE_LARGE_COMPS)} 部材, 一部不足対象: {len(SHORTAGE_PARTIAL_COMPS)} 部材")
    print(f"     希望納期分布: {df['requested_date'].min()} 〜 {df['requested_date'].max()}")
    return df


# ════════════════════════════════════════════════════════════════
# 3. 既存POの outstanding_qty 現実化 (上書き保存)
# ════════════════════════════════════════════════════════════════
def realign_purchase_orders(po_df: pd.DataFrame) -> pd.DataFrame:
    """
    purchase_orders.csv の outstanding_qty を現実的な数値にキャップする。
    1部材あたりの合計outstanding が 3000個を超えないように調整。
    """
    df = po_df.copy()
    df["outstanding_qty"] = pd.to_numeric(df["outstanding_qty"], errors="coerce").fillna(0).astype(int)
    df["received_qty"] = pd.to_numeric(df["received_qty"], errors="coerce").fillna(0).astype(int)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

    # 部材ごとの合計outstandingを集計
    by_comp = df.groupby("component_id")["outstanding_qty"].sum()

    CAP_PER_COMP = 3000
    for comp_id, total in by_comp.items():
        if total > CAP_PER_COMP:
            # 比例縮小
            ratio = CAP_PER_COMP / total
            mask = (df["component_id"] == comp_id) & (df["outstanding_qty"] > 0)
            df.loc[mask, "outstanding_qty"] = (df.loc[mask, "outstanding_qty"] * ratio).astype(int)
            # received_qty も整合性のため再計算 (quantity - outstanding)
            df.loc[mask, "received_qty"] = df.loc[mask, "quantity"] - df.loc[mask, "outstanding_qty"]

    print(f"  → purchase_orders 修正: 1部材あたり outstanding_qty 上限 {CAP_PER_COMP} 個に調整")
    return df


# ════════════════════════════════════════════════════════════════
# メイン
# ════════════════════════════════════════════════════════════════
def main():
    print(f"[新規テーブル生成] 基準日 (TODAY) = {TODAY.isoformat()}, 対象顧客 = {TARGET_CUSTOMER_ID}")

    components_df = pd.read_csv(OUT / "components.csv", encoding="utf-8-sig")
    warehouses_df = pd.read_csv(OUT / "warehouses.csv", encoding="utf-8-sig")
    forecasts_df  = pd.read_csv(OUT / "forecasts.csv",  encoding="utf-8-sig")
    bom_df        = pd.read_csv(OUT / "bom.csv",        encoding="utf-8-sig")
    products_df   = pd.read_csv(OUT / "products.csv",   encoding="utf-8-sig")
    inventory_df  = pd.read_csv(OUT / "inventory_current.csv", encoding="utf-8-sig")
    po_df         = pd.read_csv(OUT / "purchase_orders.csv",   encoding="utf-8-sig")

    # POを先に現実化
    po_fixed = realign_purchase_orders(po_df)
    po_fixed.to_csv(OUT / "purchase_orders.csv", index=False, encoding="utf-8-sig")
    print(f"  → purchase_orders.csv 上書き保存")

    # フリー在庫
    free_inv_df = gen_macnica_free_inventory(components_df, warehouses_df)
    free_inv_df.to_csv(OUT / "macnica_free_inventory.csv", index=False, encoding="utf-8-sig")

    # 需要計画 (POキャップ済の数値を使う)
    inventory_df["stock_qty"] = pd.to_numeric(inventory_df["stock_qty"], errors="coerce").fillna(0)
    demand_df = gen_demand_plan_components(
        forecasts_df, bom_df, products_df, components_df,
        inventory_df, free_inv_df, po_fixed,
    )
    demand_df.to_csv(OUT / "demand_plan_components.csv", index=False, encoding="utf-8-sig")

    print("\n[完了] 出力ファイル:")
    print(f"  - {OUT / 'purchase_orders.csv'} (修正)")
    print(f"  - {OUT / 'macnica_free_inventory.csv'}")
    print(f"  - {OUT / 'demand_plan_components.csv'}")


if __name__ == "__main__":
    main()
