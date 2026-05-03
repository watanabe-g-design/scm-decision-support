"""
SCM Demo - 新規テーブル生成スクリプト
======================================
本デモAppの大幅再設計にともない、以下2本のSilverテーブルのソースCSVを生成する:

  1. macnica_free_inventory.csv
     - マクニカが顧客向けに引当済みの「フリー在庫」
     - 顧客側在庫 (inventory_current.csv) と区別する
     - 今回は顧客 CUS001 (ネクサス精機株式会社) 向けに固定

  2. demand_plan_components.csv
     - 部材レベルの希望納期データ
     - source_type = "FCST_AUTO":      製品FCST × BOM展開で自動生成
     - source_type = "EMERGENCY_MANUAL": 緊急の手動入力 (シミュレーション用)

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


# ════════════════════════════════════════════════════════════════
# 1. マクニカフリー在庫
# ════════════════════════════════════════════════════════════════
def gen_macnica_free_inventory(components_df: pd.DataFrame, warehouses_df: pd.DataFrame) -> pd.DataFrame:
    """
    マクニカが顧客 CUS001 向けに引当済みのフリー在庫を生成。

    設計方針:
      - 全90部材中、約35部材のみ引当 (主要部材に絞る)
      - 1部材あたり1〜2倉庫に保管 (マクニカ倉庫 = 既存warehousesを流用)
      - 数量は控えめ (顧客側在庫より少なめ): 30〜800個程度
      - 高価格部材は数量を少なく、低価格部材は数量を多く
      - 引当の有効期限なし (allocated_to に顧客IDを記録するのみ)
    """
    rows = []
    free_id = 1

    # 対象部材を選定: 90部材から35部材をランダム抽出
    target_components = components_df.sample(n=35, random_state=SEED).copy()

    # マクニカ側の主要倉庫: 上位5倉庫 (浦和・横浜・名古屋・大阪・神戸)
    macnica_warehouses = warehouses_df["warehouse_id"].head(5).tolist()

    for _, comp in target_components.iterrows():
        # 1部材あたり1〜2倉庫に分散
        n_wh = rng.choices([1, 2], weights=[0.7, 0.3])[0]
        selected_whs = rng.sample(macnica_warehouses, k=n_wh)

        # 数量: 単価が高いほど少なめ
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
# 2. 需要計画 (部材レベル)
# ════════════════════════════════════════════════════════════════
def gen_demand_plan_components(
    forecasts_df: pd.DataFrame,
    bom_df: pd.DataFrame,
    products_df: pd.DataFrame,
    components_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    部材レベルの希望納期データを生成。

    2系統:
      A. FCST_AUTO:        対象顧客の製品FCST × BOM展開
      B. EMERGENCY_MANUAL: 突発的に発生した緊急手配 (TODAY基準で前後数週間)
    """
    rows = []
    demand_id = 1

    # ── A. FCST_AUTO: 製品FCSTから自動展開 ──
    target_fcst = forecasts_df[forecasts_df["customer_id"] == TARGET_CUSTOMER_ID].copy()
    if not target_fcst.empty:
        # 日付列に複数フォーマットが混在しているため format='mixed' で吸収
        target_fcst["forecast_month"] = pd.to_datetime(
            target_fcst["forecast_month"], format="mixed", errors="coerce"
        ).dt.date

        # 直近6ヶ月分のFCSTのみ展開 (過去すぎるFCSTは需要計画に不要)
        cutoff_past = TODAY - timedelta(days=30)
        cutoff_future = TODAY + timedelta(days=180)
        active_fcst = target_fcst[
            (target_fcst["forecast_month"] >= cutoff_past)
            & (target_fcst["forecast_month"] <= cutoff_future)
        ]

        for _, fc in active_fcst.iterrows():
            product_id = fc["product_id"]
            fcst_qty = int(fc["forecast_qty"])
            req_date = fc["forecast_month"]  # 月初を希望納期とする

            # BOM展開: この製品に必要な部材リスト
            bom_for_product = bom_df[bom_df["product_id"] == product_id]
            for _, bom_row in bom_for_product.iterrows():
                comp_id = bom_row["component_id"]
                qty_per_unit = int(bom_row.get("quantity_per_unit", 1) or 1)
                required_qty = fcst_qty * qty_per_unit

                rows.append({
                    "demand_id":      f"DM{demand_id:07d}",
                    "component_id":   comp_id,
                    "requested_date": req_date.isoformat(),
                    "requested_qty":  required_qty,
                    "source_type":    "FCST_AUTO",
                    "product_id":     product_id,
                    "customer_id":    TARGET_CUSTOMER_ID,
                    "created_at":     (TODAY - timedelta(days=rng.randint(7, 30))).isoformat(),
                    "note":           "製品FCSTから自動展開",
                })
                demand_id += 1

    # ── B. EMERGENCY_MANUAL: 突発的な緊急手配 (8件サンプル) ──
    # 現実的なシナリオ: FCSTから漏れた・追加発注が必要になった部材
    emergency_samples = components_df.sample(n=8, random_state=SEED + 1)
    emergency_notes = [
        "親会社からの急な増産指示",
        "競合製品の不具合により受注急増",
        "既存ロットで品質問題、再手配",
        "新規プロジェクトの試作分",
        "客先からの緊急仕様変更",
        "在庫見込み誤差による補填",
        "代替部材調達失敗による手配",
        "量産立上げ前の追加検証用",
    ]
    for i, (_, comp) in enumerate(emergency_samples.iterrows()):
        # 希望納期は今日から +3 〜 +30日 (緊急)
        days_ahead = rng.randint(3, 30)
        req_date = TODAY + timedelta(days=days_ahead)
        qty = rng.choice([20, 50, 80, 100, 150, 200, 300])

        rows.append({
            "demand_id":      f"DM{demand_id:07d}",
            "component_id":   comp["component_id"],
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
    print(f"  → demand_plan_components: {len(df)} 行 (FCST_AUTO={auto_n}, EMERGENCY_MANUAL={emerg_n})")
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

    free_inv_df = gen_macnica_free_inventory(components_df, warehouses_df)
    demand_df   = gen_demand_plan_components(forecasts_df, bom_df, products_df, components_df)

    free_inv_df.to_csv(OUT / "macnica_free_inventory.csv", index=False, encoding="utf-8-sig")
    demand_df.to_csv(OUT / "demand_plan_components.csv",   index=False, encoding="utf-8-sig")

    print("\n[完了] 出力ファイル:")
    print(f"  - {OUT / 'macnica_free_inventory.csv'}")
    print(f"  - {OUT / 'demand_plan_components.csv'}")


if __name__ == "__main__":
    main()
