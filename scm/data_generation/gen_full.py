"""
SCM Demo - 完全データ再生成スクリプト
3層サプライチェーン (半導体メーカー → 商社 → 部品メーカー) を反映

新規テーブル:
  - sales_orders.csv    (受注残: 顧客から受注済み・未出荷)
  - purchase_orders.csv (発注残: 商社がメーカーへ発注済み・未入荷)
  - warehouse_components.csv (倉庫×部品の割当マッピング)

修正:
  - 倉庫と部品の紐付けを現実的に (全倉庫に全部品ではなく、地域分散)
  - 在庫は商社在庫として整理
  - アラート計算: FCST + 受注残 + 発注残 + 在庫 を統合
"""
import random
import math
import pandas as pd
import numpy as np
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path

SEED = 42
rng = random.Random(SEED)
np.random.seed(SEED)

OUT = Path(__file__).parent.parent / "sample_data"
OUT.mkdir(exist_ok=True)

TODAY = date(2026, 3, 28)
START_DATE = date(2023, 1, 1)
END_DATE = date(2026, 3, 1)

# ── マスターデータ読み込み ──
def load_masters():
    sups = pd.read_csv(OUT / "suppliers.csv")
    cus  = pd.read_csv(OUT / "customers.csv")
    prods = pd.read_csv(OUT / "products.csv")
    comps = pd.read_csv(OUT / "components.csv")
    whs  = pd.read_csv(OUT / "warehouses.csv")
    bom  = pd.read_csv(OUT / "bom.csv")
    return sups, cus, prods, comps, whs, bom

# ────────────────────────────────────────
# 1. 倉庫×部品 割当マッピング
#    各部品は2〜4倉庫にのみ在庫を持つ (地域分散)
# ────────────────────────────────────────
def gen_warehouse_components(comps, whs):
    """部品ごとに主倉庫 + 副倉庫を割り当て"""
    # 地域別倉庫グループ
    regions = {
        "中部": ["WH003", "WH010"],       # 名古屋DC, 浜松DC (自動車産業集積)
        "関東": ["WH001", "WH002"],       # 東京DC, 横浜DC
        "関西": ["WH004", "WH005"],       # 大阪DC, 神戸DC
        "その他": ["WH006", "WH007", "WH008", "WH009"],  # 福岡,仙台,札幌,広島
    }

    rows = []
    for _, comp in comps.iterrows():
        cid = comp["component_id"]
        # 主要倉庫: 中部から1〜2拠点 (自動車産業向け)
        primary = rng.sample(regions["中部"], k=rng.choice([1, 2]))
        # 副倉庫: 関東・関西から1拠点
        secondary = [rng.choice(regions["関東"]), rng.choice(regions["関西"])]
        # その他地域にも必ず1拠点配置 (空倉庫を防ぐ)
        secondary.append(rng.choice(regions["その他"]))

        assigned = list(set(primary + secondary))
        for wid in assigned:
            is_primary = wid in primary
            rows.append({
                "component_id": cid,
                "warehouse_id": wid,
                "is_primary": is_primary,
                "allocation_pct": 0.4 if is_primary else round(0.6 / (len(assigned) - len(primary)), 2),
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUT / "warehouse_components.csv", index=False, encoding="utf-8-sig")
    print(f"  warehouse_components.csv: {len(df)} rows ({df['component_id'].nunique()} parts × avg {len(df)/df['component_id'].nunique():.1f} warehouses)")
    return df


# ────────────────────────────────────────
# 2. 在庫再生成 (倉庫割当ベース)
# ────────────────────────────────────────
def gen_inventory_current(comps, wc_df, bom, forecasts, so_df=None, po_df=None):
    """
    商社在庫 (inventory_current) を倉庫割当ベースで生成
    受注残・発注残を考慮して effective_stock ベースで優先度が適切に分散するよう調整
    """
    fc = forecasts.copy()
    fc["forecast_month"] = pd.to_datetime(fc["forecast_month"], format="mixed")
    month_start = pd.Timestamp("2026-03-01")
    fc_future = fc[(fc["forecast_month"] >= month_start) &
                   (fc["forecast_month"] <= month_start + pd.DateOffset(months=3))]
    demand = (fc_future.merge(bom, on="product_id")
              .assign(d=lambda x: x["forecast_qty"] * x["quantity_per_unit"])
              .groupby("component_id", as_index=False)["d"].sum()
              .rename(columns={"d": "demand_3m"}))

    comps_info = comps.merge(demand, on="component_id", how="left")
    comps_info["demand_3m"] = comps_info["demand_3m"].fillna(0)
    comps_info["daily_demand"] = comps_info["demand_3m"] / 90.0

    # 受注残・発注残をコンポーネント別に集計
    so_backlog = {}
    if so_df is not None:
        so_agg = (so_df[so_df["status"].isin(["confirmed","picking","backorder"])]
                  .groupby("component_id")["component_required_qty"].sum())
        so_backlog = so_agg.to_dict()

    po_incoming = {}
    if po_df is not None:
        po_agg = (po_df[po_df["status"].isin(["placed","acknowledged","in_production","shipped","partial_received"])]
                  .groupby("component_id")["outstanding_qty"].sum())
        po_incoming = po_agg.to_dict()

    lt_df = pd.read_csv(OUT / "lead_times.csv")
    lt_latest = lt_df.sort_values("effective_date").groupby("component_id", as_index=False).last()[["component_id","lead_time_weeks"]]
    comps_info = comps_info.merge(lt_latest, on="component_id", how="left")
    comps_info["lead_time_weeks"] = comps_info["lead_time_weeks"].fillna(comps_info["base_lead_time_weeks"])

    comp_ids = comps_info["component_id"].unique()
    np.random.shuffle(comp_ids)

    # 目標: 12 CRITICAL, 10 HIGH, 15 MEDIUM, rest LOW
    critical_set = set(comp_ids[:12])
    high_set = set(comp_ids[12:22])
    medium_set = set(comp_ids[22:37])

    rows = []
    snap_id = 1

    for _, comp in comps_info.iterrows():
        cid = comp["component_id"]
        dd = comp["daily_demand"]
        ss_weeks = comp["safety_stock_weeks"]
        lt_weeks = comp["lead_time_weeks"]

        wc_rows = wc_df[wc_df["component_id"] == cid]
        if len(wc_rows) == 0:
            continue

        # effective_stock = stock + po_incoming - so_backlog
        # target: effective_stock - safety = target_days * daily_demand
        # => stock = target_days * dd + safety + so_backlog - po_incoming
        so_bl = so_backlog.get(cid, 0)
        po_in = po_incoming.get(cid, 0)

        if cid in critical_set:
            target_days = rng.uniform(-20, 5)
        elif cid in high_set:
            target_days = rng.uniform(8, 13)
        elif cid in medium_set:
            target_days = rng.uniform(16, 28)
        else:
            target_days = rng.uniform(50, 200)

        safety_total = dd * 7 * ss_weeks
        # stock = (target_days + lt_days) * dd + safety + so_backlog - po_incoming
        total_target = max(0, int((target_days + lt_weeks * 7) * dd + safety_total + so_bl - po_in))

        for _, wc in wc_rows.iterrows():
            wid = wc["warehouse_id"]
            alloc = wc["allocation_pct"]
            noise = rng.uniform(0.7, 1.3)
            stock = max(0, int(total_target * alloc * noise))
            safety = max(0, int(safety_total * alloc))
            wh_demand = max(0, int(dd * 30 * alloc))

            rows.append({
                "snapshot_id": f"INV{snap_id:07d}",
                "component_id": cid,
                "warehouse_id": wid,
                "snapshot_month": "2026-03-01",
                "stock_qty": stock,
                "safety_stock_qty": safety,
                "replenishment_qty": 0,
                "demand_qty": wh_demand,
            })
            snap_id += 1

    df = pd.DataFrame(rows)
    df.to_csv(OUT / "inventory_current.csv", index=False, encoding="utf-8-sig")
    print(f"  inventory_current.csv: {len(df)} rows ({df['component_id'].nunique()} parts)")
    return df


# ────────────────────────────────────────
# 3. 受注残 (Sales Orders: 顧客→商社)
# ────────────────────────────────────────
def gen_sales_orders(prods, comps, bom, cus):
    """
    受注残 = 顧客から注文を受領済みだが、まだ出荷していない注文
    - 注文日は過去30日〜今日
    - 要求納期は今日〜60日後
    - ステータス: confirmed / picking / shipped_partial / backorder
    """
    rows = []
    oid = 1
    statuses = ["confirmed", "confirmed", "confirmed", "picking", "picking",
                "shipped_partial", "backorder", "backorder"]

    for _ in range(120):  # 120件の受注残 (現実的なバランス)
        prod = rng.choice(prods.to_dict("records"))
        cust = cus[cus["customer_id"] == prod["customer_id"]].iloc[0]

        order_date = TODAY - timedelta(days=rng.randint(1, 30))
        requested_date = TODAY + timedelta(days=rng.randint(0, 60))
        qty = rng.randint(5, 150) * 5  # 小ロット化

        status = rng.choice(statuses)
        shipped_qty = 0
        if status == "shipped_partial":
            shipped_qty = int(qty * rng.uniform(0.3, 0.7))
        elif status == "picking":
            shipped_qty = 0

        # BOM展開: この製品に必要な部品
        bom_rows = bom[bom["product_id"] == prod["product_id"]]

        for _, b in bom_rows.iterrows():
            comp_qty = (qty - shipped_qty) * b["quantity_per_unit"]
            rows.append({
                "sales_order_id": f"SO{oid:06d}",
                "customer_id": cust["customer_id"],
                "customer_name": cust["customer_name"],
                "product_id": prod["product_id"],
                "product_name": prod["product_name"],
                "component_id": b["component_id"],
                "order_date": order_date.isoformat(),
                "requested_delivery_date": requested_date.isoformat(),
                "order_qty": qty,
                "shipped_qty": shipped_qty,
                "remaining_qty": qty - shipped_qty,
                "component_required_qty": int(comp_qty),
                "status": status,
                "priority_flag": requested_date <= TODAY + timedelta(days=7),
            })
        oid += 1

    df = pd.DataFrame(rows)
    df.to_csv(OUT / "sales_orders.csv", index=False, encoding="utf-8-sig")
    print(f"  sales_orders.csv: {len(df)} rows ({df['sales_order_id'].nunique()} orders)")
    return df


# ────────────────────────────────────────
# 4. 発注残 (Purchase Orders: 商社→メーカー)
# ────────────────────────────────────────
def gen_purchase_orders(comps, sups):
    """
    発注残 = 商社がメーカーに発注済み・未入荷の注文
    - FCST/過去履歴ベースの先行発注
    - ステータス: placed / acknowledged / in_production / shipped / partial_received
    """
    rows = []
    po_id = 1
    statuses_weights = [
        ("placed", 0.15),
        ("acknowledged", 0.20),
        ("in_production", 0.25),
        ("shipped", 0.20),
        ("partial_received", 0.10),
        ("fully_received", 0.10),
    ]
    statuses = [s for s, _ in statuses_weights]
    weights = [w for _, w in statuses_weights]

    for _, comp in comps.iterrows():
        cid = comp["component_id"]
        sid = comp["supplier_id"]
        moq = comp["min_order_qty"]
        lt_weeks = comp["base_lead_time_weeks"]

        # 各部品に2〜5件の発注残
        n_orders = rng.randint(2, 5)
        for _ in range(n_orders):
            po_date = TODAY - timedelta(days=rng.randint(7, lt_weeks * 7 + 30))
            expected_delivery = po_date + timedelta(weeks=lt_weeks + rng.randint(-2, 4))
            qty = rng.randint(1, 6) * moq

            status = rng.choices(statuses, weights=weights, k=1)[0]
            received_qty = 0
            if status == "fully_received":
                received_qty = qty
            elif status == "partial_received":
                received_qty = int(qty * rng.uniform(0.3, 0.7))

            # 遅延判定
            is_delayed = False
            delay_days = 0
            if status not in ("fully_received",) and expected_delivery < TODAY:
                is_delayed = rng.random() < 0.3
                if is_delayed:
                    delay_days = (TODAY - expected_delivery).days + rng.randint(0, 14)

            rows.append({
                "purchase_order_id": f"PO{po_id:06d}",
                "component_id": cid,
                "supplier_id": sid,
                "order_date": po_date.isoformat(),
                "expected_delivery_date": expected_delivery.isoformat(),
                "quantity": qty,
                "received_qty": received_qty,
                "outstanding_qty": qty - received_qty,
                "status": status,
                "is_delayed": is_delayed,
                "delay_days": delay_days,
                "unit_cost_usd": comp["unit_price_usd"],
                "total_cost_usd": round(qty * comp["unit_price_usd"], 2),
            })
            po_id += 1

    df = pd.DataFrame(rows)
    df.to_csv(OUT / "purchase_orders.csv", index=False, encoding="utf-8-sig")
    print(f"  purchase_orders.csv: {len(df)} rows ({df['purchase_order_id'].nunique()} orders)")
    return df


# ────────────────────────────────────────
# 5. 物流フロー (配送ルート: メーカー→倉庫→顧客)
# ────────────────────────────────────────
def gen_shipment_routes(sups, whs, cus, comps):
    """
    配送ルートデータ (地図上のフロー表示用)
    メーカー出荷地 → 商社倉庫 → 顧客工場
    """
    # メーカーの出荷地 (日本国内の主要港/空港)
    maker_locations = {
        "SUP001": {"name": "Nextera 成田空港経由",     "lat": 35.7720, "lon": 140.3929},
        "SUP002": {"name": "Sakura 武蔵工場",          "lat": 35.7796, "lon": 139.3441},
        "SUP003": {"name": "Pacific 成田空港経由",      "lat": 35.7720, "lon": 140.3929},
        "SUP004": {"name": "Alpine 関西空港経由",       "lat": 34.4347, "lon": 135.2441},
        "SUP005": {"name": "Vertex 成田空港経由",       "lat": 35.7720, "lon": 140.3929},
        "SUP006": {"name": "Crossfield 成田空港経由",   "lat": 35.7720, "lon": 140.3929},
        "SUP007": {"name": "Hikari 京都工場",           "lat": 34.9692, "lon": 135.7556},
        "SUP008": {"name": "Fuji 川崎工場",             "lat": 35.5308, "lon": 139.7030},
        "SUP009": {"name": "Pinnacle 成田空港経由",     "lat": 35.7720, "lon": 140.3929},
        "SUP010": {"name": "Bridgelink 川崎倉庫",       "lat": 35.5308, "lon": 139.7030},
    }

    # 顧客工場所在地
    customer_locations = {
        "CUS001": {"name": "ネクサス精機 安城工場",    "lat": 34.9516, "lon": 137.0806},
        "CUS002": {"name": "テクノワイズ 刈谷工場",    "lat": 34.9891, "lon": 137.0011},
        "CUS003": {"name": "ゼニス 門真工場",          "lat": 34.7394, "lon": 135.5944},
        "CUS004": {"name": "ハーネステック 牧之原工場", "lat": 34.7322, "lon": 138.2218},
        "CUS005": {"name": "ユニオン電装 四日市工場",  "lat": 34.9661, "lon": 136.6243},
    }

    rows = []

    # 各メーカーから対応倉庫へのルート
    for sid, loc in maker_locations.items():
        # このメーカーの部品が置かれている倉庫を特定
        maker_comps = comps[comps["supplier_id"] == sid]["component_id"].unique()
        wc = pd.read_csv(OUT / "warehouse_components.csv") if (OUT / "warehouse_components.csv").exists() else pd.DataFrame()
        if len(wc) > 0:
            dest_whs = wc[wc["component_id"].isin(maker_comps)]["warehouse_id"].unique()
        else:
            dest_whs = ["WH001", "WH003"]

        for wid in dest_whs[:3]:  # 最大3倉庫
            wh = whs[whs["warehouse_id"] == wid].iloc[0]
            rows.append({
                "route_type": "inbound",
                "from_id": sid,
                "from_name": loc["name"],
                "from_lat": loc["lat"],
                "from_lon": loc["lon"],
                "to_id": wid,
                "to_name": wh["warehouse_name"],
                "to_lat": wh["latitude"],
                "to_lon": wh["longitude"],
                "avg_transit_days": rng.randint(1, 7) if "Japan" in (sups[sups["supplier_id"]==sid]["country"].values[0] if sid in sups["supplier_id"].values else "Japan") else rng.randint(3, 14),
                "monthly_shipments": rng.randint(5, 30),
            })

    # 各倉庫から顧客工場へのルート
    for wid in whs["warehouse_id"].unique():
        wh = whs[whs["warehouse_id"] == wid].iloc[0]
        # 近隣の顧客2〜3社
        for cid, cloc in list(customer_locations.items())[:rng.randint(2, 4)]:
            rows.append({
                "route_type": "outbound",
                "from_id": wid,
                "from_name": wh["warehouse_name"],
                "from_lat": wh["latitude"],
                "from_lon": wh["longitude"],
                "to_id": cid,
                "to_name": cloc["name"],
                "to_lat": cloc["lat"],
                "to_lon": cloc["lon"],
                "avg_transit_days": rng.randint(1, 3),
                "monthly_shipments": rng.randint(10, 50),
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUT / "shipment_routes.csv", index=False, encoding="utf-8-sig")
    print(f"  shipment_routes.csv: {len(df)} rows (inbound: {(df['route_type']=='inbound').sum()}, outbound: {(df['route_type']=='outbound').sum()})")
    return df


# ────────────────────────────────────────
# メイン
# ────────────────────────────────────────
def main():
    print("=" * 50)
    print("  Supply Chain Management - データ拡張生成")
    print("=" * 50)

    sups, cus, prods, comps, whs, bom = load_masters()
    fc = pd.read_csv(OUT / "forecasts.csv")

    print("\n[1/5] 倉庫×部品 割当マッピング...")
    wc_df = gen_warehouse_components(comps, whs)

    print("\n[2/5] 受注残 (Sales Orders)...")
    so_df = gen_sales_orders(prods, comps, bom, cus)

    print("\n[3/5] 発注残 (Purchase Orders)...")
    po_df = gen_purchase_orders(comps, sups)

    print("\n[4/5] 在庫データ再生成 (受注残・発注残考慮)...")
    gen_inventory_current(comps, wc_df, bom, fc, so_df, po_df)

    print("\n[5/5] 配送ルート...")
    gen_shipment_routes(sups, whs, cus, comps)

    print("\n" + "=" * 50)
    print("  拡張データ生成完了!")
    print("=" * 50)
    for f in sorted(OUT.glob("*.csv")):
        df = pd.read_csv(f)
        print(f"  {f.name:<30} {len(df):>7,} rows")


if __name__ == "__main__":
    main()
