"""
🚨 緊急調達シミュレーター
==========================
業務シーン: 営業FCSTには無かった突発需要が発生したとき。

このページの役割:
  - 部材ID / 数量 / 希望納期 を手動入力
  - 既存4ルート評価ロジックと同じアルゴリズムで即座にシミュレーション
  - 「マクニカ営業に相談」ボタン: 評価結果を整形して表示（実運用ではCS連携想定）
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.route_comparison import ROUTE_META, render_route_comparison
from components.inventory_badge import render_inventory_legend
from services.config import get_as_of_date
from services.database import (
    get_silver_components,
    get_silver_inventory_current,
    get_silver_macnica_free_inventory,
    get_silver_purchase_orders,
)

st.set_page_config(page_title="緊急調達シミュレーター | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="FCSTにない突発需要を即座に4ルート評価")

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    components = get_silver_components()
    cust_inv   = get_silver_inventory_current()
    free_inv   = get_silver_macnica_free_inventory()
    pos        = get_silver_purchase_orders()

today = get_as_of_date()
MACNICA_TRANSIT_DAYS = 3  # マクニカ→顧客の輸送日数

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 🚨 緊急調達シミュレーター")
st.caption(
    "FCSTから漏れた突発的な需要を仮想入力し、4つの調達ルートで即座にシミュレーションします。"
)
render_inventory_legend()
st.markdown("")

# ────────────────────────────────────────────────────────
# 入力フォーム
# ────────────────────────────────────────────────────────
st.markdown("### 📝 入力")

if components.empty:
    st.warning("部材マスタが空です。Lakeflow パイプラインを実行してください。")
    st.stop()

with st.form("emergency_form"):
    fc1, fc2, fc3 = st.columns([3, 1, 1])

    with fc1:
        # 部材選択肢: 「品番｜部材名」
        comp_opts = components.copy()
        comp_opts["_label"] = (
            comp_opts["component_id"].astype(str)
            + "  ｜  " + comp_opts["part_number"].astype(str)
            + "  ｜  " + comp_opts["component_name"].astype(str)
        )
        sel_comp = st.selectbox("部材を選択", comp_opts["_label"].tolist())

    with fc2:
        req_qty = st.number_input("必要数量", min_value=1, max_value=100000, value=150, step=10)

    with fc3:
        req_date = st.date_input(
            "希望納期",
            value=today + timedelta(days=14),
            min_value=today,
            max_value=today + timedelta(days=365),
        )

    submit = st.form_submit_button("🔬 4ルート評価を実行", use_container_width=True)

# ────────────────────────────────────────────────────────
# シミュレーション実行
# ────────────────────────────────────────────────────────
if submit:
    sel_comp_id = sel_comp.split("  ｜  ", 1)[0].strip()
    sel_comp_row = components[components["component_id"] == sel_comp_id].iloc[0]
    base_lt_weeks = int(pd.to_numeric(sel_comp_row.get("base_lead_time_weeks", 20), errors="coerce") or 20)

    # ① 顧客在庫
    cust_qty = int(
        pd.to_numeric(
            cust_inv[cust_inv["component_id"] == sel_comp_id]["stock_qty"], errors="coerce"
        ).fillna(0).sum()
    ) if not cust_inv.empty else 0

    # ② マクニカフリー在庫
    free_qty = int(
        pd.to_numeric(
            free_inv[free_inv["component_id"] == sel_comp_id]["qty_available"], errors="coerce"
        ).fillna(0).sum()
    ) if not free_inv.empty else 0

    # ③ 既存PO
    po_sub = pos[pos["component_id"] == sel_comp_id].copy() if not pos.empty else pd.DataFrame()
    if not po_sub.empty:
        po_sub["outstanding_qty"] = pd.to_numeric(po_sub["outstanding_qty"], errors="coerce").fillna(0)
        po_sub["expected_delivery_date"] = pd.to_datetime(po_sub["expected_delivery_date"], errors="coerce").dt.date
        po_active = po_sub[(po_sub["outstanding_qty"] > 0) & (po_sub["expected_delivery_date"] >= today)]
        po_qty = int(po_active["outstanding_qty"].sum())
        po_eta = po_active["expected_delivery_date"].min() if not po_active.empty else None
        po_any_delayed = bool(po_active.get("is_delayed", pd.Series(dtype=bool)).any()) if not po_active.empty else False
    else:
        po_qty = 0
        po_eta = None
        po_any_delayed = False

    # ④ 新規発注
    new_eta = today + timedelta(days=base_lt_weeks * 7)

    # 4ルートを options 形式の DataFrame に組み立て
    rows = [
        {
            "route_type":     "CUSTOMER_STOCK",
            "available_qty":  cust_qty,
            "eta_date":       today,
            "confidence":     "確実",
            "note":           "顧客側倉庫の引当可能在庫",
        },
        {
            "route_type":     "MACNICA_FREE",
            "available_qty":  free_qty,
            "eta_date":       today + timedelta(days=MACNICA_TRANSIT_DAYS),
            "confidence":     "確実",
            "note":           "マクニカが顧客向け引当済の在庫から出荷",
        },
        {
            "route_type":     "EXISTING_PO",
            "available_qty":  po_qty,
            "eta_date":       po_eta if po_eta else today + timedelta(days=base_lt_weeks * 7),
            "confidence":     "要相談" if po_any_delayed else ("見込み" if po_qty > 0 else "見込み"),
            "note":           ("既存発注に遅延あり、メーカー納期確認要" if po_any_delayed else "既存発注残BLからの催促"),
        },
        {
            "route_type":     "NEW_ORDER",
            "available_qty":  req_qty,  # 数量上限なしと仮定
            "eta_date":       new_eta,
            "confidence":     "要相談",
            "note":           f"新規追加発注 (LT {base_lt_weeks}週、メーカー納期確認要)",
        },
    ]
    sim_df = pd.DataFrame(rows)
    sim_df["requested_qty"] = req_qty
    sim_df["requested_date"] = req_date
    sim_df["shortage_qty"] = (sim_df["requested_qty"] - sim_df["available_qty"]).clip(lower=0)
    sim_df["is_in_time"] = sim_df["eta_date"].apply(lambda d: d <= req_date)
    sim_df["days_late"] = sim_df.apply(
        lambda r: max(0, (r["eta_date"] - r["requested_date"]).days), axis=1
    )

    st.markdown("---")
    st.markdown("### 🔬 4ルート評価結果")
    render_route_comparison(sim_df, requested_qty=int(req_qty), requested_date=req_date)

    # ────────────────────────────────────────────────────
    # 推奨アクション提示（自動推奨せず、選択肢を整理）
    # ────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 💡 検討材料")

    # 各ルートの達成度（充足×納期）を整理
    summary = []
    for _, r in sim_df.iterrows():
        meta = ROUTE_META.get(r["route_type"], {})
        label = meta.get("label_jp", r["route_type"])
        if r["shortage_qty"] <= 0 and r["is_in_time"]:
            verdict = "✅ 単独で完全充足可能"
        elif r["shortage_qty"] <= 0:
            verdict = f"⏰ 数量は確保可だが {r['days_late']} 日遅延"
        elif r["is_in_time"]:
            verdict = f"📉 納期は間に合うが {r['shortage_qty']:,} 個不足"
        else:
            verdict = f"❌ 不足 {r['shortage_qty']:,} 個 + {r['days_late']} 日遅延"
        summary.append({"ルート": label, "判定": verdict, "ETA": r["eta_date"].isoformat()})

    st.dataframe(pd.DataFrame(summary), hide_index=True, use_container_width=True)

    # 組み合わせ提案 (合計でカバー可能か)
    total_avail_today = cust_qty + free_qty
    if total_avail_today >= req_qty:
        st.success(
            f"💡 顧客在庫({cust_qty:,}) + マクニカフリー在庫({free_qty:,}) = {total_avail_today:,} 個 で"
            f" 必要数 {req_qty:,} 個を **即時充足可能** です。"
        )
    elif total_avail_today + po_qty >= req_qty:
        st.info(
            f"💡 顧客在庫 + マクニカフリー + 既存発注催促の合計 {total_avail_today + po_qty:,} 個 で"
            f" 必要数 {req_qty:,} 個を充足可能。既存発注の入荷タイミングを要確認。"
        )
    else:
        gap = req_qty - (total_avail_today + po_qty)
        st.error(
            f"⚠️ 既存3ルート合計でも {gap:,} 個不足。新規追加発注 (LT {base_lt_weeks}週) が必要です。"
        )

    # ────────────────────────────────────────────────────
    # マクニカ営業相談用テキスト
    # ────────────────────────────────────────────────────
    st.markdown("---")
    with st.expander("📧 マクニカ営業への相談用テキストを生成"):
        cs_text = f"""【調達相談】
■ 部材: {sel_comp_row['component_name']} ({sel_comp_row['part_number']})
■ 必要数量: {req_qty:,} 個
■ 希望納期: {req_date.isoformat()}
■ 現状把握:
  - 顧客在庫: {cust_qty:,} 個 (即時)
  - マクニカフリー在庫: {free_qty:,} 個 (今日+{MACNICA_TRANSIT_DAYS}日)
  - 既存発注残BL: {po_qty:,} 個 ({'要催促' if po_any_delayed else 'ETA ' + (po_eta.isoformat() if po_eta else '—')})
  - 新規発注LT: {base_lt_weeks} 週

ご相談したい内容: 上記不足分の調達方針について、最適な選択肢のご提案をお願いします。"""
        st.code(cs_text, language="text")
