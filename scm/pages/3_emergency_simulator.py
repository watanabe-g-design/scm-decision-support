"""
🚨 緊急調達シミュレーター
==========================
業務シーン: 営業FCSTには無かった突発需要が発生したとき。

このページの役割:
  - 部材ID / 数量 / 希望納期 を手動入力
  - 既存4ルート評価ロジックと同じアルゴリズムで即座にシミュレーション
  - 「マクニカ営業に相談」ボタン: 評価結果を整形して表示
  - 影響製品表示: BOMから「この部材が止まるとどの製品が止まるか」を可視化
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
from components.route_comparison import ROUTE_META, render_route_comparison, render_route_legend
from components.inventory_badge import render_inventory_legend
from components.drill_down import pop_drill_filter
from services.config import get_as_of_date
from services.recommendation import (
    generate_action_options, estimate_pull_in_qty_from_next_month,
)
from services.database import (
    get_silver_components,
    get_silver_inventory_current,
    get_silver_macnica_free_inventory,
    get_silver_purchase_orders,
    get_silver_bom,
    get_silver_products,
    get_silver_demand_plan_components,
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
    bom        = get_silver_bom()
    products   = get_silver_products()
    demand_all = get_silver_demand_plan_components()

today = get_as_of_date()
MACNICA_TRANSIT_DAYS = 3

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 🚨 緊急調達シミュレーター")
st.caption("FCSTから漏れた突発的な需要を仮想入力し、4つの調達ルートで即座にシミュレーションします。")

render_route_legend()
render_inventory_legend()
st.markdown("")

# ────────────────────────────────────────────────────────
# 入力フォーム (2段階選択)
# ────────────────────────────────────────────────────────
st.markdown("### 📝 入力")

if components.empty:
    st.warning("部材マスタが空です。Lakeflow パイプラインを実行してください。")
    st.stop()

with st.form("emergency_form"):
    # 部材選択 (Phase 7: カテゴリ二段選択を廃止、直接検索で部材を選ぶ)
    comp_show = components.copy()
    comp_show["_label"] = (
        comp_show["component_id"].astype(str)
        + "  ｜  " + comp_show["part_number"].astype(str)
        + "  ｜  " + comp_show["component_name"].astype(str)
    )
    comp_show = comp_show.sort_values("_label")
    sel_comp = st.selectbox(
        f"🔧 部材を選択（{len(comp_show)} 件、品番・部材名で検索可能）",
        comp_show["_label"].tolist(),
        help="ドロップダウン内の検索ボックスに品番や部材名を入力して絞り込めます",
    )

    fc3, fc4 = st.columns(2)
    with fc3:
        req_qty = st.number_input(
            "📦 必要数量（突発的に必要となった部材数）",
            min_value=1, max_value=100000, value=150, step=10,
        )
    with fc4:
        req_date = st.date_input(
            "⏰ 希望納期（この日までに入手したい）",
            value=today + timedelta(days=14),
            min_value=today,
            max_value=today + timedelta(days=365),
        )

    submit = st.form_submit_button("🔬 4ルート評価＋具体アクション案を生成", use_container_width=True)

# ────────────────────────────────────────────────────────
# シミュレーション実行
# ────────────────────────────────────────────────────────
if submit:
    sel_comp_id = sel_comp.split("  ｜  ", 1)[0].strip()
    sel_comp_row = components[components["component_id"] == sel_comp_id].iloc[0]
    base_lt_weeks = int(pd.to_numeric(sel_comp_row.get("base_lead_time_weeks", 20), errors="coerce") or 20)

    # ① 顧客在庫 (今後の消費を考慮した実効在庫)
    cust_total = int(pd.to_numeric(
        cust_inv[cust_inv["component_id"] == sel_comp_id]["stock_qty"], errors="coerce"
    ).fillna(0).sum()) if not cust_inv.empty else 0

    # 今後の消費見込: 同部材で req_date より前の他需要の合計
    if not demand_all.empty:
        future_demands = demand_all.copy()
        future_demands["requested_date"] = pd.to_datetime(future_demands["requested_date"], errors="coerce").dt.date
        consumption = int(pd.to_numeric(
            future_demands[
                (future_demands["component_id"] == sel_comp_id)
                & (future_demands["requested_date"] <= req_date)
                & (future_demands["requested_date"] >= today)
            ]["requested_qty"],
            errors="coerce",
        ).fillna(0).sum())
    else:
        consumption = 0

    cust_qty_effective = max(0, cust_total - consumption)

    # ② マクニカフリー在庫
    free_qty = int(pd.to_numeric(
        free_inv[free_inv["component_id"] == sel_comp_id]["qty_available"], errors="coerce"
    ).fillna(0).sum()) if not free_inv.empty else 0

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

    # ④ 新規発注 (部材ごとのLT)
    new_eta = today + timedelta(days=base_lt_weeks * 7)

    # 4ルートDataFrame構築
    rows = [
        {"route_type": "CUSTOMER_STOCK", "available_qty": cust_qty_effective, "eta_date": today,
         "confidence": "確実",
         "note": f"現在庫 {cust_total} - 期間内予定消費 {consumption} = 実効 {cust_qty_effective}"},
        {"route_type": "MACNICA_FREE",   "available_qty": free_qty,
         "eta_date": today + timedelta(days=MACNICA_TRANSIT_DAYS), "confidence": "確実",
         "note": "マクニカが顧客向け引当済の在庫から出荷"},
        {"route_type": "EXISTING_PO",    "available_qty": po_qty,
         "eta_date": po_eta if po_eta else today + timedelta(days=base_lt_weeks * 7),
         "confidence": "要相談" if po_any_delayed else "見込み",
         "note": ("既存発注に遅延あり、メーカー納期確認要" if po_any_delayed else "既存発注残BLからの催促")},
        {"route_type": "NEW_ORDER",      "available_qty": req_qty,
         "eta_date": new_eta, "confidence": "要相談",
         "note": f"新規追加発注 (LT {base_lt_weeks}週、メーカー納期確認要)"},
    ]
    sim_df = pd.DataFrame(rows)
    sim_df["requested_qty"] = req_qty
    sim_df["requested_date"] = req_date
    sim_df["shortage_qty"] = (sim_df["requested_qty"] - sim_df["available_qty"]).clip(lower=0)
    sim_df["is_in_time"] = sim_df["eta_date"].apply(lambda d: d <= req_date)
    sim_df["days_late"] = sim_df.apply(lambda r: max(0, (r["eta_date"] - r["requested_date"]).days), axis=1)

    st.markdown("---")
    st.markdown("### 🔬 4ルート評価結果")
    render_route_comparison(sim_df, requested_qty=int(req_qty), requested_date=req_date)

    # ────────────────────────────────────────────
    # 💡 具体アクション案（複数選択肢併記）
    # ────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 💡 具体アクション案（複数の実行可能な選択肢）")
    st.caption("各案には具体的な手順・数量・完了日が明記されています。比較して判断してください。")

    pull_in = estimate_pull_in_qty_from_next_month(
        component_id=sel_comp_id,
        requested_date=req_date,
        demand_df=demand_all,
        today=today,
    )
    action_opts = generate_action_options(
        routes_df=sim_df,
        requested_qty=int(req_qty),
        requested_date=req_date,
        today=today,
        other_month_pull_in_qty=pull_in,
        component_lt_weeks=base_lt_weeks,
    )

    for i, opt_a in enumerate(action_opts[:6], start=1):
        st.markdown(
            f"**{opt_a.title}** ｜ 確実度: `{opt_a.feasibility}` ｜ 充足: {opt_a.coverage_qty:,}個"
            + (f" / 未充足: {opt_a.gap_qty:,}個" if opt_a.gap_qty else "")
            + f" ｜ 完了日: **{opt_a.eta_date.isoformat()}**"
        )
        for step in opt_a.steps:
            st.markdown(f"&emsp;• {step}", unsafe_allow_html=True)
        st.markdown("")

    # 影響製品 (BOMから)
    st.markdown("---")
    st.markdown("### 🏭 この部材を使用している製品（影響範囲）")
    if not bom.empty:
        affected_bom = bom[bom["component_id"] == sel_comp_id]
        if not affected_bom.empty and not products.empty:
            affected = affected_bom.merge(products[["product_id", "product_name", "product_category"]], on="product_id", how="left")
            affected = affected[["product_id", "product_name", "product_category", "quantity_per_unit"]].rename(columns={
                "product_id": "製品ID",
                "product_name": "製品名",
                "product_category": "製品カテゴリ",
                "quantity_per_unit": "1製品あたり使用数",
            })
            st.dataframe(affected, hide_index=True, use_container_width=True)
            st.caption(f"💡 この部材の調達遅延は上記 {len(affected)} 製品の生産に影響します。")
        else:
            st.info("この部材を使用している製品はBOM上見つかりません。")

    # 検討材料
    st.markdown("---")
    st.markdown("### 💡 検討材料")
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

    # 在庫消費アラート
    if cust_total > 0 and consumption > 0:
        if cust_qty_effective < req_qty:
            st.warning(
                f"⚠️ 顧客在庫を本需要に{req_qty}個使うと、期間内の他需要 {consumption}個 を賄えない可能性があります。"
                f"現在庫 {cust_total} → 期間内予定消費差し引き後の実効在庫は {cust_qty_effective}個 のみ。"
            )
        else:
            st.info(
                f"💡 顧客在庫からこの需要({req_qty}個)を充当しても、期間内他需要({consumption}個)を含めて在庫余裕あり。"
            )

    # 組み合わせ判定
    total_certain = cust_qty_effective + free_qty
    if total_certain >= req_qty:
        st.success(
            f"💡 顧客実効在庫({cust_qty_effective:,}) + マクニカフリー({free_qty:,}) = {total_certain:,} 個 で"
            f" 必要数 {req_qty:,} 個を **即時充足可能** です。"
        )
    elif total_certain + po_qty >= req_qty:
        st.info(
            f"💡 確実2ルート + 既存PO催促の合計 {total_certain + po_qty:,} 個 で必要数を充足可能。"
            f" 既存POの入荷タイミング ({po_eta if po_eta else '—'}) を要確認。"
        )
    else:
        gap = req_qty - (total_certain + po_qty)
        st.error(
            f"⚠️ 既存3ルート合計でも {gap:,} 個不足。新規追加発注 (LT {base_lt_weeks}週) が必要です。"
        )

    # マクニカ相談用テキスト
    st.markdown("---")
    with st.expander("📧 マクニカ営業への相談用テキストを生成"):
        cs_text = f"""【調達相談】
■ 部材: {sel_comp_row['component_name']} ({sel_comp_row['part_number']})
■ 必要数量: {req_qty:,} 個
■ 希望納期: {req_date.isoformat()}
■ 現状把握:
  - 顧客在庫(実効): {cust_qty_effective:,} 個 (現在庫 {cust_total} - 期間内予定消費 {consumption})
  - マクニカフリー在庫: {free_qty:,} 個 (今日+{MACNICA_TRANSIT_DAYS}日)
  - 既存発注残BL: {po_qty:,} 個 ({'要催促' if po_any_delayed else 'ETA ' + (po_eta.isoformat() if po_eta else '—')})
  - 新規発注LT: {base_lt_weeks} 週

ご相談したい内容: 上記不足分の調達方針について、最適な選択肢のご提案をお願いします。"""
        st.code(cs_text, language="text")
