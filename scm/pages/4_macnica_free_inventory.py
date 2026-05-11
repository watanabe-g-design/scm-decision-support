"""
📦 マクニカフリー在庫モニター
==============================
業務上の役割:
  - マクニカが顧客向けに引当済の在庫を「部材別」に可視化
  - マクニカのフリー在庫は新子安ロジスティクスセンター1拠点に集約
  - 引当済みなので、マクニカ営業に相談すれば即時(今日+輸送日数)で受け取れる

Phase 7 改修:
  ✅ 部材カテゴリフィルター撤廃 → 検索 + 部材セレクタへ
  ✅ 「部材別 引当残量」表に「いつのスナップショットか・何に使うか」キャプションを追加
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.inventory_badge import render_inventory_legend
from components.search_bar import (
    render_search_bar, render_component_selector,
    apply_component_search, apply_component_id_filter,
)
from services.config import get_as_of_date
from services.plot_theme import base_layout, get_theme_tokens
from services.database import (
    get_silver_macnica_free_inventory,
    get_silver_inventory_current,
    get_silver_components,
)

st.set_page_config(page_title="マクニカフリー在庫 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="マクニカが顧客向けに引当済の在庫を部材別に可視化")

t = get_theme_tokens()

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    free_inv   = get_silver_macnica_free_inventory()
    cust_inv   = get_silver_inventory_current()
    components = get_silver_components()

today = get_as_of_date()

st.markdown("## 📦 マクニカフリー在庫モニター")
st.caption(
    "マクニカが貴社向けに引当済の在庫一覧。"
    "**全在庫は新子安ロジスティクスセンターを経由します** (マクニカ拠点は新子安1箇所)。"
    "通常LTを待たずに、相談ベースで引当 → 出荷可能です（マクニカ→貴社の輸送日数のみ）。"
)

render_inventory_legend()
st.info("📍 **マクニカのフリー在庫保管拠点**: 新子安ロジスティクスセンター（神奈川県横浜市鶴見区）のみ")

if free_inv.empty:
    st.warning("マクニカフリー在庫データが空です。")
    st.stop()

# 部材名を結合
free = free_inv.copy()
free["qty_available"] = pd.to_numeric(free["qty_available"], errors="coerce").fillna(0).astype(int)

if not components.empty:
    free = free.merge(
        components[["component_id", "part_number", "component_name", "unit_price_usd", "base_lead_time_weeks"]],
        on="component_id", how="left",
    )

# ────────────────────────────────────────────────────────
# 検索 + 部材セレクタ (Phase 7: カテゴリは廃止)
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター")
fc1, fc2 = st.columns(2)
with fc1:
    search_query = render_search_bar(components, key="free_search")
with fc2:
    selected_ids = render_component_selector(components, key="free_comp_sel")

qty_threshold = st.number_input(
    "📦 最低引当数量フィルター（この値以上の引当のみ表示）",
    min_value=0, value=0, step=10,
    help="少量引当を除外して、ある程度まとまった引当のみを見たいときに使用",
)

if search_query:
    free = apply_component_search(free, search_query)
if selected_ids:
    free = apply_component_id_filter(free, selected_ids)
if qty_threshold > 0:
    free = free[free["qty_available"] >= qty_threshold]

if free.empty:
    st.info("条件に該当する引当がありません。フィルターを調整してください。")
    st.stop()

st.markdown("---")

# ────────────────────────────────────────────────────────
# KPI
# ────────────────────────────────────────────────────────
total_qty = int(free["qty_available"].sum())
n_components = int(free["component_id"].nunique())
total_value_usd = float((free["qty_available"] * pd.to_numeric(free.get("unit_price_usd", 0), errors="coerce").fillna(0)).sum())
high_value_items = int((free["qty_available"] * pd.to_numeric(free.get("unit_price_usd", 0), errors="coerce").fillna(0) > 5000).sum())

k1, k2, k3, k4 = st.columns(4)
k1.metric("📦 引当総数量", f"{total_qty:,} 個")
k2.metric("🔧 引当部材数", f"{n_components} 品目")
k3.metric("💰 在庫評価額（参考）", f"${total_value_usd:,.0f}")
k4.metric("🏷️ 高額引当（>$5000）", f"{high_value_items} 品目")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 部材別 引当数量
# ────────────────────────────────────────────────────────
st.markdown("### 📊 部材別 引当数量（マクニカ→貴社 即時出荷可能）")
st.caption(
    f"📅 **基準日**: {today.isoformat()} 時点の引当残量。"
    "引当数量が大きい部材ほど、突発需要への即応性が高い候補です。"
    "上位の部材は『緊急調達シミュレーター』のシミュレーション対象として優先的に検討してください。"
)

by_comp = (
    free.groupby(["component_id", "part_number", "component_name"], dropna=False, as_index=False)
    .agg(引当数量=("qty_available", "sum"))
    .sort_values("引当数量", ascending=False)
)

top_n = min(20, len(by_comp))
top = by_comp.head(top_n).sort_values("引当数量", ascending=True)
fig = go.Figure()
fig.add_trace(go.Bar(
    x=top["引当数量"], y=top["part_number"].fillna("") + " / " + top["component_name"].fillna(""),
    orientation="h",
    marker=dict(color=t["green"], line=dict(width=0)),
    text=top["引当数量"].apply(lambda v: f"{v:,}"),
    textposition="outside",
    textfont=dict(color=t["text"], size=11),
    hovertemplate="%{y}<br>引当数量: %{x:,}<extra></extra>",
))
fig.update_layout(
    **base_layout(
        height=560, x_title="引当数量（個）", y_title="",
        show_legend=False, title=f"📦 引当数量 Top {top_n} ({today.isoformat()} 時点)",
    ),
)
fig.update_layout(margin=dict(l=240, r=20, t=46, b=44))
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ────────────────────────────────────────────────────────
# 部材別 引当残量と顧客在庫の比較
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 部材別 引当残量 × 顧客在庫 対比表")
st.caption(
    f"📅 **基準日**: {today.isoformat()} 時点のスナップショット (今日時点の在庫数値)。"
    "**用途**: 「顧客在庫が少ない × マクニカフリーが多い」部材を見つけ、"
    "突発需要や緊急増産の際に即時供給可能な候補を抽出します。"
    "活用余地スコアが高い部材は『緊急調達シミュレーター』で具体的なルート評価へ。"
)

free_by_comp = free.groupby("component_id", as_index=False)["qty_available"].sum()
free_by_comp.columns = ["component_id", "macnica_free_qty"]

if not cust_inv.empty:
    cust_by_comp = (
        cust_inv.groupby("component_id", as_index=False)["stock_qty"]
        .apply(lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0).sum()))
    )
    cust_by_comp.columns = ["component_id", "customer_qty"]
else:
    cust_by_comp = pd.DataFrame(columns=["component_id", "customer_qty"])

cmp = free_by_comp.merge(cust_by_comp, on="component_id", how="left")
cmp["customer_qty"] = pd.to_numeric(cmp["customer_qty"], errors="coerce").fillna(0).astype(int)
if not components.empty:
    cmp = cmp.merge(
        components[["component_id", "part_number", "component_name", "base_lead_time_weeks"]],
        on="component_id", how="left",
    )
cmp["合計"] = cmp["macnica_free_qty"] + cmp["customer_qty"]
cmp["活用余地スコア"] = cmp.apply(
    lambda r: r["macnica_free_qty"] / max(r["customer_qty"], 1) if r["macnica_free_qty"] > 0 else 0,
    axis=1,
).round(2)
cmp = cmp.sort_values("活用余地スコア", ascending=False)

show_cols_map = [
    ("component_id",         "部材ID"),
    ("part_number",          "品番"),
    ("component_name",       "部材名"),
    ("base_lead_time_weeks", "通常LT(週)"),
    ("macnica_free_qty",     "📦 マクニカフリー在庫"),
    ("customer_qty",         "🏭 顧客在庫"),
    ("合計",                  "合計"),
    ("活用余地スコア",          "活用余地(F/C比)"),
]
cols_present = [(k, v) for k, v in show_cols_map if k in cmp.columns]
st.dataframe(
    cmp[[k for k, _ in cols_present]].rename(columns=dict(cols_present)),
    hide_index=True, use_container_width=True, height=400,
)
st.caption(
    "💡 **活用余地スコア**: マクニカフリー数量 ÷ 顧客在庫数量。値が大きいほど"
    "「顧客在庫が薄く、マクニカフリーで補填する価値が高い」部材です。"
    "（スコア=0 はマクニカフリー在庫なし）"
)

# ────────────────────────────────────────────────────────
# 利用ガイド
# ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### ℹ️ マクニカフリー在庫の使い方")
st.markdown(
    """
- **特徴**: 既に貴社向けに引当済のため、マクニカ営業に相談すれば**通常の発注LTを待たずに**出荷手配が可能です。
- **保管場所**: マクニカ新子安ロジスティクスセンター（神奈川県横浜市鶴見区）一極集約。輸送日数は通常2〜3日。
- **典型的な使いどころ**:
    - 営業FCSTから漏れた突発需要が発生したとき
    - 既存発注残BLが間に合わないとき
    - 増産指示で必要数量が増えたとき
- **手順**:
    1. 「🚨 緊急調達シミュレーター」で対象部材・数量・希望納期を入力
    2. ② マクニカフリー在庫ルートで充足できるか確認
    3. 「マクニカ営業への相談用テキスト」を生成して送付
"""
)
