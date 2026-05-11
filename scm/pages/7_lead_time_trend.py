"""
⏳ リードタイム推移モニター
============================
業務上の役割:
  - 部材別リードタイム(LT)の3ヶ月前/6ヶ月前との比較
  - LTが直近で延長傾向にある部材を即座にエスカレーション
  - 月次LT推移グラフで「いつから延びたのか」を可視化
  - 「新規発注必要」の意思決定根拠として欠かせない情報源
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css, plot_colors
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.search_bar import (
    render_search_bar, render_component_selector,
    apply_component_search, apply_component_id_filter,
)
from services.plot_theme import base_layout, get_theme_tokens, palette as theme_palette
from services.config import get_as_of_date
from services.database import (
    get_lt_snapshot,
    get_lt_trend,
    get_lt_escalation,
    get_silver_components,
)

st.set_page_config(page_title="リードタイム推移 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="部材LTの3ヶ月前/6ヶ月前比較と延長傾向アラート")

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    snap   = get_lt_snapshot()
    trend  = get_lt_trend()
    escal  = get_lt_escalation()
    comps  = get_silver_components()

today = get_as_of_date()
colors = plot_colors()

st.markdown("## ⏳ リードタイム推移モニター")
st.caption(
    "部材ごとの現在LT、N-1/N-3/N-6ヶ月前との比較、延長傾向の早期察知。"
    "LTが伸びた部材は新規発注タイミングを前倒しする必要があります。"
)

if snap.empty:
    st.warning("gold_lt_snapshot_current が空です。Lakeflow パイプラインを実行してください。")
    st.stop()

# 型整え
snap = snap.copy()
for c in ("latest_lt_weeks", "lt_n1_weeks", "lt_n3_weeks", "lt_n6_weeks", "delta_vs_n1", "delta_vs_n3", "delta_vs_n6"):
    if c in snap.columns:
        snap[c] = pd.to_numeric(snap[c], errors="coerce").astype("Int64")

# ────────────────────────────────────────────────────────
# KPI
# ────────────────────────────────────────────────────────
n_total = len(snap)
n_escal = len(escal) if not escal.empty else 0
n_up_n3 = int((snap["trend_arrow_n3"] == "↑").sum()) if "trend_arrow_n3" in snap.columns else 0
n_up_n6 = int((snap["trend_arrow_n6"] == "↑").sum()) if "trend_arrow_n6" in snap.columns else 0
avg_lt  = float(snap["latest_lt_weeks"].dropna().astype(float).mean()) if not snap.empty else 0.0

k1, k2, k3, k4 = st.columns(4)
k1.metric("📦 監視中部材", f"{n_total} 品目")
k2.metric("⚠️ LT延長中（N-3↑）", f"{n_up_n3} 品目")
k3.metric("🚨 LT延長中（N-6↑）", f"{n_up_n6} 品目")
k4.metric("📊 平均LT", f"{avg_lt:.1f} 週")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 検索 + フィルター
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター（LT推移の表示範囲を絞る）")

if "part_number" in snap.columns and "item_code" not in snap.columns:
    pass
# 検索用に snap に部材名を再アタッチ (snap には item_name 既にある)
snap_for_search = snap.copy()
snap_for_search["component_id"] = snap_for_search["item_id"]
snap_for_search["part_number"] = snap_for_search["item_code"]
snap_for_search["component_name"] = snap_for_search["item_name"]

# Phase 7: 部材カテゴリは廃止、検索 + セレクタ + LT傾向で絞り込み
sb1, sb2 = st.columns(2)
with sb1:
    search_query = render_search_bar(comps if not comps.empty else snap_for_search, key="lt_search")
with sb2:
    sel_comp_ids = render_component_selector(comps if not comps.empty else snap_for_search, key="lt_comp_sel")

fc1, fc2 = st.columns(2)
with fc1:
    band_options = ["（すべて）"] + sorted([b for b in snap_for_search.get("lt_band", pd.Series(dtype=str)).dropna().unique()])
    sel_band = st.selectbox("📏 LTバンドで絞り込み", band_options)
with fc2:
    trend_options = ["（すべて）", "🚨 LT延長中（N-3↑ or N-6↑）", "📈 LT短縮中（↓）", "→ 横ばい"]
    sel_trend = st.selectbox("📊 LT推移傾向で絞り込み", trend_options)

filtered = snap_for_search.copy()
if search_query:
    filtered = apply_component_search(filtered, search_query)
if sel_comp_ids:
    filtered = apply_component_id_filter(filtered, sel_comp_ids)
if sel_band != "（すべて）":
    filtered = filtered[filtered["lt_band"] == sel_band]
if sel_trend == "🚨 LT延長中（N-3↑ or N-6↑）":
    filtered = filtered[(filtered.get("trend_arrow_n3") == "↑") | (filtered.get("trend_arrow_n6") == "↑")]
elif sel_trend == "📈 LT短縮中（↓）":
    filtered = filtered[(filtered.get("trend_arrow_n3") == "↓") | (filtered.get("trend_arrow_n6") == "↓")]
elif sel_trend == "→ 横ばい":
    filtered = filtered[(filtered.get("trend_arrow_n3") == "→") & (filtered.get("trend_arrow_n6") == "→")]

st.markdown("---")

# ────────────────────────────────────────────────────────
# 一覧表 (現在LT + N-1/N-3/N-6比較)
# ────────────────────────────────────────────────────────
st.markdown(f"### 📋 部材別 LT 一覧 ({len(filtered):,} 品目)")
if filtered.empty:
    st.info("条件に該当する部材がありません。フィルターを調整してください。")
else:
    # 表示列を業務用語に
    show_cols = [
        ("item_code",       "品番"),
        ("item_name",       "部材名"),
        ("component_category", "カテゴリ"),
        ("manufacturer_name", "メーカー"),
        ("latest_lt_weeks", "現在LT(週)"),
        ("lt_n1_weeks",     "1ヶ月前(週)"),
        ("trend_arrow_n1",  "vs1M"),
        ("lt_n3_weeks",     "3ヶ月前(週)"),
        ("trend_arrow_n3",  "vs3M"),
        ("lt_n6_weeks",     "6ヶ月前(週)"),
        ("trend_arrow_n6",  "vs6M"),
        ("lt_band",         "LTバンド"),
        ("remark",          "コメント"),
    ]
    cols_present = [(k, v) for k, v in show_cols if k in filtered.columns]
    table = filtered[[k for k, _ in cols_present]].rename(columns=dict(cols_present))
    st.dataframe(table, hide_index=True, use_container_width=True, height=380)

st.markdown("---")

# ────────────────────────────────────────────────────────
# LT延長中のエスカレーション部材
# ────────────────────────────────────────────────────────
st.markdown("### 🚨 LT延長中の部材（N-3 or N-6 比較で延びている）")
if escal.empty:
    st.success("✅ LT延長中の部材はありません。")
else:
    show_cols = [
        ("item_code",       "品番"),
        ("item_name",       "部材名"),
        ("latest_lt_weeks", "現在LT(週)"),
        ("lt_n3_weeks",     "3ヶ月前(週)"),
        ("delta_vs_n3",     "vs3M差分"),
        ("lt_n6_weeks",     "6ヶ月前(週)"),
        ("delta_vs_n6",     "vs6M差分"),
        ("escalation_reason", "理由"),
    ]
    cols_present = [(k, v) for k, v in show_cols if k in escal.columns]
    e_show = escal[[k for k, _ in cols_present]].rename(columns=dict(cols_present))
    st.dataframe(e_show, hide_index=True, use_container_width=True)
    st.caption(f"💡 上記 {len(escal)} 品目は新規発注のリードタイム見積もりを再評価してください。")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 部材別 LT月次推移グラフ
# ────────────────────────────────────────────────────────
st.markdown("### 📈 部材別 LT月次推移（製品/部材を選択してグラフ表示）")
st.caption("複数部材を選択して比較可能。突然LTが上昇したタイミングを可視化します。")

if trend.empty:
    st.info("gold_lt_trend_monthly が空です。")
else:
    trend = trend.copy()
    trend["lead_time_weeks"] = pd.to_numeric(trend["lead_time_weeks"], errors="coerce")

    # ピッカー用ラベル
    pick_df = trend[["component_id", "part_number", "component_name"]].drop_duplicates()
    pick_df["_label"] = (
        pick_df["component_id"].astype(str)
        + "  ｜  " + pick_df["part_number"].fillna("").astype(str)
        + "  ｜  " + pick_df["component_name"].fillna("").astype(str)
    )
    top3 = filtered.nlargest(3, "latest_lt_weeks")["item_id"].tolist() if not filtered.empty else []
    default_labels = pick_df[pick_df["component_id"].isin(top3)]["_label"].tolist()

    sel_labels = st.multiselect(
        "📊 LT推移を見たい部材（最大10件まで選択推奨）",
        pick_df["_label"].tolist(),
        default=default_labels[:3],
    )
    if sel_labels:
        sel_ids = [s.split("  ｜  ", 1)[0].strip() for s in sel_labels]
        sub = trend[trend["component_id"].isin(sel_ids)].copy()
        if not sub.empty:
            sub["month_dt"] = pd.to_datetime(sub["month"] + "-01", errors="coerce")
            sub = sub.sort_values(["component_id", "month_dt"])
            color_pal = theme_palette()

            fig = go.Figure()
            for i, cid in enumerate(sel_ids):
                s = sub[sub["component_id"] == cid]
                if s.empty:
                    continue
                label = f"{s['part_number'].iloc[0]} / {s['component_name'].iloc[0]}"
                fig.add_trace(go.Scatter(
                    x=s["month_dt"], y=s["lead_time_weeks"],
                    mode="lines+markers",
                    name=label,
                    line=dict(color=color_pal[i % len(color_pal)], width=2.5),
                    marker=dict(size=7),
                ))

            fig.update_layout(**base_layout(height=420, x_title="年月", y_title="リードタイム（週）"))
            fig.update_xaxes(tickformat="%Y-%m")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("選択部材の推移データが見つかりません。")
    else:
        st.info("上のセレクタから部材を選んでください。")
