"""
🎯 調達アクションセンター【本Appのコア画面】
=============================================
業務フロー: バランスチェック → 4ルート判断 → 納期回答

このページの役割:
  - 全部材の充足状況を一覧表示（部材×希望納期×充足ランク）
  - 行クリックで4ルート比較（① 顧客在庫 / ② マクニカフリー / ③ 既存PO / ④ 新規発注）
  - 顧客（購買担当）が「どのルートで調達するか」を判断する中核UI
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.route_comparison import render_route_comparison
from components.inventory_badge import render_inventory_legend
from services.config import get_as_of_date
from services.database import (
    get_silver_demand_plan_components,
    get_silver_components,
    get_procurement_options,
)

st.set_page_config(page_title="調達アクションセンター | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="全部材の充足状況と4ルート評価")

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    demand     = get_silver_demand_plan_components()
    components = get_silver_components()
    options    = get_procurement_options()

today = get_as_of_date()

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 🎯 調達アクションセンター")
st.caption("全需要×全部材の充足状況を一覧。行を選択すると4ルート比較を表示します。")
render_inventory_legend()
st.markdown("")

if demand.empty or options.empty:
    st.warning("需要計画 または 調達評価データが空です。Lakeflow パイプラインを実行してください。")
    st.stop()

# ────────────────────────────────────────────────────────
# 需要×部材の集約 (各需要に対する4ルート評価から「最良ルート」を選定)
# ────────────────────────────────────────────────────────
opt = options.copy()
opt["shortage_qty"] = pd.to_numeric(opt["shortage_qty"], errors="coerce").fillna(0).astype(int)
opt["available_qty"] = pd.to_numeric(opt["available_qty"], errors="coerce").fillna(0).astype(int)
opt["days_late"] = pd.to_numeric(opt["days_late"], errors="coerce").fillna(0).astype(int)
opt["requested_qty"] = pd.to_numeric(opt["requested_qty"], errors="coerce").fillna(0).astype(int)
opt["requested_date"] = pd.to_datetime(opt["requested_date"], errors="coerce").dt.date

# 需要ごとの集計: 4ルート合計の確保可能数 / 最良ルート / 充足判定
agg = opt.groupby("demand_id", as_index=False).agg(
    requested_qty=("requested_qty", "first"),
    requested_date=("requested_date", "first"),
    component_id=("component_id", "first"),
    total_avail_4routes=("available_qty", "sum"),
)
agg["total_shortage_4routes"] = (agg["requested_qty"] - agg["total_avail_4routes"]).clip(lower=0)

# 部材名・カテゴリを結合
if not components.empty:
    comp_lite = components[["component_id", "part_number", "component_name", "component_category"]]
    agg = agg.merge(comp_lite, on="component_id", how="left")

# 需要メタ（source_type, note）も結合
demand_meta = demand[["demand_id", "source_type", "note"]] if not demand.empty else pd.DataFrame()
if not demand_meta.empty:
    agg = agg.merge(demand_meta, on="demand_id", how="left")

# 充足ランク
def _rank(row):
    if row["total_shortage_4routes"] <= 0:
        return "🟢 充足"
    elif row["total_shortage_4routes"] / max(row["requested_qty"], 1) < 0.3:
        return "🟡 一部不足"
    else:
        return "🔴 不足大"

agg["充足ランク"] = agg.apply(_rank, axis=1)
agg["希望納期まで"] = (pd.to_datetime(agg["requested_date"]) - pd.Timestamp(today)).dt.days

# ────────────────────────────────────────────────────────
# フィルター
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込み")
fc1, fc2, fc3, fc4 = st.columns(4)

with fc1:
    rank_options = ["（すべて）", "🔴 不足大", "🟡 一部不足", "🟢 充足"]
    sel_rank = st.selectbox("充足ランク", rank_options)

with fc2:
    cat_opts = ["（すべて）"] + sorted([c for c in agg["component_category"].dropna().unique()]) if "component_category" in agg.columns else ["（すべて）"]
    sel_cat = st.selectbox("部材カテゴリ", cat_opts)

with fc3:
    src_opts = ["（すべて）", "FCST_AUTO", "EMERGENCY_MANUAL"]
    sel_src = st.selectbox("需要発生源", src_opts)

with fc4:
    days_filter = st.selectbox(
        "希望納期",
        ["（すべて）", "今日まで（超過のみ）", "30日以内", "60日以内", "90日以内"],
    )

# 絞り込み適用
df = agg.copy()
if sel_rank != "（すべて）":
    df = df[df["充足ランク"] == sel_rank]
if sel_cat != "（すべて）" and "component_category" in df.columns:
    df = df[df["component_category"] == sel_cat]
if sel_src != "（すべて）" and "source_type" in df.columns:
    df = df[df["source_type"] == sel_src]
if days_filter == "今日まで（超過のみ）":
    df = df[df["希望納期まで"] < 0]
elif days_filter == "30日以内":
    df = df[(df["希望納期まで"] >= 0) & (df["希望納期まで"] <= 30)]
elif days_filter == "60日以内":
    df = df[(df["希望納期まで"] >= 0) & (df["希望納期まで"] <= 60)]
elif days_filter == "90日以内":
    df = df[(df["希望納期まで"] >= 0) & (df["希望納期まで"] <= 90)]

# ────────────────────────────────────────────────────────
# 一覧表
# ────────────────────────────────────────────────────────
st.markdown(f"### 📋 需要一覧 ({len(df):,} 件)")

if df.empty:
    st.info("条件に該当する需要がありません。フィルターを調整してください。")
else:
    # 表示順: 不足大→一部不足→充足、希望納期昇順
    rank_order = {"🔴 不足大": 0, "🟡 一部不足": 1, "🟢 充足": 2}
    df["_rank_order"] = df["充足ランク"].map(rank_order).fillna(3)
    df = df.sort_values(["_rank_order", "希望納期まで"]).drop(columns="_rank_order")

    show_cols_map = [
        ("demand_id",              "需要ID"),
        ("part_number",            "品番"),
        ("component_name",         "部材名"),
        ("source_type",            "発生源"),
        ("requested_date",         "希望納期"),
        ("希望納期まで",            "残日数"),
        ("requested_qty",          "必要数"),
        ("total_avail_4routes",    "4ルート合計確保可能数"),
        ("total_shortage_4routes", "不足数"),
        ("充足ランク",              "充足ランク"),
    ]
    cols_present = [(k, v) for k, v in show_cols_map if k in df.columns]
    df_show = df[[k for k, _ in cols_present]].rename(columns=dict(cols_present))

    st.dataframe(df_show, hide_index=True, use_container_width=True, height=380)

    st.markdown("---")

    # ────────────────────────────────────────────────────
    # 4ルート比較: 1需要を選択
    # ────────────────────────────────────────────────────
    st.markdown("### 🔬 4ルート比較（需要を選択）")

    # 選択肢ラベル: "需要ID  ｜ 品番  ｜ 必要N  ｜ 希望納期"
    df_select = df.head(200).copy()  # 大量データ時に選択肢が膨大化しないよう制限
    df_select["_label"] = (
        df_select["demand_id"].astype(str)
        + "  ｜  " + df_select.get("part_number", pd.Series("")).fillna("").astype(str)
        + "  ｜  " + df_select.get("component_name", pd.Series("")).fillna("").astype(str)
        + "  ｜  必要 " + df_select["requested_qty"].astype(str) + " 個"
        + "  ｜  納期 " + df_select["requested_date"].astype(str)
        + "  ｜  " + df_select["充足ランク"].astype(str)
    )
    sel_label = st.selectbox("詳細を見る需要を選択", df_select["_label"].tolist())
    if sel_label:
        sel_demand_id = sel_label.split("  ｜  ", 1)[0].strip()
        # 該当4ルート
        sel_options = opt[opt["demand_id"] == sel_demand_id].copy()
        sel_demand = df_select[df_select["demand_id"] == sel_demand_id].iloc[0]

        render_route_comparison(
            sel_options,
            requested_qty=int(sel_demand["requested_qty"]),
            requested_date=sel_demand["requested_date"],
        )

        # 補助情報
        st.markdown("")
        st.caption(
            f"💡 部材: {sel_demand.get('component_name', '—')} ({sel_demand.get('part_number', '—')})"
            f" ｜ 発生源: {sel_demand.get('source_type', '—')}"
            f" ｜ メモ: {sel_demand.get('note', '') or '—'}"
        )
