"""
🧩 製品BOM充足ビュー
=====================
業務上の重要性:
  - 部材1点が充足しても、BOM上の他部材が揃わなければ製品は作れない
  - 「製品Xの生産が4月に止まりそう」という意思決定上の重要情報を一画面で把握
  - 各製品×月の充足率と不足部材リストを表示
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from services.config import get_as_of_date
from services.database import (
    get_bom_fulfillment_status,
    get_procurement_options,
    get_silver_components,
    get_silver_bom,
    get_silver_products,
    get_silver_demand_plan_components,
)

st.set_page_config(page_title="製品BOM充足ビュー | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="製品単位で『全部材揃うか？』を可視化")

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    fulfill    = get_bom_fulfillment_status()
    proc       = get_procurement_options()
    components = get_silver_components()
    bom        = get_silver_bom()
    products   = get_silver_products()
    demand     = get_silver_demand_plan_components()

today = get_as_of_date()

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 🧩 製品BOM充足ビュー")
st.caption(
    "営業FCSTの製品需要に対し、BOM展開された全部材が組み合わせ充足可能か製品単位で判定。"
    "1つでも『重』レベルの部材があれば製品の生産が遅延します。"
)

if fulfill.empty:
    st.warning("BOM充足状況データ (gold_bom_fulfillment_status) が空です。Lakeflow パイプラインを実行してください。")
    st.stop()

# ────────────────────────────────────────────────────────
# KPI
# ────────────────────────────────────────────────────────
df = fulfill.copy()

# 「is_all_fulfilled」の boolean 化（パースの揺れ吸収）
def _to_bool(v):
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "t")

df["is_all_fulfilled"] = df["is_all_fulfilled"].apply(_to_bool)
df["fulfillment_rate"] = pd.to_numeric(df["fulfillment_rate"], errors="coerce").fillna(0)
df["total_components"] = pd.to_numeric(df["total_components"], errors="coerce").fillna(0).astype(int)
df["shortage_components"] = pd.to_numeric(df["shortage_components"], errors="coerce").fillna(0).astype(int)

n_total = len(df)
n_ok = int(df["is_all_fulfilled"].sum())
n_partial = int(((df["fulfillment_rate"] >= 0.8) & (~df["is_all_fulfilled"])).sum())
n_critical = int((df["fulfillment_rate"] < 0.8).sum())

k1, k2, k3, k4 = st.columns(4)
k1.metric("評価対象（製品×月）", f"{n_total} 件")
k2.metric("🟢 生産可能", f"{n_ok} 件")
k3.metric("🟡 一部部材不足", f"{n_partial} 件")
k4.metric("🔴 生産困難", f"{n_critical} 件")

st.markdown("---")

# ────────────────────────────────────────────────────────
# フィルター（タイトル明確化）
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター（製品BOM充足状況の表示範囲を絞る）")
fc1, fc2, fc3 = st.columns(3)

with fc1:
    status_options = ["（すべて）", "🔴 生産困難", "🟡 一部部材不足", "🟢 生産可能"]
    sel_status = st.selectbox(
        "🏭 生産可否ステータス（BOM部材の充足度合いで分類）",
        status_options,
    )

with fc2:
    cat_options = ["（すべて）"] + sorted([c for c in df.get("product_category", pd.Series(dtype=str)).dropna().unique()])
    sel_cat = st.selectbox(
        "📦 製品カテゴリで絞り込み",
        cat_options,
    )

with fc3:
    months = sorted(df["requested_month"].dropna().unique().tolist())
    month_options = ["（すべて）"] + months
    sel_month = st.selectbox(
        "📅 生産対象月で絞り込み（FCSTの納期月）",
        month_options,
    )

df_show = df.copy()
if sel_status != "（すべて）":
    df_show = df_show[df_show["production_status"] == sel_status]
if sel_cat != "（すべて）":
    df_show = df_show[df_show["product_category"] == sel_cat]
if sel_month != "（すべて）":
    df_show = df_show[df_show["requested_month"] == sel_month]

st.markdown("---")

# ────────────────────────────────────────────────────────
# 製品×月 一覧
# ────────────────────────────────────────────────────────
st.markdown(f"### 📋 製品×月 充足状況 ({len(df_show)} 件)")

if df_show.empty:
    st.info("条件に該当する製品がありません。")
else:
    # ソート
    status_order = {"🔴 生産困難": 0, "🟡 一部部材不足": 1, "🟢 生産可能": 2}
    df_show["_o"] = df_show["production_status"].map(status_order).fillna(9)
    df_show = df_show.sort_values(["_o", "requested_month"]).drop(columns="_o")

    show_cols = [
        ("product_id",            "製品ID"),
        ("product_name",          "製品名"),
        ("product_category",      "製品カテゴリ"),
        ("requested_month",       "対象月"),
        ("total_components",      "BOM部材数"),
        ("fulfillable_components", "充足可能部材数"),
        ("shortage_components",   "不足部材数"),
        ("fulfillment_rate",      "充足率"),
        ("production_status",     "ステータス"),
    ]
    cols_present = [(k, v) for k, v in show_cols if k in df_show.columns]
    table = df_show[[k for k, _ in cols_present]].rename(columns=dict(cols_present))
    st.dataframe(table, hide_index=True, use_container_width=True, height=320)

st.markdown("---")

# ────────────────────────────────────────────────────────
# 製品×月を選択して詳細
# ────────────────────────────────────────────────────────
st.markdown("### 🔬 製品×月 詳細（不足部材の特定）")

if df_show.empty:
    st.info("詳細を見たい行がありません。")
else:
    df_select = df_show.head(100).copy()
    df_select["_label"] = (
        df_select["product_name"].fillna("").astype(str)
        + "  ｜  " + df_select["requested_month"].astype(str)
        + "  ｜  " + df_select["production_status"].astype(str)
        + "  ｜  充足 " + (df_select["fulfillment_rate"] * 100).round(1).astype(str) + "%"
    )

    sel_label = st.selectbox("製品×月を選択", df_select["_label"].tolist())
    if sel_label:
        sel_row = df_select[df_select["_label"] == sel_label].iloc[0]
        sel_product_id = sel_row["product_id"]
        sel_month = sel_row["requested_month"]

        st.markdown(f"**選択中**: {sel_row['product_name']} / {sel_month}")
        st.caption(
            f"BOM部材数 {int(sel_row['total_components'])} 件中、"
            f"充足可能 {int(sel_row['fulfillable_components'])} 件 / "
            f"不足 {int(sel_row['shortage_components'])} 件 "
            f"(充足率 {sel_row['fulfillment_rate'] * 100:.1f}%)"
        )

        # この製品×月のBOM展開と各部材の充足状況
        if not bom.empty and not demand.empty and not proc.empty:
            # BOMの当該製品の部材
            prod_bom = bom[bom["product_id"] == sel_product_id]

            # その月のFCST_AUTO需要
            demand_local = demand.copy()
            demand_local["requested_date"] = pd.to_datetime(demand_local["requested_date"], errors="coerce")
            demand_local["requested_month"] = demand_local["requested_date"].dt.strftime("%Y-%m")
            demand_target = demand_local[
                (demand_local["product_id"] == sel_product_id)
                & (demand_local["requested_month"] == sel_month)
            ]

            # 各部材の action_level
            proc_local = proc.copy()
            proc_local = proc_local.drop_duplicates(subset=["demand_id", "component_id"])

            merged = demand_target.merge(
                proc_local[["demand_id", "component_id", "action_level", "combo_ok", "available_qty", "shortage_qty"]],
                on=["demand_id", "component_id"],
                how="left",
            )

            # 部材名を結合
            if not components.empty:
                merged = merged.merge(
                    components[["component_id", "part_number", "component_name"]],
                    on="component_id", how="left",
                )

            # 表示
            display_cols = [
                ("part_number",    "品番"),
                ("component_name", "部材名"),
                ("requested_qty",  "必要数"),
                ("available_qty",  "確保可能数"),
                ("shortage_qty",   "不足数"),
                ("action_level",   "対応レベル"),
            ]
            cols_present = [(k, v) for k, v in display_cols if k in merged.columns]
            if cols_present and not merged.empty:
                merged_show = merged[[k for k, _ in cols_present]].rename(columns=dict(cols_present))
                # 対応レベル順にソート
                level_order = {"重": 0, "中": 1, "軽": 2, "不要": 3}
                if "対応レベル" in merged_show.columns:
                    merged_show["_l"] = merged_show["対応レベル"].map(level_order).fillna(9)
                    merged_show = merged_show.sort_values("_l").drop(columns="_l")
                st.dataframe(merged_show, hide_index=True, use_container_width=True)

                # ボトルネック部材を強調
                bottleneck = merged[merged["action_level"] == "重"] if "action_level" in merged.columns else pd.DataFrame()
                if not bottleneck.empty:
                    names = bottleneck.get("component_name", pd.Series([""])).fillna("").astype(str).tolist()
                    st.error(
                        f"🔴 ボトルネック部材（新規発注LT必須）: {len(bottleneck)} 件 — "
                        f"{', '.join(names[:5])}"
                        + ("..." if len(names) > 5 else "")
                    )
            else:
                st.info("詳細データを取得できませんでした。")
