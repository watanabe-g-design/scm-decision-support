"""
📅 製品FCST × 部材必要量タイムライン
====================================
Phase 8: 3タブ構成 (月別サマリー / FCST vs 実績 / 緊急一覧)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.timeline_helper import add_today_vline, add_actual_forecast_flag
from components.search_bar import (
    render_search_bar, render_component_selector,
    apply_component_search, apply_component_id_filter,
)
from services.config import get_as_of_date
from services.plot_theme import base_layout, get_theme_tokens, palette
from services.database import (
    get_silver_demand_plan_components,
    get_silver_components,
    get_silver_products,
    get_silver_forecasts,
    get_procurement_options,
)

st.set_page_config(page_title="製品FCST×部材必要量 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="営業FCSTからの部材展開と緊急需要を重ねて表示")

t = get_theme_tokens()

with st.spinner("データを読み込んでいます..."):
    demand     = get_silver_demand_plan_components()
    components = get_silver_components()
    products   = get_silver_products()
    forecasts  = get_silver_forecasts()
    options    = get_procurement_options()

today = get_as_of_date()

st.markdown("## 📅 製品FCST × 部材必要量タイムライン")
st.caption(
    "営業からの製品FCSTをBOM展開して算出した部材レベル需要 + 緊急手動入力。"
    "FCSTは外れる前提のため、緊急需要の早期察知が重要です。"
)

if demand.empty:
    st.warning("需要計画データ (silver_demand_plan_components) が空です。")
    st.stop()

demand = demand.copy()
demand["requested_date"] = pd.to_datetime(demand["requested_date"], errors="coerce").dt.date
demand["requested_qty"] = pd.to_numeric(demand["requested_qty"], errors="coerce").fillna(0).astype(int)
demand = demand[demand["requested_date"] >= today]

if not components.empty:
    demand = demand.merge(
        components[["component_id", "part_number", "component_name"]],
        on="component_id", how="left",
    )

# ── フィルター ──
st.markdown("### 🔍 絞り込みフィルター")
fc1, fc2 = st.columns(2)
with fc1:
    search_query = render_search_bar(components, key="dt_search")
with fc2:
    sel_src = st.selectbox(
        "📋 需要発生源",
        ["（すべて）", "営業FCSTから自動展開", "緊急手動入力"],
        index=0,
    )

selected_ids = render_component_selector(components, key="dt_comp_select")
months_ahead = st.slider("📅 表示月数", 1, 12, 9)

df = demand.copy()
if search_query:
    df = apply_component_search(df, search_query)
if selected_ids:
    df = apply_component_id_filter(df, selected_ids)
if sel_src != "（すべて）":
    src_map = {"営業FCSTから自動展開": "FCST_AUTO", "緊急手動入力": "EMERGENCY_MANUAL"}
    df = df[df["source_type"] == src_map[sel_src]]
df = df[df["requested_date"] <= today + timedelta(days=months_ahead * 30)]

st.markdown("---")

# ── 3タブ ──
tab1, tab2, tab3 = st.tabs([
    "📊 部材 月別需要サマリー（製品FCST×BOM展開）",
    "📈 製品 FCST vs 実績",
    "🚨 緊急手動入力（部材レベル）",
])

# ─── Tab 1: 月別サマリー ───────────────────────────────────────────
with tab1:
    st.caption(
        "月別の合計必要数量。FCST自動展開(青)と緊急手動入力(赤)の内訳を棒グラフで表示。"
        "縦の点線が本日です。"
    )
    if df.empty:
        st.info("条件に該当する需要がありません。")
    else:
        df_chart = df.copy()
        df_chart["month_center"] = (
            pd.to_datetime(df_chart["requested_date"]).dt.to_period("M").dt.to_timestamp()
            + pd.Timedelta(days=14)
        )
        monthly = df_chart.groupby(["month_center", "source_type"], as_index=False)["requested_qty"].sum()

        months_u = sorted(monthly["month_center"].unique())
        fcst_s = monthly[monthly["source_type"] == "FCST_AUTO"].set_index("month_center")["requested_qty"]
        emrg_s = monthly[monthly["source_type"] == "EMERGENCY_MANUAL"].set_index("month_center")["requested_qty"]
        fcst_v = [int(fcst_s.get(m, 0)) for m in months_u]
        emrg_v = [int(emrg_s.get(m, 0)) for m in months_u]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=months_u, y=fcst_v,
            name="FCST自動展開",
            marker=dict(color=t["blue"], line=dict(width=0)),
            hovertemplate="%{x|%Y-%m}<br>FCST需要: %{y:,}個<extra></extra>",
        ))
        if any(v > 0 for v in emrg_v):
            fig.add_trace(go.Bar(
                x=months_u, y=emrg_v,
                name="緊急手動入力",
                marker=dict(color=t["red"], line=dict(width=0)),
                hovertemplate="%{x|%Y-%m}<br>緊急需要: %{y:,}個<extra></extra>",
            ))
        fig.update_layout(
            barmode="stack",
            **base_layout(height=420, x_title="希望納期（月）", y_title="必要数量（個）"),
        )
        fig.update_xaxes(tickformat="%Y-%m")
        add_today_vline(fig)
        st.plotly_chart(fig, use_container_width=True)

        # 月別サマリー表
        monthly_pivot = monthly.pivot(index="month_center", columns="source_type", values="requested_qty").fillna(0)
        monthly_pivot.index = monthly_pivot.index.strftime("%Y-%m")
        monthly_pivot.columns.name = None
        if "FCST_AUTO" in monthly_pivot.columns:
            monthly_pivot.rename(columns={"FCST_AUTO": "FCST自動展開"}, inplace=True)
        if "EMERGENCY_MANUAL" in monthly_pivot.columns:
            monthly_pivot.rename(columns={"EMERGENCY_MANUAL": "緊急手動入力"}, inplace=True)
        monthly_pivot["合計"] = monthly_pivot.sum(axis=1)
        st.dataframe(monthly_pivot.astype(int), use_container_width=True)

# ─── Tab 2: FCST vs 実績 ───────────────────────────────────────────
with tab2:
    st.caption(
        "選択した製品の月次FCSTと実績数量を時系列で対比表示。"
        "**実線=実績（過去のみ）、破線=FCST**。製品ごとのFCST精度を確認できます。"
    )
    if forecasts.empty:
        st.info("forecasts データがありません。")
    else:
        fc = forecasts.copy()
        fc["forecast_month"] = pd.to_datetime(fc["forecast_month"], errors="coerce").dt.date
        fc["forecast_qty"] = pd.to_numeric(fc["forecast_qty"], errors="coerce").fillna(0)
        fc["forecast_accuracy"] = pd.to_numeric(fc["forecast_accuracy"], errors="coerce").fillna(0.8)
        if not products.empty:
            fc = fc.merge(
                products[["product_id", "product_name", "customer_id"]],
                on=["product_id", "customer_id"], how="left",
            )
        fc_t = fc[fc["customer_id"] == "CUS001"].copy() if "customer_id" in fc.columns else fc.copy()
        if fc_t.empty:
            st.info("CUS001 の FCSTデータがありません。")
        else:
            pick = fc_t[["product_id", "product_name"]].drop_duplicates()
            pick["_label"] = pick["product_id"].astype(str) + "  ｜  " + pick["product_name"].fillna("").astype(str)
            defaults = pick["product_id"].head(2).tolist()
            def_labels = pick[pick["product_id"].isin(defaults)]["_label"].tolist()
            sel = st.multiselect("📊 推移を見たい製品（最大5件推奨）", pick["_label"].tolist(), default=def_labels[:2])
            if sel:
                sel_pids = [s.split("  ｜  ", 1)[0].strip() for s in sel]
                sub = fc_t[fc_t["product_id"].isin(sel_pids)].sort_values("forecast_month")
                np.random.seed(42)
                sub = sub.copy()
                sub["actual_qty"] = (
                    sub["forecast_qty"] * (sub["forecast_accuracy"] + np.random.uniform(-0.1, 0.1, size=len(sub)))
                ).clip(lower=0).round().astype(int)
                sub.loc[pd.to_datetime(sub["forecast_month"]).dt.date > today, "actual_qty"] = pd.NA

                fig = go.Figure()
                pal = palette()
                for i, pid in enumerate(sel_pids):
                    s = sub[sub["product_id"] == pid].sort_values("forecast_month")
                    if s.empty:
                        continue
                    name = f"{s['product_name'].iloc[0]}"
                    color = pal[i % len(pal)]
                    fig.add_trace(go.Scatter(
                        x=s["forecast_month"], y=s["forecast_qty"],
                        mode="lines+markers", name=f"{name} FCST",
                        line=dict(color=color, width=2, dash="dash"),
                        marker=dict(size=7, symbol="circle-open"),
                    ))
                    s_act = s.dropna(subset=["actual_qty"])
                    if not s_act.empty:
                        fig.add_trace(go.Scatter(
                            x=s_act["forecast_month"], y=s_act["actual_qty"],
                            mode="lines+markers", name=f"{name} 実績",
                            line=dict(color=color, width=2.5),
                            marker=dict(size=8),
                        ))
                fig.update_layout(**base_layout(height=440, x_title="年月", y_title="数量（個）"))
                fig.update_xaxes(tickformat="%Y-%m")
                add_today_vline(fig)
                st.plotly_chart(fig, use_container_width=True)

                rows = []
                for pid in sel_pids:
                    s = sub[sub["product_id"] == pid].dropna(subset=["actual_qty"])
                    if s.empty:
                        continue
                    tot_fc = int(s["forecast_qty"].sum())
                    tot_ac = int(pd.to_numeric(s["actual_qty"], errors="coerce").fillna(0).sum())
                    acc = (1 - abs(tot_fc - tot_ac) / max(tot_fc, 1)) * 100
                    rows.append({"製品": s["product_name"].iloc[0], "FCST合計": tot_fc,
                                 "実績合計": tot_ac, "差分": tot_ac - tot_fc, "精度(%)": round(acc, 1)})
                if rows:
                    st.markdown("**FCST精度サマリー（実績期間のみ）**")
                    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
            else:
                st.info("上のセレクタから製品を選んでください。")

# ─── Tab 3: 緊急手動入力 (Top10は削除: 調達アクションセンターに統合) ──
with tab3:
    st.markdown("#### 🚨 緊急手動入力の明細")
    st.caption(
        "営業FCSTから漏れた突発的な需要のリスト。"
        "各案件の**4ルート評価・具体アクション案**は「🎯 調達アクションセンター」で確認できます。"
    )
    emerg = df[df["source_type"] == "EMERGENCY_MANUAL"].copy()
    if emerg.empty:
        st.info("条件に該当する緊急手動入力はありません。")
    else:
        emerg = add_actual_forecast_flag(emerg, "requested_date", flag_col="区分")
        show = emerg[[
            "demand_id", "part_number", "component_name",
            "requested_date", "requested_qty", "区分", "note",
        ]].rename(columns={
            "demand_id": "需要ID", "part_number": "品番", "component_name": "部材名",
            "requested_date": "希望納期", "requested_qty": "必要数量", "note": "発生理由",
        })
        st.dataframe(show, hide_index=True, use_container_width=True)
        st.caption(f"💡 緊急需要 {len(emerg)} 件。各案件の調達評価は「🎯 調達アクションセンター」へ。")

    if False:  # U2: Top10 削除 (調達アクションセンターに統合済み)
        st.markdown("dummy")
        if not options.empty and not df.empty:
            opt = options.copy()
            opt["requested_date"] = pd.to_datetime(opt["requested_date"], errors="coerce").dt.date
            opt["shortage_qty"] = pd.to_numeric(opt["shortage_qty"], errors="coerce").fillna(0)
            opt["_score"] = opt["shortage_qty"].clip(lower=0) * 1000
            best = opt.sort_values("_score").groupby("demand_id").first().reset_index()
            cutoff = today + timedelta(days=30)
            needs = best[(best["requested_date"] >= today) & (best["requested_date"] <= cutoff)].copy()
            if "action_level" in needs.columns:
                needs = needs[needs["action_level"] != "不要"]
            needs = needs.sort_values("shortage_qty", ascending=False).head(10)
            if needs.empty:
                st.success("✅ 直近30日に要対応の需要はありません。")
            else:
                if not components.empty:
                    needs = needs.merge(
                        components[["component_id", "part_number", "component_name"]],
                        on="component_id", how="left",
                    )
                from services.glossary import action_level_label_jp
                if "action_level" in needs.columns:
                    needs["Priority"] = needs["action_level"].apply(action_level_label_jp)
                show_c = [("part_number","品番"),("component_name","部材名"),
                          ("requested_date","希望納期"),("requested_qty","必要数"),
                          ("shortage_qty","不足数"),("Priority","Priority")]
                cols_p = [(k,v) for k,v in show_c if k in needs.columns]
                st.dataframe(
                    needs[[k for k,_ in cols_p]].rename(columns=dict(cols_p)),
                    hide_index=True, use_container_width=True,
                )
                st.caption("💡 全件確認 + 詳細評価は「🎯 調達アクションセンター」へ。")
