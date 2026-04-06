"""
E. Network & Warehouse Health
仕様書§11: 倉庫別健全性と物流依存の偏りを判断
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from services.database import get_geo_warehouse, get_shipment_routes
from components.japan_map import render_japan_map
from components.global_filter import render_global_filter
from components.explain_panel import render_explain
from services.genie_client import get_genie_client

st.set_page_config(page_title="拠点・倉庫健全性 | SCM判断支援", page_icon="🗺️", layout="wide")
inject_css()
from components.sidebar import render_sidebar
render_sidebar()

if "selected_warehouses" not in st.session_state:
    st.session_state.selected_warehouses = []

wh_df = get_geo_warehouse()
routes = get_shipment_routes()

st.markdown("## 🗺️ 拠点・倉庫健全性")
st.caption("倉庫別健全性 + 物流依存 + 危険部品の偏在を地理的に判断")

# ── KPI (仕様書§11) ──
k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("倉庫数", f'{len(wh_df)}拠点')
k2.metric("平均健全性", f'{wh_df["health_score"].mean():.1f}%')
danger_wh = len(wh_df[wh_df["health_score"]<50])
k3.metric("危険倉庫", f'{danger_wh}拠点', delta="要対応" if danger_wh>0 else None, delta_color="inverse")
k4.metric("ZERO+UNDER品目", f'{int(wh_df["zero_count"].sum()+wh_df["under_count"].sum())}')
k5.metric("OVER品目", f'{int(wh_df["over_count"].sum())}')

st.markdown("---")

# ── マップ + サイドパネル ──
show_routes = st.toggle("📍 物流ルート表示 (依存分析時のみON)", value=False)

left, right = st.columns([5,2])
with left:
    map_df = wh_df.rename(columns={"geo_lat":"latitude","geo_lon":"longitude",
        "managed_count":"component_count","zero_count":"below_safety_count",
        "critical_count":"critical_items","high_count":"high_items"})
    for c in ["total_stock_qty","total_stock_value_jpy","medium_items","incoming_shipments","incoming_qty","delayed_shipments"]:
        if c not in map_df.columns: map_df[c] = 0
    render_japan_map(map_df, routes_df=routes, height=500, show_routes=show_routes)

with right:
    st.markdown("#### 倉庫ステータス")
    for _, w in wh_df.sort_values("health_score").iterrows():
        score = w["health_score"]
        zero = int(w.get("zero_count",0)); under = int(w.get("under_count",0)); over = int(w.get("over_count",0))
        color = "#ff4646" if score<40 else "#ffa000" if score<70 else "#2ea043"
        wid = w["warehouse_id"]
        sel = wid in st.session_state.selected_warehouses
        label = f"{'●' if sel else '○'} {w['warehouse_name']} — {score:.0f}% | Z:{zero} U:{under} O:{over}"
        if st.button(label, key=f"ws_{wid}", use_container_width=True):
            if wid in st.session_state.selected_warehouses:
                st.session_state.selected_warehouses.remove(wid)
            else:
                st.session_state.selected_warehouses.append(wid)
            st.rerun()

    if st.session_state.selected_warehouses:
        if st.button("選択クリア"): st.session_state.selected_warehouses = []; st.rerun()

st.markdown("---")

# ── 選択倉庫の詳細表示 ──
if st.session_state.selected_warehouses:
    sel_wh = wh_df[wh_df["warehouse_id"].isin(st.session_state.selected_warehouses)]
    names = ", ".join(sel_wh["warehouse_name"].values)
    st.markdown(f"### 選択中: {names}")

    # KPI
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("管理品目", f'{int(sel_wh["managed_count"].sum())}')
    s2.metric("安全在庫割れ (UNDER)", f'{int(sel_wh["under_count"].sum())}品目')
    s3.metric("過剰在庫 (OVER)", f'{int(sel_wh["over_count"].sum())}品目')
    s4.metric("平均健全性", f'{sel_wh["health_score"].mean():.1f}%')

    # 選択倉庫の在庫一覧
    inv_cur = pd.read_csv(Path(__file__).parent.parent / "sample_data" / "inventory_current.csv")
    comp_df = pd.read_csv(Path(__file__).parent.parent / "sample_data" / "components.csv")

    wh_inv = inv_cur[inv_cur["warehouse_id"].isin(st.session_state.selected_warehouses)]
    wh_inv = wh_inv.merge(comp_df[["component_id","part_number","component_name","component_category","min_stock","max_stock"]], on="component_id", how="left")

    # 在庫状態を判定
    total_stock = inv_cur.groupby("component_id")["stock_qty"].sum().to_dict()
    wh_inv["total_stock"] = wh_inv["component_id"].map(total_stock).fillna(0).astype(int)
    wh_inv["status"] = wh_inv.apply(lambda r:
        "ZERO" if r["total_stock"]<=0 else
        "UNDER" if r["total_stock"]<r.get("min_stock",0) else
        "OVER" if r["total_stock"]>r.get("max_stock",99999) else "OK", axis=1)

    show = wh_inv[["part_number","component_name","component_category","stock_qty","total_stock","min_stock","max_stock","status"]].rename(columns={
        "part_number":"品番","component_name":"部品名","component_category":"カテゴリ",
        "stock_qty":"この倉庫の在庫","total_stock":"全倉庫合計","min_stock":"安全在庫(min)","max_stock":"上限(max)","status":"状態",
    }).sort_values("状態")

    st.dataframe(show.style.format({"この倉庫の在庫":"{:,}","全倉庫合計":"{:,}","安全在庫(min)":"{:,}","上限(max)":"{:,}"}),
                 use_container_width=True, height=min(400, len(show)*35+40), hide_index=True)

    # GeoGenie
    genie = get_genie_client()
    if geo_q := st.chat_input("選択倉庫群について質問...", key="geo_chat"):
        result = genie.query(geo_q, context={"warehouse_ids": st.session_state.selected_warehouses})
        st.write(result["text"])

# ── 倉庫別分布チャート ──
st.markdown("### 倉庫別 ZERO/UNDER/OVER 分布")
fig = go.Figure()
fig.add_trace(go.Bar(name="ZERO", x=wh_df["warehouse_name"], y=wh_df["zero_count"], marker_color="#ff4646", opacity=0.85))
fig.add_trace(go.Bar(name="UNDER", x=wh_df["warehouse_name"], y=wh_df["under_count"], marker_color="#ffa000", opacity=0.85))
fig.add_trace(go.Bar(name="OVER", x=wh_df["warehouse_name"], y=wh_df["over_count"], marker_color="#58a6ff", opacity=0.85))
fig.add_trace(go.Scatter(name="健全性(%)", x=wh_df["warehouse_name"], y=wh_df["health_score"],
    mode="markers+lines", marker=dict(color="#2ea043",size=6), line=dict(color="#2ea043",width=2,dash="dot"), yaxis="y2"))
fig.update_layout(barmode="stack", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#c9d1d9"), legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(l=0,r=0,t=8,b=0), height=280,
    xaxis=dict(gridcolor="#30363d"), yaxis=dict(title="件数",gridcolor="#30363d"),
    yaxis2=dict(title="健全性(%)",overlaying="y",side="right",range=[0,110]), hovermode="x unified")
st.plotly_chart(fig, use_container_width=True)
