"""
B. Lead Time Intelligence
仕様書§8: 長期化傾向の発見 + 要対応候補の特定
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from services.database import get_lt_snapshot, get_lt_trend, get_lt_escalation
from components.global_filter import render_global_filter, apply_filters
from components.explain_panel import render_explain

st.set_page_config(page_title="LTインテリジェンス | SCM判断支援", page_icon="📊", layout="wide")
inject_css()
from components.sidebar import render_sidebar
render_sidebar()

snapshot = get_lt_snapshot()
trend = get_lt_trend()
escalation = get_lt_escalation()

for col in ["latest_lt_weeks","lt_n3_weeks","lt_n6_weeks"]:
    if col in snapshot.columns:
        snapshot[col] = pd.to_numeric(snapshot[col], errors="coerce")
if "lead_time_weeks" in trend.columns:
    trend["lead_time_weeks"] = pd.to_numeric(trend["lead_time_weeks"], errors="coerce")
if "effective_date" in trend.columns:
    trend["effective_date"] = pd.to_datetime(trend["effective_date"], errors="coerce", format="mixed")


def _classify_lt_band(weeks):
    """LT バンドを週数から計算 (Gold テーブルに lt_band が無い場合のフォールバック)"""
    if pd.isna(weeks):
        return None
    w = float(weeks)
    if w <= 13:  return "13週以内"
    if w <= 26:  return "14週〜半年"
    if w <= 52:  return "半年〜1年"
    if w <= 78:  return "1年〜1.5年"
    return "1.5年〜2年"


# Gold 側で lt_band が NULL/空のケースに備え、ここで再計算で上書き保証
if "latest_lt_weeks" in snapshot.columns:
    snapshot["lt_band"] = snapshot["latest_lt_weeks"].apply(_classify_lt_band)

BAND_COLORS = {"13週以内":"#2ea043","14週〜半年":"#58a6ff","半年〜1年":"#ffa000",
               "1年〜1.5年":"#f78166","1.5年〜2年":"#ff4646"}

st.markdown("## 📊 LTインテリジェンス")
st.caption("長期化傾向の発見 → 要対応候補の特定 → 発注タイミングへの影響評価")

# ── Global Filter ─────────────────────────────
makers = sorted(snapshot["manufacturer_name"].dropna().unique()) if "manufacturer_name" in snapshot.columns else []
cats = sorted(snapshot["component_category"].dropna().unique()) if "component_category" in snapshot.columns else []
filters = render_global_filter(manufacturers=makers, categories=cats, show_priority=False, show_scope=False)
df = apply_filters(snapshot, filters, manufacturer_col="manufacturer_name", category_col="component_category")

# ── KPI (仕様書§8: LT長期化/16週超/26週超/平均LT/中央値LT/変動幅大) ──
esc_count = len(df[(df.get("trend_arrow_n3",pd.Series())=="↑") | (df.get("trend_arrow_n6",pd.Series())=="↑")])
over16 = int((df["latest_lt_weeks"]>16).sum()) if "latest_lt_weeks" in df.columns else 0
over26 = int((df["latest_lt_weeks"]>26).sum()) if "latest_lt_weeks" in df.columns else 0
avg_lt = round(df["latest_lt_weeks"].mean(),1) if "latest_lt_weeks" in df.columns else 0
median_lt = round(df["latest_lt_weeks"].median(),1) if "latest_lt_weeks" in df.columns else 0

k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("LT長期化", f"{esc_count}品目", delta="要注意" if esc_count>0 else None, delta_color="inverse")
k2.metric("16週超", f"{over16}品目")
k3.metric("26週超", f"{over26}品目")
k4.metric("平均LT", f"{avg_lt}週")
k5.metric("中央値LT", f"{median_lt}週")

st.markdown("---")

# ── LT比較テーブル (全幅) ──
st.markdown("### LT比較テーブル")
st.caption("N-3/N-6比較 | ↑延長 ↓短縮 →変化なし | 推奨発注タイミング影響")

def fmt_w(v):
    if pd.isna(v): return ""
    return f"{int(v)}週"

df_sorted = df.copy()
df_sorted["_sort"] = df_sorted.apply(lambda r: 0 if r.get("trend_arrow_n3")=="↑" or r.get("trend_arrow_n6")=="↑"
                       else 2 if r.get("trend_arrow_n3")=="↓" else 1, axis=1)
df_sorted = df_sorted.sort_values(["_sort","latest_lt_weeks"], ascending=[True,False])

rows = []
for _, r in df_sorted.iterrows():
    rows.append({
        "品番": r.get("item_code",""), "部品名": r.get("item_name",""),
        "メーカー": r.get("manufacturer_name",""),
        "現在LT": fmt_w(r.get("latest_lt_weeks")),
        "N-6": fmt_w(r.get("lt_n6_weeks")), "N-3": fmt_w(r.get("lt_n3_weeks")),
        "6ヶ月比": r.get("trend_arrow_n6",""), "3ヶ月比": r.get("trend_arrow_n3",""),
        "備考": r.get("remark",""),
    })
st.dataframe(pd.DataFrame(rows), use_container_width=True, height=450, hide_index=True)

st.markdown("---")

# ── メーカー別LTバンド分布 (全幅) ──
st.markdown("### メーカー別LTバンド分布")
if "lt_band" in df.columns and "manufacturer_name" in df.columns:
    band_data = df.groupby(["manufacturer_name","lt_band"]).size().reset_index(name="count")
    maker_list = sorted(band_data["manufacturer_name"].dropna().unique())
    fig = go.Figure()
    for band in ["13週以内","14週〜半年","半年〜1年","1年〜1.5年","1.5年〜2年"]:
        sub = band_data[band_data["lt_band"]==band]
        counts = [int(sub[sub["manufacturer_name"]==m]["count"].sum()) for m in maker_list]
        fig.add_trace(go.Bar(name=band, x=maker_list, y=counts,
                             marker_color=BAND_COLORS.get(band,"#8b949e"), opacity=0.85))
    fig.update_layout(barmode="stack", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#c9d1d9",size=10), legend=dict(bgcolor="rgba(0,0,0,0)",orientation="h",y=1.05),
        margin=dict(l=0,r=0,t=20,b=0), height=350,
        xaxis=dict(gridcolor="#30363d",tickangle=-30), yaxis=dict(gridcolor="#30363d",title="品目数"))
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── LTトレンド (フィルターベース) ──
st.markdown("### LTトレンド (複数品目比較)")
parts = sorted(df["item_code"].dropna().unique()) if "item_code" in df.columns else []
sel = st.multiselect("表示する部品", parts, default=parts[:3] if len(parts)>=3 else parts)

if sel and not trend.empty:
    code_to_id = df.set_index("item_code")["item_id"].to_dict() if "item_id" in df.columns else {}
    fig = go.Figure()
    colors = ["#58a6ff","#ffa000","#2ea043","#ff4646","#bc8cff","#f78166","#39d353"]
    for i, code in enumerate(sel):
        cid = code_to_id.get(code)
        if not cid: continue
        ct = trend[trend["component_id"]==cid].sort_values("effective_date")
        if len(ct)==0: continue
        fig.add_trace(go.Scatter(x=ct["effective_date"], y=ct["lead_time_weeks"],
            mode="lines+markers", name=code, line=dict(color=colors[i%len(colors)],width=2), marker=dict(size=3)))
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#c9d1d9"), legend=dict(bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=0,r=0,t=8,b=0), height=300,
        xaxis=dict(gridcolor="#30363d"), yaxis=dict(gridcolor="#30363d",title="LT(週)"), hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── LT長期化: 影響確認への導線 ──
st.markdown("### LT長期化品目の影響確認")
if len(escalation) > 0:
    esc_f = apply_filters(escalation, filters, manufacturer_col="manufacturer_name", category_col="component_category")
    st.warning(f"⚠ {len(esc_f)}品目のLTが延長傾向です。納期への影響を確認してください。")

    show_esc = esc_f[["item_code","item_name","manufacturer_name","latest_lt_weeks",
                       "trend_arrow_n3","trend_arrow_n6","escalation_reason"]].head(10).rename(columns={
        "item_code":"品番","item_name":"部品名","manufacturer_name":"メーカー",
        "latest_lt_weeks":"現在LT(週)","trend_arrow_n3":"3ヶ月比","trend_arrow_n6":"6ヶ月比",
        "escalation_reason":"延長理由",
    })
    st.dataframe(show_esc, use_container_width=True, hide_index=True)

    st.page_link("pages/2_commit_supply_balance.py", label="⚖️ 納期コミット・需給バランスで影響を確認 →")
else:
    st.success("LT長期化アイテムなし")
