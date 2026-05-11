"""
🔧 データパイプライン健全性（データエンジニア向け）
=====================================================
業務上の役割:
  - Bronze/Silver/Gold データパイプラインの最終更新時刻・行数・品質を一覧
  - データソースごとの取り込み件数を表示し、データ欠損を早期検出
  - 担当データエンジニア／運用担当が日次でチェックする画面

Phase 7 改修:
  ✅ 構成図を「カードレイアウト」と「Plotly Sankey」で再可視化
  ✅ ASCII図を廃止
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
from services.config import get_as_of_date
from services.plot_theme import base_layout, get_theme_tokens
from services.database import get_pipeline_health

st.set_page_config(page_title="パイプライン健全性 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="データパイプラインの日次更新状況とデータ品質")

t = get_theme_tokens()

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    ph = get_pipeline_health()

today = get_as_of_date()

st.markdown("## 🔧 データパイプライン健全性")
st.caption(
    "Bronze/Silver/Gold 三層パイプラインの最新ステータス、行数、品質スコア。"
    "データ品質が下がっていればバッチログを確認してください。"
)

if ph.empty:
    st.warning("gold_data_pipeline_health が空です。Lakeflow パイプラインを実行してください。")
    st.stop()

ph = ph.copy()
ph["record_count"] = pd.to_numeric(ph["record_count"], errors="coerce").fillna(0).astype(int)
ph["quality_score"] = pd.to_numeric(ph["quality_score"], errors="coerce").fillna(0)
ph["success_flag"] = ph["success_flag"].astype(str).str.lower().isin(["true", "1", "t"])

# ────────────────────────────────────────────────────────
# KPI
# ────────────────────────────────────────────────────────
n_total = len(ph)
n_success = int(ph["success_flag"].sum())
n_failed = n_total - n_success
total_rows = int(ph["record_count"].sum())
avg_quality = float(ph["quality_score"].mean()) if not ph.empty else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("📊 監視中パイプライン", f"{n_total} 本")
k2.metric("✅ 成功", f"{n_success} 本")
k3.metric("❌ 失敗", f"{n_failed} 本")
k4.metric("⭐ 平均品質スコア", f"{avg_quality:.1f}/100")

st.markdown("---")

# ────────────────────────────────────────────────────────
# Phase 7: パイプライン構成図を「カードレイアウト」で可視化
# ────────────────────────────────────────────────────────
st.markdown("### 🗺️ パイプライン構成図（Bronze → Silver → Gold）")
st.caption(
    "Lakeflow Declarative Pipeline (DLT) で運用される三層構成。"
    "CSVを Volume にアップロードすると Bronze → Silver → Gold へと自動で伝播し、本Appが Gold/Silver を読みます。"
)

# テーマカラーで box style 生成
box_style = (
    f"background:{t['panel']};border:1px solid {t['border']};"
    f"border-radius:10px;padding:14px 18px;"
)
arrow_style = (
    f"font-size:22px;color:{t['text_muted']};text-align:center;"
    f"line-height:1.0;margin:8px 0;"
)
header_style = (
    f"font-size:13px;font-weight:700;color:{t['blue']};"
    f"letter-spacing:0.5px;margin-bottom:6px;text-transform:uppercase;"
)
sub_style = f"font-size:11px;color:{t['text_sub']};line-height:1.5;"

c1, c2, c3 = st.columns(3)
with c1:
    st.markdown(
        f"""
        <div style="{box_style}">
            <div style="{header_style}">① Bronze</div>
            <div style="font-size:14px;font-weight:600;color:{t['text']};">CSV → 生テーブル</div>
            <div style="{sub_style}">
                Volumes/scm_data/csv/*.csv を Auto Loader で取り込み。<br>
                型未変換・null許容のまま。
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c2:
    st.markdown(
        f"""
        <div style="{box_style}">
            <div style="{header_style}">② Silver</div>
            <div style="font-size:14px;font-weight:600;color:{t['text']};">クレンジング+ 結合</div>
            <div style="{sub_style}">
                型変換、外部キー検証、part_number等の結合、派生列追加。<br>
                @dlt.expect でデータ品質を検証。
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c3:
    st.markdown(
        f"""
        <div style="{box_style}">
            <div style="{header_style}">③ Gold</div>
            <div style="font-size:14px;font-weight:600;color:{t['text']};">業務集計テーブル</div>
            <div style="{sub_style}">
                gold_procurement_options / gold_bom_fulfillment_status<br>
                gold_balance_projection_monthly 他、本Appが直接参照。
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# 矢印
st.markdown(f"<div style='{arrow_style}'>↓ DLT pipeline</div>", unsafe_allow_html=True)

# 出力先
c1, c2 = st.columns(2)
with c1:
    st.markdown(
        f"""
        <div style="{box_style};border-left:3px solid {t['green']};">
            <div style="{header_style}">📊 Streamlit App</div>
            <div style="{sub_style}">本アプリ。Gold/Silver から SQL Warehouse 経由で読込。</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c2:
    st.markdown(
        f"""
        <div style="{box_style};border-left:3px solid {t['purple']};">
            <div style="{header_style}">💬 Genie Space</div>
            <div style="{sub_style}">自然言語で Gold/Silver にクエリ可能。物流ページで利用。</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("---")

# ────────────────────────────────────────────────────────
# パイプライン別 取り込み件数 (バーチャート)
# ────────────────────────────────────────────────────────
st.markdown("### 📊 パイプライン別 取り込みレコード数")
chart_df = ph.sort_values("record_count", ascending=True)

fig = go.Figure()
fig.add_trace(go.Bar(
    x=chart_df["record_count"],
    y=chart_df["pipeline_name"],
    orientation="h",
    marker=dict(color=[t["green"] if s else t["red"] for s in chart_df["success_flag"]], line=dict(width=0)),
    text=chart_df["record_count"].apply(lambda v: f"{v:,}"),
    textposition="outside",
    textfont=dict(color=t["text"], size=11),
    hovertemplate="%{y}<br>レコード数: %{x:,}<extra></extra>",
))
fig.update_layout(
    **base_layout(height=520, x_title="レコード数", y_title="", show_legend=False),
)
fig.update_layout(margin=dict(l=240, r=40, t=26, b=44))
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ────────────────────────────────────────────────────────
# パイプライン一覧
# ────────────────────────────────────────────────────────
st.markdown("### 📋 パイプライン詳細一覧")
st.caption("各 Bronze テーブルの取り込み結果と品質スコア。失敗があれば error_message を確認してください。")

show_cols = [
    ("pipeline_name",  "パイプライン名"),
    ("source_table",   "ソース"),
    ("target_table",   "出力テーブル"),
    ("record_count",   "レコード数"),
    ("quality_score",  "品質スコア"),
    ("success_flag",   "成功"),
    ("freshness_ts",   "最終更新"),
    ("error_message",  "エラー"),
]
cols_present = [(k, v) for k, v in show_cols if k in ph.columns]
ph_show = ph[[k for k, _ in cols_present]].rename(columns=dict(cols_present))
st.dataframe(ph_show, hide_index=True, use_container_width=True, height=520)

# ────────────────────────────────────────────────────────
# 運用フロー
# ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 🔄 運用フロー")
st.markdown(
    """
1. CSVを `/Volumes/{catalog}/{schema}/scm_data/csv/` にアップロード
2. Databricks Lakeflow Pipeline を手動またはスケジュール実行
3. パイプラインが Bronze → Silver → Gold まで自動更新
4. 本アプリは Gold/Silver テーブルから即時に画面更新（10分キャッシュ）

**異常検知**: 上記の「失敗 (赤色バー)」が出たらバッチログを `databricks pipelines get-update` で確認してください。
"""
)
