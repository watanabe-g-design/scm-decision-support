"""
🔧 データパイプライン健全性（データエンジニア向け）
=====================================================
業務上の役割:
  - Bronze/Silver/Gold データパイプラインの最終更新時刻・行数・品質を一覧
  - データソースごとの取り込み件数を表示し、データ欠損を早期検出
  - 担当データエンジニア／運用担当が日次でチェックする画面
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
from services.config import get_as_of_date
from services.database import get_pipeline_health

st.set_page_config(page_title="パイプライン健全性 | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="データパイプラインの日次更新状況とデータ品質")

colors = plot_colors()

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    ph = get_pipeline_health()

today = get_as_of_date()

st.markdown("## 🔧 データパイプライン健全性（データエンジニア向け）")
st.caption(
    "Bronze/Silver/Gold パイプラインの最新ステータスと、CSV取り込み・テーブル品質。"
    "データ品質スコアが下がっていたらバッチログを確認してください。"
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
# パイプライン別 取り込み件数 (バーチャート)
# ────────────────────────────────────────────────────────
st.markdown("### 📊 パイプライン別 取り込みレコード数")
chart_df = ph.sort_values("record_count", ascending=True)

fig = go.Figure()
fig.add_trace(go.Bar(
    x=chart_df["record_count"],
    y=chart_df["pipeline_name"],
    orientation="h",
    marker_color=[colors["green"] if s else colors["red"] for s in chart_df["success_flag"]],
    text=chart_df["record_count"].apply(lambda v: f"{v:,}"),
    textposition="auto",
))
fig.update_layout(
    height=520,
    plot_bgcolor=colors["bg"],
    paper_bgcolor=colors["paper"],
    font=dict(color=colors["text"], size=11),
    xaxis=dict(title="レコード数", gridcolor=colors["grid"]),
    yaxis=dict(title="", gridcolor=colors["grid"]),
    margin=dict(l=200, r=20, t=20, b=40),
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ────────────────────────────────────────────────────────
# パイプライン一覧
# ────────────────────────────────────────────────────────
st.markdown("### 📋 パイプライン詳細一覧")
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
# パイプライン構成図 (テキスト)
# ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 🗺️ データパイプライン構成図")
st.markdown(
    """
**Bronze → Silver → Gold** の3層構成。Lakeflow Declarative Pipeline (DLT) で運用。

```
[Volumes/csv/*.csv]
        ↓
   ┌────────────┐
   │  Bronze    │  生CSV → DLT auto_loader でテーブル化
   └────┬───────┘
        ↓ 型変換 / null チェック / 結合
   ┌────────────┐
   │  Silver    │  クレンジング済テーブル（business-ready）
   └────┬───────┘
        ↓ KPI集計 / 4ルート評価 / BOM充足判定
   ┌────────────┐
   │  Gold      │  ダッシュボード直結テーブル
   └────┬───────┘
        ↓
   [Streamlit App / Genie Space]
```

**運用フロー**:
1. CSVを `/Volumes/{catalog}/{schema}/scm_data/csv/` にアップロード
2. Databricks Lakeflow Pipeline を手動またはスケジュール実行
3. 本アプリは Gold/Silver テーブルから即時に画面更新
"""
)
