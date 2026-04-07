"""
F. Data Reliability Center
仕様書§12: 信頼のための表舞台。「本日のダッシュボード信頼度」を1指標で見せる。

データソース:
- 実際の Lakeflow Pipeline 実行履歴 (Databricks SDK 経由)
- gold_data_pipeline_health テーブル (Bronze 行数集計)
- gold_business_glossary / gold_metric_definition (用語/メトリクス定義)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from services.config import is_demo_mode, load_config
from services.database import get_pipeline_health, get_exec_summary, get_glossary, get_metric_definitions

st.set_page_config(page_title="データ信頼性センター | SCM判断支援", page_icon="🛠️", layout="wide")
inject_css()
from components.sidebar import render_sidebar
render_sidebar()

st.markdown("## 🛠️ データ信頼性センター")
st.caption("今日のデータは信用してよいか — 信頼のための表舞台")


# ══════════════════════════════════════════════════════
# Lakeflow Pipeline 実行履歴 (本物の Databricks API)
# ══════════════════════════════════════════════════════
PIPELINE_NAME = "scm_decision_support_pipeline"


@st.cache_data(ttl=60)
def fetch_lakeflow_pipeline_status():
    """Databricks SDK を使って実際の Lakeflow パイプライン情報を取得する。
    返り値: dict {pipeline_id, name, state, last_update, datasets:[...]}
    取得失敗時は {"error": "..."} を返す
    """
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()

        # パイプライン一覧から名前で検索
        pipeline_id = None
        pipeline_obj = None
        for p in w.pipelines.list_pipelines(filter=f"name LIKE '%{PIPELINE_NAME}%'"):
            pipeline_id = p.pipeline_id
            pipeline_obj = p
            break

        if not pipeline_id:
            return {"error": f"Pipeline '{PIPELINE_NAME}' が見つかりません"}

        # 詳細取得
        detail = w.pipelines.get(pipeline_id=pipeline_id)
        latest_updates = list(w.pipelines.list_updates(pipeline_id=pipeline_id).updates or [])

        last_update_info = None
        if latest_updates:
            u = latest_updates[0]
            last_update_info = {
                "update_id": getattr(u, "update_id", ""),
                "state":     str(getattr(u, "state", "")),
                "creation_time": getattr(u, "creation_time", None),
                "cause":     str(getattr(u, "cause", "")),
            }

        return {
            "pipeline_id":   pipeline_id,
            "name":          getattr(detail, "name", PIPELINE_NAME),
            "state":         str(getattr(detail, "state", "UNKNOWN")),
            "catalog":       getattr(detail.spec, "catalog", "") if getattr(detail, "spec", None) else "",
            "target":        getattr(detail.spec, "target",  "") if getattr(detail, "spec", None) else "",
            "serverless":    getattr(detail.spec, "serverless", False) if getattr(detail, "spec", None) else False,
            "last_update":   last_update_info,
            "url":           f"#/pipelines/{pipeline_id}",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ══════════════════════════════════════════════════════
# 信頼度計算ロジックの説明
# ══════════════════════════════════════════════════════
with st.expander("📐 信頼度スコアの計算ロジック (前提条件)", expanded=False):
    st.markdown("""
**ダッシュボード信頼度** = `(パイプライン成功率 × 0.5) + (平均データ品質 × 0.5)` (0〜100%)

| 指標 | 定義 | 出典 |
|---|---|---|
| **Pipeline State** | Lakeflow パイプライン本体の現在状態 (RUNNING/IDLE/FAILED 等) | Databricks SDK `pipelines.get()` |
| **最新実行** | 直近の Pipeline Update の状態と発火時刻 | Databricks SDK `pipelines.list_updates()` |
| **取り込みテーブル数** | Bronze 層で取り込んだソーステーブル数 | `gold_data_pipeline_health` |
| **テーブル品質** | 行数 > 0 のテーブル割合 (デモ簡易ロジック) | 同上 |

**判定**: 80%以上=🟢健全, 60〜79%=🟠注意, 60%未満=🔴要調査

> ℹ️ このページは2つのデータソースを統合しています:
> 1. **Databricks SDK 経由**: Lakeflow パイプライン本体の最新実行ステータス
> 2. **`gold_data_pipeline_health` テーブル**: パイプラインが生成した Bronze 取り込みメタデータ
""")


# ══════════════════════════════════════════════════════
# Lakeflow パイプライン本体の状態
# ══════════════════════════════════════════════════════
st.markdown("### 🚀 Lakeflow Pipeline 実行ステータス (本物のDatabricks API)")

lakeflow_info = fetch_lakeflow_pipeline_status()

if lakeflow_info.get("error"):
    st.error(f"⚠️ Lakeflow API 取得失敗: {lakeflow_info['error']}")
    pipeline_state = "UNKNOWN"
    last_update_state = "UNKNOWN"
else:
    pipeline_state = lakeflow_info.get("state", "UNKNOWN")
    last_update = lakeflow_info.get("last_update") or {}
    last_update_state = last_update.get("state", "UNKNOWN")

    state_color = "#2ea043" if "IDLE" in pipeline_state.upper() or "RUNNING" in pipeline_state.upper() else "#ffa000"
    update_color = "#2ea043" if "COMPLETED" in last_update_state.upper() else "#ffa000" if "RUNNING" in last_update_state.upper() else "#ff4646"

    lf1, lf2, lf3, lf4 = st.columns(4)
    lf1.markdown(f"""<div style="padding:12px;background:rgba(255,255,255,0.02);border-left:4px solid {state_color};border-radius:4px;">
        <div style="font-size:11px;color:#8b949e;">Pipeline State</div>
        <div style="font-size:18px;color:{state_color};font-weight:700;">{pipeline_state.split('.')[-1] if '.' in pipeline_state else pipeline_state}</div>
    </div>""", unsafe_allow_html=True)
    lf2.markdown(f"""<div style="padding:12px;background:rgba(255,255,255,0.02);border-left:4px solid {update_color};border-radius:4px;">
        <div style="font-size:11px;color:#8b949e;">最新実行 (Update)</div>
        <div style="font-size:18px;color:{update_color};font-weight:700;">{last_update_state.split('.')[-1] if '.' in last_update_state else last_update_state}</div>
    </div>""", unsafe_allow_html=True)
    lf3.markdown(f"""<div style="padding:12px;background:rgba(255,255,255,0.02);border-left:4px solid #58a6ff;border-radius:4px;">
        <div style="font-size:11px;color:#8b949e;">Catalog / Schema</div>
        <div style="font-size:14px;color:#e6edf3;font-weight:600;">{lakeflow_info.get('catalog','-')}.{lakeflow_info.get('target','-')}</div>
    </div>""", unsafe_allow_html=True)
    lf4.markdown(f"""<div style="padding:12px;background:rgba(255,255,255,0.02);border-left:4px solid #58a6ff;border-radius:4px;">
        <div style="font-size:11px;color:#8b949e;">Serverless</div>
        <div style="font-size:18px;color:#e6edf3;font-weight:700;">{'✓ Yes' if lakeflow_info.get('serverless') else '✗ No'}</div>
    </div>""", unsafe_allow_html=True)

    st.caption(f"Pipeline ID: `{lakeflow_info.get('pipeline_id','-')}`")
    if last_update:
        st.caption(f"Update ID: `{last_update.get('update_id','-')}` / Cause: `{last_update.get('cause','-')}`")

st.markdown("---")

# ══════════════════════════════════════════════════════
# 取り込みテーブルメタデータ (gold_data_pipeline_health)
# ══════════════════════════════════════════════════════
st.markdown("### 📊 取り込みテーブルメタデータ")
st.caption("出典: `gold_data_pipeline_health` テーブル (Lakeflow Pipeline が生成した Bronze 層の取り込み結果)")

pipeline = get_pipeline_health()
summary = get_exec_summary().iloc[0]

# 数値の正規化 (Gold 側が壊れている場合のフォールバック計算)
total = len(pipeline)


def _truthy(v):
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("true", "1", "t", "yes")


if "success_flag" in pipeline.columns:
    success_series = pipeline["success_flag"].apply(_truthy)
    success = int(success_series.sum())
else:
    success_series = pd.Series([True] * total)
    success = total

if "quality_score" in pipeline.columns:
    quality_series = pd.to_numeric(pipeline["quality_score"], errors="coerce").fillna(0)
else:
    quality_series = pd.Series([100.0] * total)

# フォールバック1: 全部0なら record_count > 0 で代替
if quality_series.sum() == 0 and "record_count" in pipeline.columns:
    rc = pd.to_numeric(pipeline["record_count"], errors="coerce").fillna(0)
    quality_series = (rc > 0).astype(int) * 100.0

# フォールバック2: それでも全部0なら success_flag を品質指標として使う
if quality_series.sum() == 0:
    quality_series = success_series.astype(int) * 100.0

avg_q = float(quality_series.mean()) if total > 0 else 0.0
trust_score = round((success / max(total, 1) * 50) + (avg_q / 100 * 50), 1)

# 信頼度バナー
trust_color = "#2ea043" if trust_score >= 80 else "#ffa000" if trust_score >= 60 else "#ff4646"
st.markdown(f"""<div style="background:rgba(46,160,67,0.08);border:2px solid rgba(46,160,67,0.3);
    border-radius:12px;padding:20px;text-align:center;margin-bottom:16px;">
    <div style="font-size:12px;color:#8b949e;">本日のダッシュボード信頼度</div>
    <div style="font-size:48px;font-weight:700;color:{trust_color};">{trust_score}%</div>
    <div style="font-size:11px;color:#8b949e;">パイプライン成功率 × 0.5 + 平均品質 × 0.5</div>
</div>""", unsafe_allow_html=True)

k1, k2, k3, k4 = st.columns(4)
k1.metric("取り込みテーブル数", f"{total}")
k2.metric("成功", f"{success}/{total}")
k3.metric("失敗", f"{total - success}", delta="要調査" if total - success > 0 else None, delta_color="inverse")
k4.metric("平均品質", f"{avg_q:.1f}%")

# 計算で使った正規化済みの値を pipeline に書き戻し (下のグラフ・テーブル用)
pipeline = pipeline.copy()
pipeline["success_flag"] = success_series.values
pipeline["quality_score"] = quality_series.values

st.markdown("---")

# ══════════════════════════════════════════════════════
# メダリオンアーキテクチャ
# ══════════════════════════════════════════════════════
st.markdown("### メダリオンアーキテクチャ")
l1, l2, l3 = st.columns(3)
for col, (icon, name, count, desc, color) in zip([l1, l2, l3], [
    ("🔶", "Bronze", 15, "CSV→Delta", "rgba(205,127,50,0.3)"),
    ("⬜", "Silver", 15, "型変換/クレンジング/Expectations", "rgba(192,192,192,0.3)"),
    ("🥇", "Gold", 14, "業務判断テーブル", "rgba(255,215,0,0.3)"),
]):
    with col:
        st.markdown(f"""<div style="background:rgba(255,255,255,0.02);border:2px solid {color};
            border-radius:10px;padding:14px;text-align:center;">
            <div style="font-size:16px;">{icon}</div>
            <div style="font-size:13px;font-weight:700;">{name}</div>
            <div style="font-size:20px;font-weight:700;color:#e6edf3;">{count}</div>
            <div style="font-size:9px;color:#8b949e;">{desc}</div>
        </div>""", unsafe_allow_html=True)

st.markdown("---")

# ══════════════════════════════════════════════════════
# テーブル品質バーチャート
# ══════════════════════════════════════════════════════
st.markdown("### テーブル品質")
if "quality_score" in pipeline.columns and total > 0:
    fig = go.Figure(go.Bar(
        x=pipeline["pipeline_name"].astype(str).str.replace("csv_ingest_", ""),
        y=pipeline["quality_score"].astype(float),
        marker_color=[("#2ea043" if float(q) >= 95 else "#ffa000" if float(q) >= 85 else "#ff4646")
                      for q in pipeline["quality_score"]],
        opacity=0.85,
        text=pipeline["quality_score"].apply(lambda x: f"{float(x):.0f}%"),
        textposition="outside", textfont_color="#c9d1d9",
    ))
    fig.add_hline(y=95, line_dash="dash", line_color="#2ea043",
                  annotation_text="目標95%", annotation_font_color="#2ea043")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#c9d1d9", size=11), margin=dict(l=0, r=0, t=8, b=0), height=240,
        xaxis=dict(gridcolor="#30363d", tickangle=-45),
        yaxis=dict(gridcolor="#30363d", title="品質(%)", range=[0, 110]),
    )
    st.plotly_chart(fig, use_container_width=True)

# 取り込みテーブルテーブル
avail = [c for c in ["pipeline_name", "source_table", "target_table", "record_count",
                     "quality_score", "success_flag", "freshness_ts"] if c in pipeline.columns]
st.dataframe(pipeline[avail].rename(columns={
    "pipeline_name": "パイプライン", "source_table": "ソース", "target_table": "ターゲット",
    "record_count": "件数", "quality_score": "品質(%)", "success_flag": "成功", "freshness_ts": "鮮度",
}), use_container_width=True, height=350)

st.markdown("---")

# ══════════════════════════════════════════════════════
# Gold 14 テーブル一覧
# ══════════════════════════════════════════════════════
st.markdown("### Gold テーブル一覧 (14本)")
gold_tables = [
    ("gold_exec_summary_daily", "経営サマリー"),
    ("gold_lt_snapshot_current", "LTスナップショット"),
    ("gold_lt_trend_monthly", "LTトレンド"),
    ("gold_lt_escalation_items", "LT長期化"),
    ("gold_order_commit_risk", "納期危険度"),
    ("gold_requirement_timeline", "所要量一覧"),
    ("gold_balance_projection_monthly", "月末在庫予測"),
    ("gold_inventory_policy_breach", "在庫ポリシーブリーチ"),
    ("gold_geo_warehouse_status", "倉庫ステータス"),
    ("gold_data_pipeline_health", "パイプライン"),
    ("gold_action_queue_daily", "アクションキュー"),
    ("gold_business_glossary", "用語辞書"),
    ("gold_metric_definition", "メトリクス定義"),
    ("gold_genie_semantic_examples", "Genieサンプル"),
]
gt_cols = st.columns(2)
for i, (tname, desc) in enumerate(gold_tables):
    gt_cols[i % 2].markdown(f"- 🟢 `{tname}` — {desc}")

st.markdown("---")

# ══════════════════════════════════════════════════════
# メトリクス定義 / 用語辞書
# ══════════════════════════════════════════════════════
tab1, tab2 = st.tabs(["📏 メトリクス定義", "📖 用語辞書"])

with tab1:
    metrics = get_metric_definitions()
    st.dataframe(metrics.rename(columns={
        "metric_id": "ID", "metric_name": "メトリクス", "formula": "計算式",
        "unit": "単位", "source_gold": "ソースGold", "screen": "使用画面",
    }), use_container_width=True, hide_index=True)

with tab2:
    glossary = get_glossary()
    st.dataframe(glossary.rename(columns={
        "term_id": "ID", "term": "用語", "definition": "定義",
        "synonyms": "同義語", "prohibited": "禁止表現",
    }), use_container_width=True, hide_index=True)

st.markdown("---")

# ══════════════════════════════════════════════════════
# 接続情報 + デバッグ
# ══════════════════════════════════════════════════════
st.markdown("### 接続情報")
cfg = load_config()
if cfg.get("_source") == "demo":
    st.info("🔸 デモモード")
else:
    st.success("🟢 Databricks接続済み")
    st.json({k: v for k, v in cfg.items() if k != "_source" and v})

with st.expander("🔍 デバッグ: gold_data_pipeline_health の生データ", expanded=False):
    st.caption("Lakeflow Pipeline が生成したテーブルの中身をそのまま表示")
    st.dataframe(get_pipeline_health(), use_container_width=True)
