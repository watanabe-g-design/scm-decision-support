"""
A. Executive Control Tower
仕様書§7: 5秒で全体異常を掴む。グラフ10個以下。判断の起点。
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from services.config import is_demo_mode
from services.database import (
    get_exec_summary, get_action_queue, get_lt_escalation,
    get_inventory_breach, get_geo_warehouse,
)
from components.global_filter import render_global_filter
from components.explain_panel import render_explain, render_metric_explain
from services.genie_client import get_genie_client, SAMPLE_QUERIES

st.set_page_config(page_title="経営コントロールタワー | SCM判断支援", page_icon="📦", layout="wide")
inject_css()

if "genie_messages" not in st.session_state:
    st.session_state.genie_messages = []

# ── 共通サイドバー ────────────────────────────
from components.sidebar import render_sidebar
render_sidebar()

# ── データ ────────────────────────────────────
with st.spinner("データ読み込み中..."):
    summary = get_exec_summary().iloc[0]
    action_q = get_action_queue()
    lt_esc = get_lt_escalation()
    breach = get_inventory_breach()
    geo = get_geo_warehouse()

# ── タイトル ──────────────────────────────────
st.markdown("""<div class="title-bar">
    <span class="logo">📦</span>
    <div>
        <div class="title">経営コントロールタワー</div>
        <div class="subtitle">重大異常の存在 → 今すべきアクション → どこを見るべきか</div>
    </div>
</div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════
# ① KPIカード (8枚, 仕様書§7)
# ══════════════════════════════════════════
k1, k2, k3, k4 = st.columns(4)
k1.metric("🔴 Critical Orders", f'{int(summary.get("critical_count",0))}件', help="指定納期3日以内")
k2.metric("🟠 High Orders", f'{int(summary.get("high_count",0))}件', help="指定納期7日以内")
k3.metric("📊 LT Escalation", f'{int(summary.get("lt_escalation_item_count",0))}品目', help="N-3/N-6比で↑")
k4.metric("⚠️ 3M Policy Breaches", f'{int(summary.get("zero_count",0))+int(summary.get("under_count",0))+int(summary.get("over_count",0))}品目')

k5, k6, k7, k8 = st.columns(4)
k5.metric("🔴 Stockout予測", f'{int(summary.get("zero_count",0))}品目', help="3ヶ月以内にZERO")
k6.metric("🔵 Excess Inventory", f'{int(summary.get("over_count",0))}品目', help="max超過予測")
k7.metric("📈 FCST Accuracy", f'{summary.get("forecast_accuracy_pct",0):.1f}%')
k8.metric("🏭 Warehouse Health", f'{summary.get("warehouse_health_score",0):.1f}%')

st.markdown("---")

# ══════════════════════════════════════════
# ② 今週の優先アクションテーブル (仕様書§7)
# ══════════════════════════════════════════
st.markdown("### 今週の優先アクション")
st.caption("リスク種別 × 推奨アクション × 期限 × 根拠 | 根拠は1文で制約")

if len(action_q) > 0:
    show_aq = action_q[["risk_type","item_code","item_name","recommended_action",
                         "due_date","rationale","urgency_rank"]].head(15)
    show_aq = show_aq.rename(columns={
        "risk_type":"リスク種別","item_code":"対象","item_name":"部品名",
        "recommended_action":"推奨アクション","due_date":"期限",
        "rationale":"根拠","urgency_rank":"緊急度",
    })
    st.dataframe(show_aq, use_container_width=True, height=min(400, len(show_aq)*35+40), hide_index=True)

    # 各行のExplain Panel
    for _, r in action_q.head(5).iterrows():
        render_explain(
            title=f"{r.get('item_code','')} {r.get('item_name','')}",
            rationale=r.get("rationale",""),
            action=r.get("recommended_action",""),
            due=str(r.get("due_date","")),
            severity=r.get("risk_type",""),
        )

    st.page_link("pages/2_commit_supply_balance.py", label="⚖️ 全オーダー詳細へ →")
else:
    st.success("今週の優先アクションはありません")

st.markdown("---")

# ══════════════════════════════════════════
# ③ 下段: リスク分布 + 倉庫健全性 + FCST精度 + Top5 LT悪化
# ══════════════════════════════════════════
col_l, col_r = st.columns(2)

with col_l:
    # 倉庫健全性 (コンパクト)
    st.markdown("### 倉庫健全性")
    for _, w in geo.sort_values("health_score").iterrows():
        score = w["health_score"]
        color = "#ff4646" if score < 40 else "#ffa000" if score < 70 else "#2ea043"
        zero = int(w.get("zero_count",0))
        under = int(w.get("under_count",0))
        over = int(w.get("over_count",0))
        bar_w = max(5, min(100, score))
        st.markdown(f"""<div style="margin-bottom:3px;padding:5px 8px;background:rgba(255,255,255,0.02);
            border:1px solid #30363d;border-radius:4px;">
            <div style="display:flex;justify-content:space-between;">
                <span style="font-size:12px;">{w['warehouse_name']}</span>
                <span style="font-size:11px;color:{color};font-weight:600;">{score:.0f}%</span>
            </div>
            <div style="background:#30363d;border-radius:2px;height:4px;overflow:hidden;">
                <div style="background:{color};width:{bar_w}%;height:100%;"></div>
            </div>
            <div style="font-size:9px;color:#8b949e;">Z:{zero} U:{under} O:{over}</div>
        </div>""", unsafe_allow_html=True)

    st.page_link("pages/4_network_warehouse.py", label="🗺️ 拠点・倉庫健全性 →")

with col_r:
    # FCST精度推移
    st.markdown("### FCST精度推移")
    fc = pd.read_csv(Path(__file__).parent / "sample_data" / "forecasts.csv")
    fc["forecast_month"] = pd.to_datetime(fc["forecast_month"], format="mixed")
    fc["forecast_accuracy"] = pd.to_numeric(fc["forecast_accuracy"], errors="coerce")
    fc["_ym"] = fc["forecast_month"].dt.strftime("%Y-%m")
    fc_m = fc.groupby("_ym", as_index=False)["forecast_accuracy"].mean()
    fc_m.columns = ["month","accuracy"]
    fc_m["pct"] = (fc_m["accuracy"]*100).round(1)
    fc_m = fc_m.sort_values("month").tail(12)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=fc_m["month"], y=fc_m["pct"],
        mode="lines+markers", line=dict(color="#58a6ff",width=2.5), marker=dict(size=4),
        fill="tozeroy", fillcolor="rgba(88,166,255,0.06)"))
    fig.add_hline(y=85, line_dash="dash", line_color="#2ea043", annotation_text="目標85%", annotation_font_color="#2ea043")
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#c9d1d9"), margin=dict(l=0,r=0,t=8,b=0), height=220,
        xaxis=dict(gridcolor="#30363d",tickangle=-45), yaxis=dict(gridcolor="#30363d",title="精度(%)",range=[30,100]),
        showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # Top5 LT悪化
    st.markdown("### LT長期化 Top 5")
    if len(lt_esc) > 0:
        for _, r in lt_esc.head(5).iterrows():
            render_explain(
                title=f"{r.get('item_code','')} {r.get('item_name','')}",
                rationale=f"現在{r.get('latest_lt_weeks','')}週 ({r.get('escalation_reason','')})",
                action="発注タイミング前倒し検討",
                severity="LT長期化",
            )
    st.page_link("pages/1_lt_intelligence.py", label="📊 Lead Time Intelligence →")

st.markdown("---")

# ══════════════════════════════════════════
# ④ AI/Genie Panel (展開式 — 初期は閉じた状態)
# ══════════════════════════════════════════
with st.expander("🤖 SCM Genie — 画面の数字の意味を確認・深掘り", expanded=False):
    genie = get_genie_client()

    if not genie.is_available:
        st.warning(
            "Genie が未接続です。`SCM_GENIE_SPACE_ID` 環境変数を設定し、"
            "App のサービスプリンシパルに Genie スペースの 'Can run' 権限を付与してください。"
        )
    else:
        # ── モード切替 (single-turn / multi-turn リサーチ) ────
        mode_col, info_col = st.columns([2, 3])
        with mode_col:
            mode = st.radio(
                "🧭 回答モード",
                ["💡 1回回答 (Single-turn)", "🔬 リサーチ (Multi-turn)"],
                horizontal=False,
                key="genie_mode",
                help=(
                    "1回回答: 各質問を独立して処理 (高速)\n\n"
                    "リサーチ: 会話を継続し前の質問の文脈を引き継ぐ。深掘り質問に向く"
                ),
            )
        with info_col:
            is_research = "リサーチ" in mode
            if is_research:
                st.info(
                    "🔬 **リサーチモード**: 会話を継続します。"
                    "「では、その中で◯◯はどれですか?」のようにフォローアップできます。"
                    "新しいトピックに移る時は会話履歴をクリアしてください。",
                    icon="ℹ️",
                )
            else:
                st.caption(
                    "💡 **1回回答モード**: 各質問を独立して処理。"
                    "前の会話を引き継ぎません。短い質問に最適。"
                )

        st.caption(
            "💬 SCMデータに自然言語で質問できます。"
            "下のサンプル質問をクリックするか、自由入力してください。"
            "応答には数十秒かかる場合があります (LLM 推論 + SQL 実行 + 結果整形)。"
        )

        def _ask_genie(question: str):
            """Genie に質問を送信して結果をセッションに保存"""
            use_multi_turn = "リサーチ" in st.session_state.get("genie_mode", "")
            st.session_state.genie_messages.append({"role": "user", "content": question})
            with st.status("🤖 Genie に問い合わせ中...", expanded=True) as status:
                st.write("📡 質問を送信...")
                result = genie.query(question, multi_turn=use_multi_turn)
                elapsed = result.get("elapsed", 0)
                if result.get("error"):
                    status.update(label=f"⚠️ エラー ({elapsed}秒)", state="error", expanded=True)
                    st.error(result["error"])
                else:
                    status.update(
                        label=f"✅ 応答取得 ({elapsed}秒)",
                        state="complete",
                        expanded=False,
                    )
            answer = result.get("text") or "(回答なし)"
            if result.get("error"):
                answer = f"⚠️ エラー: {result['error']}"
            payload = {
                "role":    "assistant",
                "content": answer,
                "elapsed": result.get("elapsed", 0),
            }
            if result.get("sql"):
                payload["sql"] = result["sql"]
            st.session_state.genie_messages.append(payload)

        # ── サンプル質問ボタン ────────────────────
        st.markdown("**サンプル質問**")
        cols = st.columns(2)
        for i, q in enumerate(SAMPLE_QUERIES[:8]):
            with cols[i % 2]:
                if st.button(q, key=f"sq_{i}", use_container_width=True):
                    _ask_genie(q)
                    st.rerun()

        st.markdown("---")

        # ── 会話履歴 ─────────────────────────────
        # SQL は折りたたんで表示。chat_message 内でネスト expander が使えないため、
        # トグルボタン + コードブロックで実装する。
        for idx, msg in enumerate(st.session_state.genie_messages):
            with st.chat_message(msg["role"]):
                st.write(msg["content"])
                if msg.get("elapsed"):
                    st.caption(f"⏱ {msg['elapsed']}秒")
                if msg.get("sql"):
                    show_key = f"show_sql_{idx}"
                    if show_key not in st.session_state:
                        st.session_state[show_key] = False
                    if st.button(
                        "📄 実行された SQL を表示" if not st.session_state[show_key] else "📄 SQL を隠す",
                        key=f"toggle_sql_{idx}",
                    ):
                        st.session_state[show_key] = not st.session_state[show_key]
                        st.rerun()
                    if st.session_state[show_key]:
                        st.code(msg["sql"], language="sql")

        # ── フリー入力 ───────────────────────────
        if prompt := st.chat_input("SCMデータについて質問..."):
            _ask_genie(prompt)
            st.rerun()

        # ── 会話クリアボタン ─────────────────────
        if st.session_state.genie_messages:
            clear_col, _ = st.columns([1, 4])
            with clear_col:
                if st.button("🗑️ 会話履歴をクリア", key="clear_genie"):
                    st.session_state.genie_messages = []
                    # SQL 表示状態もクリア
                    for k in list(st.session_state.keys()):
                        if str(k).startswith("show_sql_"):
                            del st.session_state[k]
                    # multi-turn の会話 ID もリセット
                    genie.reset_conversation()
                    st.rerun()
