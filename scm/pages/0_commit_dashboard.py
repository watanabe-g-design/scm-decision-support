"""
📌 納期コミットダッシュボード (顧客への納期コミット可視化)
================================================================
業務上の役割:
  - 「いつまでに何をしなければいけないのか」を顧客視点で可視化
  - 顧客への受注に対する納期コミット状況を一画面で把握
  - 各受注の deadline_date / days_to_due / priority_rank / adjustment_action を表示
  - 残日数の少ない順に並び、最優先で動くべき案件を即時特定

業務フロー上の位置:
  顧客FCST → 受注 (Sales Order) → ★この画面で納期コミットを管理 → 部材調達

データソース: gold_order_commit_risk
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from services.config import get_as_of_date
from services.plot_theme import base_layout, get_theme_tokens
from services.database import get_order_commit_risk, get_procurement_options, get_silver_components

st.set_page_config(page_title="納期コミット | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="顧客への納期コミット — いつまでに何をしなければいけないか")

t = get_theme_tokens()

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    commit  = get_order_commit_risk()
    options = get_procurement_options()
    comps   = get_silver_components()

today = get_as_of_date()

st.markdown("## 📌 製品 納期コミットダッシュボード（受注ベース）")
st.caption(
    "顧客から受注した**製品**の納期コミット状況を一覧表示します。"
    "**残日数の少ない順** に並べ、**今すぐ動くべき案件 (Critical)** を最優先で対応します。"
    "各Critical案件の下部には、どの**半導体部材**がボトルネックになっているかと、"
    "「誰に何を依頼すべきか」を自動生成します。"
)

if commit.empty:
    st.warning(
        "gold_order_commit_risk が空です。Lakeflow パイプラインを実行してください。"
        " （または受注データ silver_sales_orders が空の可能性があります）"
    )
    st.stop()

# 型整え
commit = commit.copy()
commit["requested_delivery_date"] = pd.to_datetime(commit["requested_delivery_date"], errors="coerce").dt.date
commit["deadline_date"] = pd.to_datetime(commit["deadline_date"], errors="coerce").dt.date

# days_to_due: 列がなければ requested_delivery_date から計算
if "days_to_due" not in commit.columns:
    commit["days_to_due"] = commit["requested_delivery_date"].apply(
        lambda d: int((d - today).days) if d is not None and not pd.isna(d) else 0
    )

for col in ("days_to_due", "remaining_qty", "current_customer_stock", "risk_score"):
    if col in commit.columns:
        commit[col] = pd.to_numeric(commit[col], errors="coerce").fillna(0).astype(int)

# ────────────────────────────────────────────────────────
# KPI: 優先度別件数 (Critical / High / Mid / Low)
# ────────────────────────────────────────────────────────
priority_counts = commit["priority_rank"].value_counts().to_dict()
n_critical = priority_counts.get("Critical", 0)
n_high     = priority_counts.get("High", 0)
n_mid      = priority_counts.get("Mid", 0)
n_low      = priority_counts.get("Low", 0)
n_total    = len(commit)

st.markdown("### 🎯 納期コミット 優先度サマリー")
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric(
        "🔴 Critical (残3日以内)",
        f"{n_critical} 件",
        help="納期まで残3日以内。今すぐ対応しないと納期遅延の可能性が高い。",
    )
with k2:
    st.metric(
        "🟠 High (残7日以内)",
        f"{n_high} 件",
        help="納期まで残7日以内。今週中にマクニカへの相談・前倒し調整が必要。",
    )
with k3:
    st.metric(
        "🟡 Mid (残14日以内)",
        f"{n_mid} 件",
        help="納期まで残14日以内。来週中に状況確認・対応方針を確定。",
    )
with k4:
    st.metric(
        "🟢 Low (残15日以上)",
        f"{n_low} 件",
        help="納期まで残15日以上。状況モニタリング段階。",
    )
st.caption(f"📊 評価対象受注: 合計 **{n_total}** 件 (基準日 {today.isoformat()})")

st.markdown("---")

# ────────────────────────────────────────────────────────
# フィルター
# ────────────────────────────────────────────────────────
st.markdown("### 🔍 絞り込みフィルター")
fc1, fc2, fc3 = st.columns(3)
with fc1:
    priority_options = ["（すべて）", "🔴 Critical", "🟠 High", "🟡 Mid", "🟢 Low"]
    sel_priority = st.selectbox("📊 優先度で絞り込み", priority_options)
with fc2:
    deadline_options = ["（すべて）", "今日まで（超過）", "3日以内", "7日以内", "14日以内", "30日以内"]
    sel_deadline = st.selectbox("⏰ 残日数で絞り込み", deadline_options)
with fc3:
    customer_options = ["（すべて）"] + sorted([c for c in commit["customer_name"].dropna().unique()])
    sel_customer = st.selectbox("🏢 顧客で絞り込み", customer_options)

df = commit.copy()
priority_map = {
    "🔴 Critical": "Critical",
    "🟠 High":     "High",
    "🟡 Mid":      "Mid",
    "🟢 Low":      "Low",
}
if sel_priority != "（すべて）":
    df = df[df["priority_rank"] == priority_map[sel_priority]]

if sel_deadline == "今日まで（超過）":
    df = df[df["days_to_due"] < 0]
elif sel_deadline == "3日以内":
    df = df[(df["days_to_due"] >= 0) & (df["days_to_due"] <= 3)]
elif sel_deadline == "7日以内":
    df = df[(df["days_to_due"] >= 0) & (df["days_to_due"] <= 7)]
elif sel_deadline == "14日以内":
    df = df[(df["days_to_due"] >= 0) & (df["days_to_due"] <= 14)]
elif sel_deadline == "30日以内":
    df = df[(df["days_to_due"] >= 0) & (df["days_to_due"] <= 30)]

if sel_customer != "（すべて）":
    df = df[df["customer_name"] == sel_customer]

st.markdown("---")

# ────────────────────────────────────────────────────────
# Timeline: 受注を残日数でプロット (横軸=残日数, 色=priority)
# ────────────────────────────────────────────────────────
st.markdown("### 📅 納期タイムライン (横軸=残日数、縦軸=リスクスコア)")
st.caption(
    "横軸の **0** が本日。"
    "**左側 (マイナス)** = 既に納期超過済み、**右側 (プラス)** = まだ猶予あり。"
    "上にいくほどリスクスコアが高い (＝早く対応すべき)。"
)

if df.empty:
    st.info("条件に該当する受注がありません。")
else:
    priority_colors = {
        "Critical": t["red"],
        "High":     t["orange"],
        "Mid":      "#eab308",      # 黄色 (Mid)
        "Low":      t["green"],
    }
    fig = go.Figure()
    for prio in ["Critical", "High", "Mid", "Low"]:
        sub = df[df["priority_rank"] == prio]
        if sub.empty:
            continue
        hover_texts = [
            f"<b>{row['sales_order_id']}</b><br>"
            f"顧客: {row['customer_name']}<br>"
            f"製品: {row['product_name']}<br>"
            f"部材: {row['part_number']} / {row['component_name']}<br>"
            f"希望納期: {row['requested_delivery_date']}<br>"
            f"残数量: {row['remaining_qty']:,}個<br>"
            f"推奨アクション: {row.get('adjustment_action', '—')}<br>"
            f"リスク理由: {row.get('risk_reason', '—')}"
            for _, row in sub.iterrows()
        ]
        fig.add_trace(go.Scatter(
            x=sub["days_to_due"],
            y=sub["risk_score"],
            mode="markers",
            name=f"{prio} ({len(sub)}件)",
            marker=dict(
                size=14,
                color=priority_colors[prio],
                line=dict(color="#ffffff", width=1.5),
                opacity=0.85,
            ),
            text=hover_texts,
            hovertemplate="%{text}<extra></extra>",
        ))

    # 今日線 (x=0)
    fig.add_vline(x=0, line_width=1.5, line_dash="dash", line_color=t["text_sub"])
    fig.add_annotation(
        x=0, y=1.02, yref="paper",
        text=f"📅 本日 ({today.isoformat()})",
        showarrow=False,
        font=dict(size=11, color=t["text_sub"]),
    )
    # Critical閾値
    fig.add_vrect(x0=-30, x1=3, fillcolor=t["red"], opacity=0.04, line_width=0)
    fig.add_vrect(x0=3, x1=7, fillcolor=t["orange"], opacity=0.04, line_width=0)

    fig.update_layout(**base_layout(
        height=420,
        x_title="残日数 (日)",
        y_title="リスクスコア (0-100)",
    ))
    fig.update_xaxes(zeroline=True, zerolinecolor=t["border"], zerolinewidth=1)
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── タブ: 一覧 / Critical詳細（ボトルネック分析）──────────
tab_list, tab_crit = st.tabs(["📋 納期コミット一覧", "🚨 Critical 受注 ボトルネック分析"])

# ─── Tab 1: 一覧表 ──────────────────────────────────────
with tab_list:
    st.caption("**残日数の少ない順** に表示。「推奨アクション」欄に従って今日の対応を決めてください。")
    if df.empty:
        st.info("条件に該当する受注がありません。")
    else:
        # React #185対策: st.dataframe を完全に回避してHTMLテーブルで描画
        df_show = df.copy()
        for col in ["sales_order_id","customer_name","product_name","part_number",
                    "adjustment_action","priority_rank"]:
            if col in df_show.columns:
                df_show[col] = df_show[col].fillna("").astype(str)
        for col in ["days_to_due","remaining_qty"]:
            if col in df_show.columns:
                df_show[col] = pd.to_numeric(df_show[col], errors="coerce").fillna(0).astype(int)

        priority_order = {"Critical": 0, "High": 1, "Mid": 2, "Low": 3}
        df_show["_p"] = df_show["priority_rank"].map(priority_order).fillna(9)
        df_show = df_show.sort_values(["_p", "days_to_due"]).drop(columns="_p")

        # 受注ID単位で重複削除 (BOM展開で受注ごとに複数行ある状態を解消)
        if "sales_order_id" in df_show.columns:
            df_show = df_show.drop_duplicates(subset=["sales_order_id"])

        MAX_ROWS = 100
        total_rows = len(df_show)
        if total_rows > MAX_ROWS:
            df_show = df_show.head(MAX_ROWS)
            st.caption(f"⚠️ 全{total_rows:,}件中、上位 {MAX_ROWS} 件を表示。フィルターで絞り込むと全件確認可能。")

        # ── 軽量HTMLテーブル (st.dataframe 不使用) ──
        priority_badge = {
            "Critical": '<span style="color:#dc2626;font-weight:600;">🔴 Critical</span>',
            "High":     '<span style="color:#d97706;font-weight:600;">🟠 High</span>',
            "Mid":      '<span style="color:#ca8a04;font-weight:600;">🟡 Mid</span>',
            "Low":      '<span style="color:#059669;font-weight:600;">🟢 Low</span>',
        }
        header = (
            '<thead><tr style="background:#f1f5f9;text-align:left;font-size:12px;">'
            '<th style="padding:8px;">受注ID</th>'
            '<th style="padding:8px;">顧客</th>'
            '<th style="padding:8px;">製品</th>'
            '<th style="padding:8px;">希望納期</th>'
            '<th style="padding:8px;text-align:right;">残日数</th>'
            '<th style="padding:8px;">Priority</th>'
            '<th style="padding:8px;">推奨アクション</th>'
            '</tr></thead>'
        )
        body_rows = []
        for _, row in df_show.iterrows():
            pr = priority_badge.get(row.get("priority_rank","Low"), row.get("priority_rank",""))
            body_rows.append(
                '<tr style="border-bottom:1px solid #e2e8f0;font-size:12px;">'
                f'<td style="padding:8px;">{row.get("sales_order_id","")}</td>'
                f'<td style="padding:8px;">{row.get("customer_name","")}</td>'
                f'<td style="padding:8px;">{row.get("product_name","")}</td>'
                f'<td style="padding:8px;">{row.get("requested_delivery_date","")}</td>'
                f'<td style="padding:8px;text-align:right;">{row.get("days_to_due",0)}日</td>'
                f'<td style="padding:8px;">{pr}</td>'
                f'<td style="padding:8px;">{row.get("adjustment_action","")}</td>'
                '</tr>'
            )
        html = (
            '<div style="max-height:420px;overflow-y:auto;border:1px solid #e2e8f0;border-radius:8px;">'
            '<table style="width:100%;border-collapse:collapse;">'
            f'{header}<tbody>{"".join(body_rows)}</tbody></table></div>'
        )
        st.markdown(html, unsafe_allow_html=True)

# ─── Tab 2: Critical ボトルネック分析 ──────────────────
with tab_crit:
    st.caption(
        "Critical受注ごとに「どの**半導体部材**が不足しているか」と"
        "「**誰に何を依頼すべきか**」を自動分析します。"
    )
    crit = df[df["priority_rank"] == "Critical"].copy()
    if crit.empty:
        st.success("✅ Critical (残3日以内) の受注はありません。今日緊急対応すべき案件なし。")
    else:
        crit = crit.sort_values("days_to_due").head(10)

        # procurement_options から部材ごとのベストルートを取得
        best_route = pd.DataFrame()
        if not options.empty:
            opt = options.copy()
            opt["shortage_qty"] = pd.to_numeric(opt.get("shortage_qty", 0), errors="coerce").fillna(0)
            opt["available_qty"] = pd.to_numeric(opt.get("available_qty", 0), errors="coerce").fillna(0)
            # 各 component_id のベストルート (shortage_qty 最小を選択)
            best_route = (
                opt.sort_values("shortage_qty")
                .groupby("component_id")
                .first()
                .reset_index()[["component_id", "route_type", "available_qty", "shortage_qty", "action_level"]]
            )

        # React #185回避: 複雑なHTMLカード→Streamlitネイティブコンポーネントへ
        for idx, (_, row) in enumerate(crit.iterrows()):
            try:
                days = int(pd.to_numeric(row.get("days_to_due", 0), errors="coerce") or 0)
            except Exception:
                days = 0
            comp_id = row.get("component_id", "")
            days_label = (f"納期超過 {abs(days)}日" if days < 0 else f"残 {days} 日")

            # ボトルネック情報を取得
            route_info = None
            if not best_route.empty and comp_id in best_route["component_id"].values:
                route_info = best_route[best_route["component_id"] == comp_id].iloc[0]

            # 推奨アクション生成
            try:
                required = int(pd.to_numeric(row.get("component_required_qty", 0), errors="coerce") or 0)
                cust_stock = int(pd.to_numeric(row.get("current_customer_stock", 0), errors="coerce") or 0)
                remaining = int(pd.to_numeric(row.get("remaining_qty", 0), errors="coerce") or 0)
            except Exception:
                required = cust_stock = remaining = 0

            if route_info is not None:
                al = str(route_info.get("action_level", ""))
                avail = int(pd.to_numeric(route_info.get("available_qty", 0), errors="coerce") or 0)
                short = required - avail
                route = str(route_info.get("route_type", ""))
                if al == "不要" or short <= 0:
                    action_who, action_what = "（対応不要）", "顧客在庫で充足可能です。"
                elif al in ("軽", "中"):
                    action_who = "📞 マクニカ 営業担当へ連絡"
                    action_what = (f"フリー在庫 {avail:,}個 の引当依頼（今日中）" if route == "MACNICA_FREE"
                                   else "既存発注残BL の納期前倒し・催促依頼（今週中）")
                else:
                    action_who = "📩 マクニカ 調達部へ緊急発注依頼"
                    action_what = f"新規追加発注が必要。不足見込み {max(short, 0):,}個。LT確認後に即時手配。"
            else:
                action_who = str(row.get("adjustment_action", "—"))
                action_what = str(row.get("risk_reason", "—"))

            stock_status = "✅ 充足" if cust_stock >= required else f"⚠️ 不足 {max(0, required - cust_stock):,}個"

            # Streamlit native container
            with st.container():
                st.markdown(
                    f"### 🔴 {row.get('sales_order_id', '—')} ｜ "
                    f"{row.get('customer_name', '—')} → {row.get('product_name', '—')}"
                )
                meta = (
                    f"**希望納期:** {row.get('requested_delivery_date', '—')}　｜　"
                    f"**{days_label}**　｜　**残数量:** {remaining:,}個"
                )
                st.markdown(meta)

                col_left, col_right = st.columns(2)
                with col_left:
                    st.markdown("#### 🔍 半導体部材の状況")
                    st.markdown(
                        f"- 品番: **{row.get('part_number', '—')}**\n"
                        f"- 部材名: {row.get('component_name', '—')}\n"
                        f"- 必要数: **{required:,}個** ｜ 顧客現在庫: **{cust_stock:,}個**\n"
                        f"- 在庫状況: **{stock_status}**"
                    )
                with col_right:
                    st.markdown("#### 📝 推奨アクション")
                    st.markdown(
                        f"**{action_who}**\n\n{action_what}"
                    )
                st.markdown("---")

# ────────────────────────────────────────────────────────
# 使い方ガイド
# ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### ℹ️ このページの使い方")
st.markdown(
    """
**朝一のルーティン**:
1. 上部の **Critical / High** 件数をチェック → 件数が多ければ今日の対応を集中させる
2. 📅 **納期タイムライン** で全体の納期分布を確認 → 左側 (超過) / 右側 (猶予あり) の構成を把握
3. 📋 **納期コミット一覧** で残日数の少ない順に詳細確認 → 推奨アクション欄に従って対応
4. 🚨 **Critical 受注の詳細カード** で個別アクションを確認 → マクニカ営業に相談

**他のページとの連携**:
- 部材レベルの対応は「🎯 調達アクションセンター」で4ルート評価を確認
- 部材ごとの将来在庫推移は「🏭 顧客在庫×安全在庫」で確認
- 緊急の突発需要は「🚨 緊急調達シミュレーター」でシミュレーション
"""
)
