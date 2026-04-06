"""
Inventory Policy & Breach Radar — 在庫基準逸脱レーダー
仕様書 §10 準拠
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from styles import inject_css
from services.database import get_balance_projection, get_inventory_breach
from components.global_filter import render_global_filter, apply_filters
from components.explain_panel import render_explain

st.set_page_config(page_title="在庫基準逸脱レーダー | SCM判断支援", page_icon="📦", layout="wide")
inject_css()
from components.sidebar import render_sidebar
render_sidebar()

# ── タイトルバー ──────────────────────────────────
st.markdown("""
<div class="title-bar">
    <span class="logo">📦</span>
    <div>
        <div class="title">在庫基準逸脱レーダー</div>
        <div class="subtitle">Inventory Policy & Breach Radar</div>
    </div>
    <span class="badge">§10 在庫政策</span>
</div>""", unsafe_allow_html=True)

# ── データ取得 ──────────────────────────────────
_DATA = Path(__file__).parent.parent / "sample_data"


@st.cache_data(ttl=600)
def load_all():
    balance = get_balance_projection()
    breach = get_inventory_breach()

    # 型変換
    for col in ["customer_stock_proj", "confirmed_order_qty", "forecast_qty",
                "inbound_qty_order_linked", "production_use_qty", "min_qty", "max_qty"]:
        if col in balance.columns:
            balance[col] = pd.to_numeric(balance[col], errors="coerce").fillna(0).astype(int)

    for col in ["projected_stock", "min_qty", "max_qty"]:
        if col in breach.columns:
            breach[col] = pd.to_numeric(breach[col], errors="coerce").fillna(0).astype(int)

    if "priority_order" in breach.columns:
        breach["priority_order"] = pd.to_numeric(breach["priority_order"], errors="coerce").fillna(9)

    # メーカー情報を結合
    comp = pd.read_csv(_DATA / "components.csv")
    sups = pd.read_csv(_DATA / "suppliers.csv")
    comp_sup = comp.merge(sups[["supplier_id", "supplier_name"]], on="supplier_id", how="left")
    comp_map = comp_sup.set_index("component_id")[["supplier_name", "component_category"]].to_dict("index")

    # 倉庫情報
    wc = pd.read_csv(_DATA / "warehouse_components.csv")
    wh = pd.read_csv(_DATA / "warehouses.csv")
    wc_wh = wc.merge(wh[["warehouse_id", "warehouse_name"]], on="warehouse_id", how="left")
    # 部品ごとの主倉庫
    primary_wh = wc_wh.sort_values("allocation_pct", ascending=False).drop_duplicates("component_id")
    wh_map = primary_wh.set_index("component_id")[["warehouse_id", "warehouse_name"]].to_dict("index")

    def enrich(df):
        if "item_id" in df.columns:
            df["manufacturer_name"] = df["item_id"].map(
                lambda x: comp_map.get(x, {}).get("supplier_name", ""))
            df["component_category"] = df["item_id"].map(
                lambda x: comp_map.get(x, {}).get("component_category", ""))
            df["warehouse_name"] = df["item_id"].map(
                lambda x: wh_map.get(x, {}).get("warehouse_name", ""))
            df["warehouse_id"] = df["item_id"].map(
                lambda x: wh_map.get(x, {}).get("warehouse_id", ""))
        return df

    balance = enrich(balance)
    breach = enrich(breach)
    return balance, breach


balance_raw, breach_raw = load_all()

# ── 表示期間トグル (§10 二段表示) ─────────────────
period = st.radio("表示期間", ["3ヶ月", "6ヶ月"], horizontal=True)
n_months = 3 if period == "3ヶ月" else 6

# ── グローバルフィルター ──────────────────────────
manufacturers = sorted(balance_raw["manufacturer_name"].dropna().unique().tolist())
warehouses = sorted(balance_raw["warehouse_name"].dropna().unique().tolist())
categories = sorted(balance_raw["component_category"].dropna().unique().tolist()) if "component_category" in balance_raw.columns else None

filters = render_global_filter(
    manufacturers=manufacturers if manufacturers else None,
    warehouses=warehouses if warehouses else None,
    categories=categories,
    show_priority=False,
    show_scope=False,
)

balance = apply_filters(balance_raw, filters, warehouse_col="warehouse_id")
breach = apply_filters(breach_raw, filters, warehouse_col="warehouse_id")

# ── 期間フィルタ適用 ─────────────────────────────
if "month_end_date" in balance.columns and len(balance) > 0:
    all_months = sorted(balance["month_end_date"].unique())
    # 現在月を起点に n_months 先まで
    today_str = pd.Timestamp.now().strftime("%Y-%m")
    future_months = [m for m in all_months if m >= today_str][:n_months]
    past_months = [m for m in all_months if m < today_str][-3:]  # 過去3ヶ月も表示
    display_months = sorted(set(past_months + future_months))
    balance = balance[balance["month_end_date"].isin(display_months)]

if "breach_date" in breach.columns and len(breach) > 0:
    cutoff_date = (pd.Timestamp.now() + pd.DateOffset(months=n_months)).strftime("%Y-%m")
    breach = breach[breach["breach_date"] <= cutoff_date]

# ── KPIカード ────────────────────────────────────
st.markdown("---")

if len(breach) > 0:
    zero_count = int(breach[breach["breach_type"] == "ZERO"]["item_id"].nunique())
    under_count = int(breach[breach["breach_type"] == "UNDER"]["item_id"].nunique())
    over_count = int(breach[breach["breach_type"] == "OVER"]["item_id"].nunique())
else:
    zero_count = under_count = over_count = 0

# 基準逸脱が近い件数: OK だが来月 breach になるアイテム
approaching = 0
if len(balance) > 0:
    future_bal = balance[balance["month_end_date"] >= pd.Timestamp.now().strftime("%Y-%m")]
    if len(future_bal) > 0:
        next_month = sorted(future_bal["month_end_date"].unique())[:2]
        if len(next_month) >= 2:
            # 今月 OK だが来月 breach
            this_m = future_bal[future_bal["month_end_date"] == next_month[0]]
            next_m = future_bal[future_bal["month_end_date"] == next_month[1]]
            ok_items = set(this_m[this_m["policy_status"] == "OK"]["item_id"])
            breach_next = set(next_m[next_m["policy_status"] != "OK"]["item_id"])
            approaching = len(ok_items & breach_next)

kpi_cols = st.columns(4)
with kpi_cols[0]:
    st.metric("ZERO件数", f"{zero_count} 件", help="在庫ゼロ予測の部品数")
with kpi_cols[1]:
    st.metric("UNDER件数", f"{under_count} 件", help="最小基準を下回る部品数")
with kpi_cols[2]:
    st.metric("OVER件数", f"{over_count} 件", help="最大基準を超過する部品数")
with kpi_cols[3]:
    st.metric("基準逸脱が近い件数", f"{approaching} 件", help="今月OKだが来月逸脱予測の部品数")

st.markdown("---")

# ── メインタブ ────────────────────────────────────
tab_breach, tab_heatmap, tab_mfr, tab_detail = st.tabs([
    "逸脱一覧", "月次ヒートマップ", "メーカー別サマリー", "部品別詳細"
])

# ━━ TAB 1: 逸脱一覧テーブル ━━━━━━━━━━━━━━━━━━━━━
with tab_breach:
    if len(breach) == 0:
        st.info("現在の条件で在庫基準逸脱は検出されていません。")
    else:
        # days_to_breach 計算
        breach_disp = breach.copy()
        today = pd.Timestamp.now()
        breach_disp["breach_month"] = breach_disp["breach_date"]
        breach_disp["breach_dt"] = pd.to_datetime(breach_disp["breach_date"] + "-01", format="mixed")
        breach_disp["days_to_breach"] = (breach_disp["breach_dt"] - today).dt.days
        breach_disp["days_to_breach"] = breach_disp["days_to_breach"].clip(lower=0)

        # breach_depth_qty
        breach_disp["breach_depth_qty"] = breach_disp.apply(
            lambda r: r["projected_stock"] - r["min_qty"] if r["breach_type"] in ("ZERO", "UNDER")
            else r["projected_stock"] - r["max_qty"] if r["breach_type"] == "OVER"
            else 0, axis=1
        )

        # 根拠概要 / 推奨対応
        def rationale(r):
            if r["breach_type"] == "ZERO":
                return f"予測在庫が{r['breach_month']}にゼロ到達"
            elif r["breach_type"] == "UNDER":
                return f"予測在庫{r['projected_stock']:,}が最小基準{r['min_qty']:,}を下回る"
            else:
                return f"予測在庫{r['projected_stock']:,}が最大基準{r['max_qty']:,}を超過"

        def recommendation(r):
            if r["breach_type"] == "ZERO":
                return "緊急補充・横持ち検討"
            elif r["breach_type"] == "UNDER":
                return "発注前倒し検討"
            else:
                return "発注抑制・出荷促進"

        breach_disp["根拠概要"] = breach_disp.apply(rationale, axis=1)
        breach_disp["推奨対応"] = breach_disp.apply(recommendation, axis=1)

        # ソート: breach が最も近い順
        breach_disp = breach_disp.sort_values(["days_to_breach", "priority_order"])

        # 表示テーブル
        display_cols = {
            "item_code": "部品",
            "manufacturer_name": "メーカー",
            "warehouse_name": "倉庫/拠点",
            "breach_type": "breach_type",
            "breach_month": "breach_month",
            "days_to_breach": "days_to_breach",
            "breach_depth_qty": "breach_depth_qty",
            "min_qty": "min",
            "max_qty": "max",
            "根拠概要": "根拠概要",
            "推奨対応": "推奨対応",
        }
        table_df = breach_disp[[c for c in display_cols.keys() if c in breach_disp.columns]].copy()
        table_df.columns = [display_cols[c] for c in table_df.columns]

        st.dataframe(
            table_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "breach_type": st.column_config.TextColumn("逸脱区分", width="small"),
                "breach_month": st.column_config.TextColumn("逸脱月", width="small"),
                "days_to_breach": st.column_config.NumberColumn("残日数", width="small"),
                "breach_depth_qty": st.column_config.NumberColumn("逸脱深度", width="small"),
                "min": st.column_config.NumberColumn("min", width="small"),
                "max": st.column_config.NumberColumn("max", width="small"),
            },
        )

        # CSV ダウンロード
        csv_data = table_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "CSV ダウンロード",
            data=csv_data,
            file_name="inventory_breach_list.csv",
            mime="text/csv",
        )

        # 根拠パネル (上位3件)
        st.markdown("##### 主要逸脱の根拠")
        for _, r in breach_disp.head(3).iterrows():
            render_explain(
                title=f"{r['item_code']} — {r.get('product_name', '')}",
                rationale=r["根拠概要"],
                action=r["推奨対応"],
                severity=r["breach_type"],
            )


# ━━ TAB 2: 月次ヒートマップ ━━━━━━━━━━━━━━━━━━━━━
with tab_heatmap:
    if len(balance) == 0:
        st.info("表示データがありません。")
    else:
        # ピボット: 行=item_code+product_name, 列=month
        hm = balance.copy()
        hm["row_label"] = hm["item_code"] + " " + hm["product_name"]

        # breach が最も近い順にソート
        if len(breach) > 0:
            first_breach = breach.groupby("item_id")["breach_date"].min().reset_index()
            first_breach.rename(columns={"breach_date": "sort_key"}, inplace=True)
            hm = hm.merge(first_breach, on="item_id", how="left")
            hm["sort_key"] = hm["sort_key"].fillna("9999-99")
        else:
            hm["sort_key"] = "9999-99"

        hm = hm.sort_values("sort_key")
        item_order = hm.drop_duplicates("row_label")["row_label"].tolist()

        months_sorted = sorted(hm["month_end_date"].unique())
        pivot_stock = hm.pivot_table(
            index="row_label", columns="month_end_date",
            values="customer_stock_proj", aggfunc="first",
        ).reindex(index=item_order, columns=months_sorted)

        pivot_status = hm.pivot_table(
            index="row_label", columns="month_end_date",
            values="policy_status", aggfunc="first",
        ).reindex(index=item_order, columns=months_sorted)

        # セルテキスト: 在庫数 + シンボル
        symbol_map = {"ZERO": " [Z]", "UNDER": " [U]", "OVER": " [O]", "OK": ""}
        text_matrix = []
        for idx in pivot_stock.index:
            row_text = []
            for col in pivot_stock.columns:
                val = pivot_stock.loc[idx, col]
                status = pivot_status.loc[idx, col] if pd.notna(pivot_status.loc[idx, col]) else "OK"
                sym = symbol_map.get(status, "")
                val_str = f"{int(val):,}" if pd.notna(val) else "-"
                row_text.append(f"{val_str}{sym}")
            text_matrix.append(row_text)

        # 色マトリクス: ZERO/UNDER=red, OVER=blue, OK=dark
        color_map = {"ZERO": "#ff4646", "UNDER": "#ff4646", "OVER": "#58a6ff", "OK": "#1c2128"}
        z_colors = []
        for idx in pivot_status.index:
            row_colors = []
            for col in pivot_status.columns:
                status = pivot_status.loc[idx, col] if pd.notna(pivot_status.loc[idx, col]) else "OK"
                row_colors.append(color_map.get(status, "#1c2128"))
            z_colors.append(row_colors)

        # 数値Z (色のスケール代わりに使う — カスタムカラーにはannotatedテーブルを使用)
        # plotly heatmapではcellcolorが直接指定できないので table を使う
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=["部品"] + [str(m) for m in months_sorted],
                fill_color="#161b22",
                font=dict(color="#e6edf3", size=11),
                align="center",
                line_color="#30363d",
            ),
            cells=dict(
                values=(
                    [list(pivot_stock.index)]
                    + [
                        [text_matrix[i][j] for i in range(len(text_matrix))]
                        for j in range(len(months_sorted))
                    ]
                ),
                fill_color=(
                    [["#161b22"] * len(pivot_stock.index)]
                    + [
                        [z_colors[i][j] for i in range(len(z_colors))]
                        for j in range(len(months_sorted))
                    ]
                ),
                font=dict(color="#e6edf3", size=11),
                align="center",
                line_color="#30363d",
                height=28,
            ),
        )])
        fig.update_layout(
            margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor="#0d1117",
            height=max(400, len(pivot_stock.index) * 30 + 60),
            title=dict(text="月次在庫ヒートマップ  [Z]=ZERO  [U]=UNDER  [O]=OVER",
                       font=dict(color="#8b949e", size=13)),
        )
        st.plotly_chart(fig, use_container_width=True)


# ━━ TAB 3: メーカー別サマリー ━━━━━━━━━━━━━━━━━━━━
with tab_mfr:
    if len(breach) == 0:
        st.info("逸脱データがありません。")
    else:
        mfr_summary = breach.groupby("manufacturer_name").apply(
            lambda g: pd.Series({
                "OVER": int((g["breach_type"] == "OVER").any().sum()) if False else int(g[g["breach_type"] == "OVER"]["item_id"].nunique()),
                "ZERO_UNDER": int(g[g["breach_type"].isin(["ZERO", "UNDER"])]["item_id"].nunique()),
            }),
            include_groups=False,
        ).reset_index()
        mfr_summary.columns = ["メーカー", "OVER", "ZERO & UNDER"]
        mfr_summary = mfr_summary.sort_values("ZERO & UNDER", ascending=False)

        st.dataframe(mfr_summary, use_container_width=True, hide_index=True)

        # 横棒グラフ
        fig_mfr = go.Figure()
        fig_mfr.add_trace(go.Bar(
            y=mfr_summary["メーカー"], x=mfr_summary["ZERO & UNDER"],
            name="ZERO & UNDER", orientation="h",
            marker_color="#ff4646",
        ))
        fig_mfr.add_trace(go.Bar(
            y=mfr_summary["メーカー"], x=mfr_summary["OVER"],
            name="OVER", orientation="h",
            marker_color="#58a6ff",
        ))
        fig_mfr.update_layout(
            barmode="group",
            paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
            font=dict(color="#e6edf3"),
            legend=dict(font=dict(color="#e6edf3")),
            xaxis=dict(gridcolor="#30363d", title="部品数"),
            yaxis=dict(gridcolor="#30363d"),
            margin=dict(l=0, r=20, t=30, b=40),
            height=max(300, len(mfr_summary) * 40 + 80),
        )
        st.plotly_chart(fig_mfr, use_container_width=True)


# ━━ TAB 4: 部品別詳細 (エキスパンダー) ━━━━━━━━━━━━
with tab_detail:
    if len(balance) == 0:
        st.info("表示データがありません。")
    else:
        # breach がある部品を優先表示
        if len(breach) > 0:
            breach_items = breach.sort_values(
                ["priority_order", "breach_date"]
            )["item_id"].unique().tolist()
        else:
            breach_items = []

        all_items = balance["item_id"].unique().tolist()
        # breach 品を先頭に
        ordered_items = breach_items + [i for i in all_items if i not in breach_items]

        for item_id in ordered_items:
            item_bal = balance[balance["item_id"] == item_id].sort_values("month_end_date")
            if len(item_bal) == 0:
                continue

            first_row = item_bal.iloc[0]
            label_parts = [first_row.get("item_code", item_id)]
            if "product_name" in first_row:
                label_parts.append(str(first_row["product_name"]))
            if "manufacturer_name" in first_row and first_row["manufacturer_name"]:
                label_parts.append(f"[{first_row['manufacturer_name']}]")

            # breach マーク
            item_breach = breach[breach["item_id"] == item_id] if len(breach) > 0 else pd.DataFrame()
            breach_mark = ""
            if len(item_breach) > 0:
                types = item_breach["breach_type"].unique()
                marks = []
                if "ZERO" in types:
                    marks.append("ZERO")
                if "UNDER" in types:
                    marks.append("UNDER")
                if "OVER" in types:
                    marks.append("OVER")
                breach_mark = " — " + "/".join(marks)

            with st.expander(f"{' | '.join(label_parts)}{breach_mark}", expanded=False):
                # 月次詳細テーブル
                detail_cols = {
                    "month_end_date": "月",
                    "customer_stock_proj": "予測在庫",
                    "confirmed_order_qty": "確定受注",
                    "forecast_qty": "フォーキャスト",
                    "inbound_qty_order_linked": "入荷予定",
                    "production_use_qty": "消費量",
                    "min_qty": "min",
                    "max_qty": "max",
                    "policy_status": "ステータス",
                }
                avail = [c for c in detail_cols if c in item_bal.columns]
                detail_df = item_bal[avail].copy()
                detail_df.columns = [detail_cols[c] for c in avail]
                st.dataframe(detail_df, use_container_width=True, hide_index=True)

                # 折れ線グラフ: 予測在庫 + min/max ライン
                months = item_bal["month_end_date"].tolist()
                stock = item_bal["customer_stock_proj"].tolist()
                min_vals = item_bal["min_qty"].tolist()
                max_vals = item_bal["max_qty"].tolist()

                fig_item = go.Figure()
                fig_item.add_trace(go.Scatter(
                    x=months, y=stock,
                    mode="lines+markers",
                    name="予測在庫",
                    line=dict(color="#e6edf3", width=2),
                    marker=dict(size=6),
                ))
                fig_item.add_trace(go.Scatter(
                    x=months, y=min_vals,
                    mode="lines",
                    name="最小基準 (min)",
                    line=dict(color="#ffa000", width=1, dash="dash"),
                ))
                fig_item.add_trace(go.Scatter(
                    x=months, y=max_vals,
                    mode="lines",
                    name="最大基準 (max)",
                    line=dict(color="#58a6ff", width=1, dash="dash"),
                ))

                # breach 月をマーク
                if len(item_breach) > 0:
                    for _, br in item_breach.iterrows():
                        bm = br["breach_date"]
                        bs = br["projected_stock"]
                        bt = br["breach_type"]
                        marker_color = "#ff4646" if bt in ("ZERO", "UNDER") else "#58a6ff"
                        fig_item.add_trace(go.Scatter(
                            x=[bm], y=[bs],
                            mode="markers",
                            marker=dict(size=12, color=marker_color, symbol="x"),
                            name=f"{bt} ({bm})",
                            showlegend=False,
                        ))

                fig_item.update_layout(
                    paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
                    font=dict(color="#e6edf3", size=11),
                    legend=dict(font=dict(color="#e6edf3", size=10)),
                    xaxis=dict(gridcolor="#30363d", title="月"),
                    yaxis=dict(gridcolor="#30363d", title="数量"),
                    margin=dict(l=40, r=20, t=20, b=40),
                    height=300,
                )
                st.plotly_chart(fig_item, use_container_width=True, key=f"chart_{item_id}")
