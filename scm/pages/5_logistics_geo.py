"""
🚚 物流トラッキング（GeoGenie活用）
====================================
業務フロー: 納期回答 → 出荷 → 顧客倉庫到着

このページの役割:
  - サプライチェーンの物流フロー（メーカー→マクニカ新子安→顧客倉庫）を地図で可視化
  - 「倉庫」は基本的に**顧客倉庫**（顧客自社拠点）を指す
  - マクニカ拠点は新子安ロジスティクスセンター1箇所のみ
  - 顧客倉庫の在庫健全性と入荷遅延を地図で一覧把握
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st

from styles import inject_css
from components.sidebar import render_sidebar
from components.today_banner import render_today_banner
from components.japan_map import render_japan_map
from services.config import get_as_of_date
from services.database import (
    get_geo_warehouse,
    get_silver_shipment_routes,
    get_silver_logistics,
    get_silver_warehouses,
)

try:
    from services.genie_client import get_genie_client
    GENIE_AVAILABLE = True
except Exception:
    GENIE_AVAILABLE = False

# マクニカ新子安拠点ID
MACNICA_WAREHOUSE_ID = "WH_MAC_SHINKOYASU"

st.set_page_config(page_title="物流トラッキング | SCM調達支援", layout="wide")
inject_css()
render_sidebar()
render_today_banner(extra_note="メーカー→マクニカ新子安→顧客倉庫の物流フロー")

# ────────────────────────────────────────────────────────
# データロード
# ────────────────────────────────────────────────────────
with st.spinner("データを読み込んでいます..."):
    geo_wh    = get_geo_warehouse()
    routes    = get_silver_shipment_routes()
    logistics = get_silver_logistics()
    warehouses = get_silver_warehouses()

today = get_as_of_date()

# ────────────────────────────────────────────────────────
# タイトル
# ────────────────────────────────────────────────────────
st.markdown("## 🚚 物流トラッキング（GeoGenie活用）")
st.caption(
    "メーカー → マクニカ新子安ロジスティクスセンター → **顧客倉庫** の物流フローを地図で可視化。"
    "顧客倉庫の在庫健全性とルート別出荷数を一目で把握できます。"
)

# 用語の説明
with st.expander("📖 倉庫の種類について（用語の混同防止）", expanded=False):
    st.markdown(
        """
- **顧客倉庫**（このページの『倉庫』）: 顧客自社が保有する倉庫。地図プロットされる10拠点はすべて**顧客倉庫**。
- **マクニカ拠点**: 新子安ロジスティクスセンター（神奈川県）1箇所のみ。マクニカが保有する全部材は新子安を経由します。
- **マクニカフリー在庫**: マクニカ新子安に保管された顧客向け引当済在庫。詳細は『📦 マクニカフリー在庫モニター』へ。
"""
    )

# ────────────────────────────────────────────────────────
# KPI (顧客倉庫向けの入荷/遅延)
# ────────────────────────────────────────────────────────
if not logistics.empty:
    log = logistics.copy()
    log["actual_arrival_date"] = pd.to_datetime(log["actual_arrival_date"], errors="coerce").dt.date
    log["delay_days"] = pd.to_numeric(log["delay_days"], errors="coerce").fillna(0).astype(int)

    recent_cutoff = today - pd.Timedelta(days=30).to_pytimedelta()
    recent_log = log[log["actual_arrival_date"] >= recent_cutoff]

    # 顧客倉庫件数 (マクニカ新子安を除く)
    customer_wh_count = 0
    if not geo_wh.empty:
        customer_wh_count = int((geo_wh["warehouse_id"] != MACNICA_WAREHOUSE_ID).sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📦 直近30日 出荷件数", f"{len(recent_log):,} 件")
    delayed_n = int((recent_log["delay_days"] > 0).sum())
    k2.metric("⏰ 直近30日 遅延件数", f"{delayed_n:,} 件")
    avg_delay = float(recent_log[recent_log["delay_days"] > 0]["delay_days"].mean() or 0)
    k3.metric("📊 平均遅延日数", f"{avg_delay:.1f} 日")
    k4.metric("🏭 顧客倉庫数", f"{customer_wh_count} 拠点")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 日本地図
# ────────────────────────────────────────────────────────
st.markdown("### 🗾 サプライチェーン物流マップ（顧客倉庫の健全性）")
st.caption("地図にプロットされる倉庫は**顧客倉庫**です。健全性スコアで色分け表示。")

col_chk1, col_chk2, _ = st.columns([1, 1, 4])
with col_chk1:
    show_routes = st.checkbox("配送ルートを表示", value=True)
with col_chk2:
    route_filter = st.selectbox(
        "ルート種別",
        ["全て", "メーカー→新子安 (inbound)", "新子安→顧客倉庫 (outbound)"],
        label_visibility="collapsed",
    )

if not geo_wh.empty:
    routes_show = routes.copy() if not routes.empty else pd.DataFrame()
    if not routes_show.empty:
        if route_filter == "メーカー→新子安 (inbound)":
            routes_show = routes_show[routes_show["route_type"] == "inbound"]
        elif route_filter == "新子安→顧客倉庫 (outbound)":
            routes_show = routes_show[routes_show["route_type"] == "outbound"]

    render_japan_map(
        warehouses_df=geo_wh,
        routes_df=routes_show if show_routes else None,
        height=520,
        show_routes=show_routes,
    )
else:
    st.warning("倉庫健全性データ (gold_geo_warehouse_status) が空です。")

st.markdown("---")

# ────────────────────────────────────────────────────────
# 顧客倉庫別 健全性ランキング
# ────────────────────────────────────────────────────────
st.markdown("### 🏆 顧客倉庫別 健全性ランキング")
st.caption("各顧客倉庫について、安全在庫レンジ内に納まっている部材数の割合で健全性を評価。")

if not geo_wh.empty:
    # マクニカ新子安を除外
    customer_wh = geo_wh[geo_wh["warehouse_id"] != MACNICA_WAREHOUSE_ID].copy()
    wh_show_cols = [
        ("warehouse_id",         "倉庫ID"),
        ("warehouse_name",       "倉庫名(顧客倉庫)"),
        ("prefecture",           "都道府県"),
        ("managed_count",        "管理部材数"),
        ("total_stock_qty",      "在庫数量"),
        ("zero_count",           "🔴 在庫切れ"),
        ("under_count",          "🟠 安全在庫割れ"),
        ("incoming_shipments",   "入荷予定"),
        ("delayed_shipments",    "遅延中"),
        ("health_score",         "健全性スコア"),
    ]
    cols_present = [(k, v) for k, v in wh_show_cols if k in customer_wh.columns]
    df_show = customer_wh[[k for k, _ in cols_present]].rename(columns=dict(cols_present))
    if "健全性スコア" in df_show.columns:
        df_show = df_show.sort_values("健全性スコア", ascending=False)
    st.dataframe(df_show, hide_index=True, use_container_width=True, height=320)

st.markdown("---")

# ────────────────────────────────────────────────────────
# 直近の出荷遅延明細
# ────────────────────────────────────────────────────────
st.markdown("### ⏰ 直近の出荷遅延（メーカー→新子安 もしくは 新子安→顧客倉庫）")
if not logistics.empty:
    log = logistics.copy()
    log["actual_arrival_date"] = pd.to_datetime(log["actual_arrival_date"], errors="coerce").dt.date
    log["expected_arrival_date"] = pd.to_datetime(log["expected_arrival_date"], errors="coerce").dt.date
    log["delay_days"] = pd.to_numeric(log["delay_days"], errors="coerce").fillna(0).astype(int)

    recent_cutoff = today - pd.Timedelta(days=60).to_pytimedelta()
    delayed = log[(log["actual_arrival_date"] >= recent_cutoff) & (log["delay_days"] > 0)].copy()
    delayed = delayed.sort_values("delay_days", ascending=False).head(20)
    if delayed.empty:
        st.success("✅ 直近60日に遅延はありません。")
    else:
        show = delayed[[
            "shipment_id", "component_id", "destination_warehouse_id",
            "expected_arrival_date", "actual_arrival_date", "delay_days", "delay_cause", "quantity",
        ]].rename(columns={
            "shipment_id":              "出荷ID",
            "component_id":             "部材ID",
            "destination_warehouse_id": "到着先倉庫",
            "expected_arrival_date":    "予定到着日",
            "actual_arrival_date":      "実到着日",
            "delay_days":               "遅延日数",
            "delay_cause":              "遅延原因",
            "quantity":                 "数量",
        })
        st.dataframe(show, hide_index=True, use_container_width=True)

st.markdown("---")

# ────────────────────────────────────────────────────────
# Genie 自然言語クエリ
# ────────────────────────────────────────────────────────
st.markdown("### 💬 Genie で質問する（自然言語クエリ）")
st.caption("例: 「埼玉県の顧客倉庫の在庫状況は？」「直近30日の遅延が多い部材を教えて」")

if not GENIE_AVAILABLE:
    st.info("Genie クライアントが利用できません。Databricks Apps 環境で実行するとアクティブになります。")
else:
    try:
        genie = get_genie_client()
        if not genie.is_available:
            st.info("Genie space ID が未設定です。app.yaml の SCM_GENIE_SPACE_ID を確認してください。")
        else:
            with st.form("genie_form"):
                q = st.text_area(
                    "質問を入力",
                    placeholder="例: 神奈川県の倉庫で安全在庫を割っている部材を教えて",
                    height=80,
                )
                submitted = st.form_submit_button("🔍 Genie に質問", use_container_width=True)

            if submitted and q.strip():
                with st.spinner("Genie が解析中..."):
                    result = genie.query(q)

                status = result.get("status")
                if status == "ok":
                    df = result.get("data")
                    st.success(f"✅ 結果取得 ({result.get('elapsed', 0):.1f}秒)")
                    if df is not None and not df.empty:
                        st.dataframe(df, hide_index=True, use_container_width=True)
                    else:
                        st.info("該当データがありません。")
                elif status == "no_data":
                    st.warning("Genie は SQL を実行しましたが、該当データがありませんでした。")
                elif status == "ng":
                    st.warning("Genie が SQL を生成できませんでした。質問を具体化してください。")
                    if result.get("genie_text"):
                        st.caption(f"Genie からのメッセージ: {result['genie_text']}")
                else:
                    st.error(f"エラー: {result.get('error') or result.get('message')}")

                if result.get("sql"):
                    with st.expander("実行された SQL"):
                        st.code(result["sql"], language="sql")
    except Exception as e:
        st.warning(f"Genie 機能が初期化できませんでした: {e}")
