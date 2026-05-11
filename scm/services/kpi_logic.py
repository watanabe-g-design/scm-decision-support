"""
KPI 集計ロジック (KPI と詳細の数値整合性を保証する単一ソース)
==============================================================
全画面のKPIはこのモジュールの関数経由で算出する。
詳細ページも同じフィルタリングロジックを呼び出すことで、
ダッシュボードの「N件」と詳細の件数を構造的に一致させる。
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd


# ════════════════════════════════════════════════════════
# 期間フィルター（共通）
# ════════════════════════════════════════════════════════
def filter_by_period(
    df: pd.DataFrame,
    date_col: str,
    today: date,
    period_days: int,
    past_buffer_days: int = 30,
) -> pd.DataFrame:
    """期間内の行に絞り込む。過去 past_buffer_days 〜 today + period_days の範囲を含める。"""
    if df.empty:
        return df
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce").dt.date
    start = today - timedelta(days=past_buffer_days)
    end = today + timedelta(days=period_days)
    return out[(out[date_col] >= start) & (out[date_col] <= end)]


# ════════════════════════════════════════════════════════
# 需要のKPI
# ════════════════════════════════════════════════════════
def kpi_demand_total(demand_filtered: pd.DataFrame) -> int:
    return len(demand_filtered)


def kpi_demand_emergency(demand_filtered: pd.DataFrame) -> int:
    if demand_filtered.empty:
        return 0
    return int((demand_filtered["source_type"] == "EMERGENCY_MANUAL").sum())


def kpi_demand_upcoming_30d(demand_filtered: pd.DataFrame, today: date) -> int:
    if demand_filtered.empty:
        return 0
    req_dates = pd.to_datetime(demand_filtered["requested_date"], errors="coerce").dt.date
    return int(((req_dates >= today) & (req_dates <= today + timedelta(days=30))).sum())


def kpi_demand_overdue(demand_filtered: pd.DataFrame, today: date) -> int:
    if demand_filtered.empty:
        return 0
    req_dates = pd.to_datetime(demand_filtered["requested_date"], errors="coerce").dt.date
    return int((req_dates < today).sum())


# ════════════════════════════════════════════════════════
# 調達評価のKPI（gold_procurement_options を需要単位に集約）
# ════════════════════════════════════════════════════════
def aggregate_best_route_per_demand(options: pd.DataFrame) -> pd.DataFrame:
    """需要IDごとに最良ルート（不足とdays_lateが少ない順）を1行に集約。"""
    if options.empty:
        return options
    opt = options.copy()
    opt["shortage_qty"] = pd.to_numeric(opt["shortage_qty"], errors="coerce").fillna(0)
    opt["days_late"] = pd.to_numeric(opt["days_late"], errors="coerce").fillna(0)
    opt["_score"] = opt["shortage_qty"].clip(lower=0) * 1000 + opt["days_late"]
    return opt.sort_values("_score").groupby("demand_id").first().reset_index()


def filter_needs_action(best_route: pd.DataFrame) -> pd.DataFrame:
    """needs_action=True のもののみ返す (顧客在庫単独で済むものを除外)"""
    if best_route.empty:
        return best_route
    if "needs_action" in best_route.columns:
        # boolean に揺れがあるため文字列化で吸収
        return best_route[best_route["needs_action"].astype(str).str.lower().isin(["true", "1", "t"])]
    return best_route[(best_route["shortage_qty"] > 0) | (best_route["days_late"] > 0)]


def filter_by_action_level(best_route: pd.DataFrame, levels: list[str]) -> pd.DataFrame:
    """action_level でフィルター"""
    if best_route.empty or "action_level" not in best_route.columns:
        return best_route
    return best_route[best_route["action_level"].isin(levels)]


def kpi_action_count_by_level(best_route: pd.DataFrame) -> dict[str, int]:
    """対応レベル別の件数 (重/中/軽/不要)"""
    if best_route.empty or "action_level" not in best_route.columns:
        return {"重": 0, "中": 0, "軽": 0, "不要": 0}
    cnt = best_route["action_level"].value_counts().to_dict()
    return {
        "重": int(cnt.get("重", 0)),
        "中": int(cnt.get("中", 0)),
        "軽": int(cnt.get("軽", 0)),
        "不要": int(cnt.get("不要", 0)),
    }


# ════════════════════════════════════════════════════════
# 在庫サマリー
# ════════════════════════════════════════════════════════
def kpi_customer_stock(cust_inv: pd.DataFrame) -> dict[str, int]:
    if cust_inv.empty:
        return {"total_qty": 0, "n_components": 0}
    return {
        "total_qty":   int(pd.to_numeric(cust_inv["stock_qty"], errors="coerce").fillna(0).sum()),
        "n_components": int(cust_inv["component_id"].nunique()),
    }


def kpi_macnica_free_stock(free_inv: pd.DataFrame) -> dict[str, int]:
    if free_inv.empty:
        return {"total_qty": 0, "n_components": 0}
    return {
        "total_qty":   int(pd.to_numeric(free_inv["qty_available"], errors="coerce").fillna(0).sum()),
        "n_components": int(free_inv["component_id"].nunique()),
    }


# ════════════════════════════════════════════════════════
# 製品BOM充足
# ════════════════════════════════════════════════════════
def kpi_bom_fulfillment(fulfill: pd.DataFrame) -> dict[str, int]:
    if fulfill.empty:
        return {"total": 0, "ok": 0, "partial": 0, "critical": 0}
    df = fulfill.copy()
    if "is_all_fulfilled" in df.columns:
        df["is_all_fulfilled"] = df["is_all_fulfilled"].astype(str).str.lower().isin(["true", "1", "t"])
    else:
        df["is_all_fulfilled"] = False
    df["fulfillment_rate"] = pd.to_numeric(df["fulfillment_rate"], errors="coerce").fillna(0)
    n_total = len(df)
    n_ok = int(df["is_all_fulfilled"].sum())
    n_partial = int(((df["fulfillment_rate"] >= 0.8) & (~df["is_all_fulfilled"])).sum())
    n_critical = int((df["fulfillment_rate"] < 0.8).sum())
    return {"total": n_total, "ok": n_ok, "partial": n_partial, "critical": n_critical}
