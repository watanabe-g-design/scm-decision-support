"""
推奨アクション生成ロジック
============================
1需要の4ルート評価結果から、複数の**具体的**な調達アクションを生成する。

抽象的な「要対応」ではなく:
  「① 5月15日までに既存POを50個催促 → 5月25日入庫見込み」
  「② フリー在庫50個 + 新規発注 → 6月15日完納」
  「③ 6月分FCSTを20個前倒し → 5月18日に対応可能」
のような実行可能アクションを並列提示する。
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd

from services.glossary import route_label_jp


# ════════════════════════════════════════════════════════
# データクラス
# ════════════════════════════════════════════════════════
class ActionOption:
    """1つの具体的調達アクション案"""

    def __init__(
        self,
        title: str,
        steps: list[str],
        eta_date: date,
        feasibility: str,           # "確実" / "見込み" / "要相談"
        coverage_qty: int,
        gap_qty: int,
        priority_score: int,
    ):
        self.title = title
        self.steps = steps
        self.eta_date = eta_date
        self.feasibility = feasibility
        self.coverage_qty = coverage_qty
        self.gap_qty = gap_qty
        self.priority_score = priority_score

    def to_dict(self) -> dict:
        return {
            "案":        self.title,
            "確実度":     self.feasibility,
            "充足数量":   f"{self.coverage_qty:,} 個",
            "未充足":     f"{self.gap_qty:,} 個" if self.gap_qty > 0 else "—",
            "完了日":     self.eta_date.isoformat(),
            "手順":       " / ".join(self.steps),
        }


# ════════════════════════════════════════════════════════
# 4ルート結果から具体アクションを生成
# ════════════════════════════════════════════════════════
def generate_action_options(
    routes_df: pd.DataFrame,
    requested_qty: int,
    requested_date: date,
    today: date,
    other_month_pull_in_qty: Optional[int] = None,
    component_lt_weeks: int = 20,
) -> list[ActionOption]:
    """
    4ルートDataFrame + 需要条件 から、3〜5個の実行可能アクション案を生成。

    Parameters
    ----------
    routes_df : pd.DataFrame
        gold_procurement_options を1需要分に絞ったもの
    requested_qty, requested_date : int, date
        需要側の要求
    today : date
        基準日
    other_month_pull_in_qty : Optional[int]
        他月（直後の月）のFCSTから前倒し可能な数量。None なら前倒し案は生成しない
    component_lt_weeks : int
        部材のリードタイム週数
    """
    options: list[ActionOption] = []

    # 各ルートの数値を取り出す
    routes = {row["route_type"]: row for _, row in routes_df.iterrows()}

    def get_route(rt: str) -> dict:
        if rt not in routes:
            return {"available_qty": 0, "eta_date": today, "is_in_time": False, "days_late": 0}
        r = routes[rt]
        return {
            "available_qty": int(pd.to_numeric(r.get("available_qty", 0), errors="coerce") or 0),
            "eta_date":      r.get("eta_date") if r.get("eta_date") else today,
            "is_in_time":    bool(r.get("is_in_time", False)),
            "days_late":     int(pd.to_numeric(r.get("days_late", 0), errors="coerce") or 0),
        }

    cust = get_route("CUSTOMER_STOCK")
    free = get_route("MACNICA_FREE")
    po   = get_route("EXISTING_PO")
    new  = get_route("NEW_ORDER")

    # ── 案① 顧客在庫単独で充足できる場合（最簡単） ──
    if cust["available_qty"] >= requested_qty:
        options.append(ActionOption(
            title="① 顧客在庫からそのまま引当",
            steps=[
                f"自社倉庫から {requested_qty:,} 個を引当",
                f"{cust['eta_date'].isoformat()} に出庫処理",
            ],
            eta_date=cust["eta_date"],
            feasibility="確実",
            coverage_qty=requested_qty,
            gap_qty=0,
            priority_score=10,
        ))

    # ── 案② フリー在庫単独で充足できる場合 ──
    if free["available_qty"] >= requested_qty:
        options.append(ActionOption(
            title="② マクニカフリー在庫から引当依頼",
            steps=[
                f"マクニカ営業に連絡し {requested_qty:,} 個の引当を依頼",
                f"{free['eta_date'].isoformat()} に入荷予定（マクニカ→顧客の輸送日数を含む）",
            ],
            eta_date=free["eta_date"],
            feasibility="確実",
            coverage_qty=requested_qty,
            gap_qty=0,
            priority_score=20,
        ))

    # ── 案③ 既存発注残BLを催促 ──
    if po["available_qty"] > 0:
        cov = min(po["available_qty"], requested_qty)
        gap = max(0, requested_qty - cov)
        steps = [f"マクニカ→メーカーに対し既存発注残 {cov:,} 個 の納期確認・催促を実施"]
        if isinstance(po["eta_date"], date):
            steps.append(f"最早入荷予定: {po['eta_date'].isoformat()}")
        if gap > 0:
            steps.append(f"⚠️ 不足分 {gap:,} 個 は別ルート（フリー在庫 or 新規発注）で補填要")
        options.append(ActionOption(
            title="③ 既存発注残BLを催促",
            steps=steps,
            eta_date=po["eta_date"] if isinstance(po["eta_date"], date) else today,
            feasibility="見込み" if po["days_late"] == 0 else "要相談",
            coverage_qty=cov,
            gap_qty=gap,
            priority_score=30,
        ))

    # ── 案④ 顧客在庫+フリー在庫 の組合せ ──
    combo_certain = cust["available_qty"] + free["available_qty"]
    if combo_certain >= requested_qty and cust["available_qty"] > 0 and free["available_qty"] > 0:
        c_take = min(cust["available_qty"], requested_qty)
        f_take = min(free["available_qty"], requested_qty - c_take)
        eta = max(cust["eta_date"], free["eta_date"]) if isinstance(cust["eta_date"], date) and isinstance(free["eta_date"], date) else today
        options.append(ActionOption(
            title="④ 顧客在庫 + フリー在庫 の組合せ",
            steps=[
                f"自社倉庫から {c_take:,} 個 を即時引当",
                f"マクニカに残り {f_take:,} 個 のフリー在庫引当を依頼",
                f"完了予定日: {eta.isoformat()}",
            ],
            eta_date=eta,
            feasibility="確実",
            coverage_qty=requested_qty,
            gap_qty=0,
            priority_score=25,
        ))

    # ── 案⑤ 新規追加発注（LT考慮）──
    new_eta = today + timedelta(days=component_lt_weeks * 7)
    new_days_late = max(0, (new_eta - requested_date).days)
    steps_new = [
        f"マクニカ経由で新規追加発注 {requested_qty:,} 個",
        f"リードタイム {component_lt_weeks} 週、入荷予定: {new_eta.isoformat()}",
    ]
    if new_days_late > 0:
        steps_new.append(f"⚠️ 希望納期から {new_days_late} 日遅延見込み")
    options.append(ActionOption(
        title="⑤ 新規追加発注",
        steps=steps_new,
        eta_date=new_eta,
        feasibility="要相談",
        coverage_qty=requested_qty,
        gap_qty=0,
        priority_score=50 + new_days_late,
    ))

    # ── 案⑥ 翌月以降のFCSTから前倒し充当 ──
    if other_month_pull_in_qty and other_month_pull_in_qty > 0:
        pull = min(other_month_pull_in_qty, requested_qty)
        gap = max(0, requested_qty - pull)
        options.append(ActionOption(
            title="⑥ 翌月以降のFCSTから前倒し充当",
            steps=[
                f"翌月分の生産FCSTから {pull:,} 個を本需要に前倒し充当",
                f"前提: 翌月分の在庫余裕を確認し、翌月の手配を別途検討",
                (f"⚠️ 不足 {gap:,} 個 は別ルートで補填" if gap > 0 else "全数前倒し充当で対応可能"),
            ],
            eta_date=today,
            feasibility="要相談",
            coverage_qty=pull,
            gap_qty=gap,
            priority_score=40,
        ))

    # 優先度順にソート
    options.sort(key=lambda o: o.priority_score)
    return options


def estimate_pull_in_qty_from_next_month(
    component_id: str,
    requested_date: date,
    demand_df: pd.DataFrame,
    today: date,
) -> int:
    """
    同部材で「次月以降に予定されているFCST_AUTO需要」から前倒し可能な数量を概算。
    手順: 翌月の同部材需要量の半分までを「前倒し許容」とみなす（保守的）。
    """
    if demand_df.empty:
        return 0
    df = demand_df.copy()
    df["requested_date"] = pd.to_datetime(df["requested_date"], errors="coerce").dt.date
    next_month_start = date(requested_date.year, requested_date.month, 1)
    # 次の月
    if requested_date.month == 12:
        nm = date(requested_date.year + 1, 1, 1)
    else:
        nm = date(requested_date.year, requested_date.month + 1, 1)
    if nm.month == 12:
        nm_end = date(nm.year + 1, 1, 1)
    else:
        nm_end = date(nm.year, nm.month + 1, 1)

    mask = (
        (df["component_id"] == component_id)
        & (df["source_type"] == "FCST_AUTO")
        & (df["requested_date"] >= nm)
        & (df["requested_date"] < nm_end)
    )
    total_next = int(pd.to_numeric(df.loc[mask, "requested_qty"], errors="coerce").fillna(0).sum())
    return total_next // 2  # 保守的に半分まで
