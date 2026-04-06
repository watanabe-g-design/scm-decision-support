"""
発注クリティカル性スコアリング
単純閾値ではなくスコア化 (0-100)
"""


def calc_criticality_score(
    days_until_shortage: float,
    effective_lead_time_days: float,
    coverage_weeks: float,
    demand_variability: float = 0.2,
    delay_rate: float = 0.0,
    open_po_coverage: float = 0.0,
) -> float:
    """
    criticality_score (0-100, 高いほど危険)

    スコア要素:
    1. 安全在庫割れまでの残日数 (max 40pt)
    2. 有効LTとの比較 (max 20pt)
    3. 在庫カバー週数 (max 15pt)
    4. 物流遅延リスク (max 10pt)
    5. 発注残カバー率 (max 10pt)
    6. 需要変動性 (max 5pt)
    """
    score = 0.0

    # 1. 残日数スコア (0-40)
    if days_until_shortage <= 0:
        score += 40
    elif days_until_shortage <= 7:
        score += 40 - (days_until_shortage / 7 * 10)   # 30-40
    elif days_until_shortage <= 14:
        score += 20 + (14 - days_until_shortage) / 7 * 10  # 20-30
    elif days_until_shortage <= 30:
        score += 10 + (30 - days_until_shortage) / 16 * 10  # 10-20
    elif days_until_shortage <= 90:
        score += (90 - days_until_shortage) / 60 * 10   # 0-10
    # >90 days → 0pt

    # 2. LTとの比較 (0-20): LT内に補充できなければ危険
    lt_margin = days_until_shortage - effective_lead_time_days
    if lt_margin < 0:
        score += 20
    elif lt_margin < 7:
        score += 15
    elif lt_margin < 14:
        score += 10
    elif lt_margin < 30:
        score += 5

    # 3. カバー週数 (0-15)
    if coverage_weeks < 1:
        score += 15
    elif coverage_weeks < 2:
        score += 12
    elif coverage_weeks < 4:
        score += 8
    elif coverage_weeks < 6:
        score += 4

    # 4. 物流遅延リスク (0-10)
    score += min(10, delay_rate * 40)

    # 5. 発注残カバー (0-10): PO で需要の何%カバーしているか
    if open_po_coverage < 0.2:
        score += 10
    elif open_po_coverage < 0.5:
        score += 6
    elif open_po_coverage < 0.8:
        score += 3

    # 6. 需要変動性 (0-5)
    score += min(5, demand_variability * 15)

    return round(min(100, max(0, score)), 1)


def score_to_priority(score: float) -> str:
    """スコアから優先度ランク"""
    if score >= 80:
        return "CRITICAL"
    elif score >= 65:
        return "HIGH"
    elif score >= 40:
        return "MEDIUM"
    else:
        return "LOW"


def calc_logistics_risk_score(delay_rate: float, avg_delay_days: float) -> float:
    """物流リスクスコア (0-100)"""
    rate_score = min(50, delay_rate * 200)
    days_score = min(50, avg_delay_days * 5)
    return round(rate_score + days_score, 1)


def calc_leadtime_risk_score(lead_time_weeks: float, delay_rate: float,
                              base_lt_weeks: float = 12) -> float:
    """リードタイムリスクスコア (0-100)"""
    lt_ratio = lead_time_weeks / max(base_lt_weeks, 1)
    lt_score = min(50, max(0, (lt_ratio - 0.8) * 100))
    delay_score = min(50, delay_rate * 200)
    return round(lt_score + delay_score, 1)
