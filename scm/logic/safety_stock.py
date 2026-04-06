"""
安全在庫計算 + 在庫状態判定
"""
import pandas as pd

# カテゴリ別安全在庫週数
SAFETY_STOCK_WEEKS_BY_CATEGORY = {
    "MCU":         4,   # 高リスク・長LT
    "PMIC":        3,
    "TRANSCEIVER": 3,
    "MEMORY":      3,
    "MOSFET":      4,   # 電力系は確保重要
    "SENSOR":      2,
    "CRYSTAL":     2,   # 短LT
}
DEFAULT_SS_WEEKS = 3


def calc_safety_stock(weekly_demand: float, category: str,
                      lead_time_weeks: float = 0, delay_rate: float = 0) -> float:
    """安全在庫 = 週間平均需要 × 安全在庫週数 (+ LT/遅延補正)"""
    base_weeks = SAFETY_STOCK_WEEKS_BY_CATEGORY.get(category, DEFAULT_SS_WEEKS)
    # LT が長い場合加算 (20週超で+1週)
    lt_adder = max(0, (lead_time_weeks - 20) * 0.1)
    # 遅延率が高い場合加算 (15%超で+1週)
    delay_adder = max(0, (delay_rate - 0.15) * 5)
    total_weeks = base_weeks + lt_adder + delay_adder
    return weekly_demand * total_weeks


def classify_inventory_status(current_stock: float, safety_stock: float) -> str:
    """在庫状態を4分類"""
    if safety_stock <= 0:
        return "healthy" if current_stock > 0 else "shortage_risk"
    ratio = current_stock / safety_stock
    if ratio < 1.0:
        return "shortage_risk"
    elif ratio <= 2.5:
        return "healthy"
    elif ratio <= 4.0:
        return "watch"
    else:
        return "overstock"


def calc_coverage_weeks(current_stock: float, weekly_demand: float) -> float:
    """在庫カバー週数"""
    if weekly_demand <= 0:
        return 999.0
    return round(current_stock / weekly_demand, 1)


def classify_breach(current_stock: float, min_stock: float, max_stock: float) -> str:
    """安全在庫ブリーチ分類"""
    if current_stock <= 0:
        return "ZERO"
    elif current_stock < min_stock:
        return "UNDER"
    elif current_stock > max_stock:
        return "OVER"
    return "OK"


def project_monthly_stock(current_stock: float, monthly_demand: list,
                          monthly_supply: list, months: int = 12) -> list:
    """月次在庫シミュレーション"""
    projections = []
    stock = current_stock
    for i in range(months):
        demand = monthly_demand[i] if i < len(monthly_demand) else (monthly_demand[-1] if monthly_demand else 0)
        supply = monthly_supply[i] if i < len(monthly_supply) else 0
        stock = stock - demand + supply
        projections.append(stock)
    return projections


def classify_lt_band(lt_weeks: float) -> str:
    """LT期間帯分類"""
    if lt_weeks <= 13:
        return "13週以内"
    elif lt_weeks <= 26:
        return "14週〜半年"
    elif lt_weeks <= 52:
        return "半年〜1年"
    elif lt_weeks <= 78:
        return "1年〜1.5年"
    else:
        return "1.5年〜2年"


def calc_shortage_date(current_stock: float, daily_demand: float,
                       snapshot_date: str = "2026-03-30") -> str:
    """在庫が安全在庫を割る予想日"""
    if daily_demand <= 0:
        return "N/A"
    days = int(current_stock / daily_demand)
    shortage = pd.Timestamp(snapshot_date) + pd.Timedelta(days=days)
    return shortage.strftime("%Y-%m-%d")
