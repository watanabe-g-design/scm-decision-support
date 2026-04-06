"""
Supply Chain Management - マスターデータ定数 (データ再生成用)
※ すべての会社名・製品名は架空のものです
"""

SUPPLIERS = [
    {"supplier_id": "SUP001", "supplier_name": "Nextera Semiconductor",       "country": "Netherlands", "region": "Europe"},
    {"supplier_id": "SUP002", "supplier_name": "Sakura Electronics",          "country": "Japan",       "region": "Asia"},
    {"supplier_id": "SUP003", "supplier_name": "Pacific Instruments",         "country": "USA",         "region": "Americas"},
    {"supplier_id": "SUP004", "supplier_name": "Alpine Microelectronics",     "country": "Switzerland", "region": "Europe"},
    {"supplier_id": "SUP005", "supplier_name": "Vertex Technologies",         "country": "Germany",     "region": "Europe"},
    {"supplier_id": "SUP006", "supplier_name": "Crossfield Semiconductor",    "country": "USA",         "region": "Americas"},
    {"supplier_id": "SUP007", "supplier_name": "Hikari Semiconductor",        "country": "Japan",       "region": "Asia"},
    {"supplier_id": "SUP008", "supplier_name": "Fuji Electronic Devices",     "country": "Japan",       "region": "Asia"},
    {"supplier_id": "SUP009", "supplier_name": "Pinnacle Technology",         "country": "USA",         "region": "Americas"},
    {"supplier_id": "SUP010", "supplier_name": "Bridgelink Connectivity",     "country": "Switzerland", "region": "Europe"},
]

CUSTOMERS = [
    {"customer_id": "CUS001", "customer_name": "ネクサス精機株式会社",     "segment": "Tier1", "location": "愛知県"},
    {"customer_id": "CUS002", "customer_name": "テクノワイズ株式会社",     "segment": "Tier1", "location": "愛知県"},
    {"customer_id": "CUS003", "customer_name": "ゼニス オートモーティブ",  "segment": "Tier1", "location": "大阪府"},
    {"customer_id": "CUS004", "customer_name": "ハーネステック株式会社",   "segment": "Tier1", "location": "静岡県"},
    {"customer_id": "CUS005", "customer_name": "ユニオン電装株式会社",     "segment": "Tier1", "location": "三重県"},
]

WAREHOUSES = [
    {"warehouse_id": "WH001", "warehouse_name": "浦和倉庫",   "prefecture": "埼玉県",   "city": "さいたま市浦和区", "latitude": 35.8617, "longitude": 139.6455},
    {"warehouse_id": "WH002", "warehouse_name": "横浜倉庫",   "prefecture": "神奈川県", "city": "横浜市鶴見区",   "latitude": 35.5068, "longitude": 139.6747},
    {"warehouse_id": "WH003", "warehouse_name": "名古屋倉庫", "prefecture": "愛知県",   "city": "名古屋市港区",   "latitude": 35.0614, "longitude": 136.8815},
    {"warehouse_id": "WH004", "warehouse_name": "大阪倉庫",   "prefecture": "大阪府",   "city": "大阪市住之江区", "latitude": 34.6270, "longitude": 135.5023},
    {"warehouse_id": "WH005", "warehouse_name": "神戸倉庫",   "prefecture": "兵庫県",   "city": "神戸市東灘区",   "latitude": 34.7226, "longitude": 135.2697},
    {"warehouse_id": "WH006", "warehouse_name": "福岡倉庫",   "prefecture": "福岡県",   "city": "福岡市東区",     "latitude": 33.6253, "longitude": 130.4781},
    {"warehouse_id": "WH007", "warehouse_name": "仙台倉庫",   "prefecture": "宮城県",   "city": "仙台市宮城野区", "latitude": 38.2682, "longitude": 141.0194},
    {"warehouse_id": "WH008", "warehouse_name": "札幌倉庫",   "prefecture": "北海道",   "city": "札幌市白石区",   "latitude": 43.0618, "longitude": 141.3545},
    {"warehouse_id": "WH009", "warehouse_name": "広島倉庫",   "prefecture": "広島県",   "city": "広島市西区",     "latitude": 34.3853, "longitude": 132.4398},
    {"warehouse_id": "WH010", "warehouse_name": "浜松倉庫",   "prefecture": "静岡県",   "city": "浜松市東区",     "latitude": 34.7108, "longitude": 137.7350},
]

PRIORITY_RULES = {
    "CRITICAL": (None, 7),
    "HIGH":     (7,   14),
    "MEDIUM":   (14,  30),
    "LOW":      (30,  None),
}

LEAD_TIME_MULTIPLIERS = {
    "2023-01": 2.2, "2023-04": 1.9, "2023-07": 1.6, "2023-10": 1.4,
    "2024-01": 1.3, "2024-04": 1.2, "2024-07": 1.1, "2024-10": 1.05,
    "2025-01": 1.0, "2025-04": 1.0, "2025-07": 1.05,"2025-10": 1.0,
    "2026-01": 0.95,"2026-04": 0.95,
}
