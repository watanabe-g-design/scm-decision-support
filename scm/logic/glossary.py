"""
ドメイン用語辞書 + メトリクス定義
アプリ・Genie・Skills全てで同一定義を参照する
"""

# ══════════════════════════════════════════
# 用語辞書 (gold_business_glossary)
# ══════════════════════════════════════════
GLOSSARY = [
    {"term_id":"T001","term":"顧客在庫","definition":"顧客保有在庫。意思決定の主軸。","synonyms":"顧客確保済在庫,顧客手元在庫","prohibited":"商社在庫と混同しない"},
    {"term_id":"T002","term":"商社在庫","definition":"商社側保有の補助在庫。既存引当を除く可用数量。","synonyms":"フリー在庫,未引当在庫","prohibited":"顧客在庫の代替として使わない"},
    {"term_id":"T003","term":"安全在庫","definition":"部品の安全在庫週数×週間平均需要で算出されるバッファ在庫。","synonyms":"SS,セーフティストック","prohibited":""},
    {"term_id":"T004","term":"LT","definition":"発注から供給可能になるまでの所要期間。","synonyms":"リードタイム,納入リードタイム","prohibited":""},
    {"term_id":"T005","term":"LT長期化","definition":"N-3またはN-6比較でLTが増加(↑)している状態。","synonyms":"LTエスカレーション","prohibited":"前月比は対象外"},
    {"term_id":"T006","term":"指定納期","definition":"顧客要求納期。","synonyms":"希望納期,CRD","prohibited":""},
    {"term_id":"T007","term":"回答納期","definition":"商社または供給側が回答した納期。","synonyms":"確認納期","prohibited":""},
    {"term_id":"T008","term":"デッドライン","definition":"顧客納期を守るための内部締切日。","synonyms":"出庫日,出荷期限","prohibited":""},
    {"term_id":"T009","term":"最短出荷可能日","definition":"システム上安全とみなす最短出荷可能日。","synonyms":"ESD","prohibited":""},
    {"term_id":"T010","term":"分納可能数","definition":"一括納入が難しい場合に先行出荷できる数量。","synonyms":"部分出荷可能数","prohibited":""},
    {"term_id":"T011","term":"ZERO","definition":"在庫予測が0未満。","synonyms":"在庫ゼロ","prohibited":""},
    {"term_id":"T012","term":"UNDER","definition":"在庫予測がmin(安全在庫)未満。","synonyms":"min割れ","prohibited":""},
    {"term_id":"T013","term":"OVER","definition":"在庫予測がmax超過。","synonyms":"過剰在庫","prohibited":""},
    {"term_id":"T014","term":"FCST","definition":"需要予測・計画値。将来需給と月末在庫予測に使用。","synonyms":"フォーキャスト","prohibited":""},
    {"term_id":"T015","term":"FCST消費","definition":"FCSTに基づく将来の在庫消費量。","synonyms":"予測消費","prohibited":""},
    {"term_id":"T016","term":"先行手配","definition":"受注に紐づかないメーカーへの事前発注。","synonyms":"見込発注","prohibited":""},
    {"term_id":"T017","term":"倉庫健全性","definition":"(min/max基準を満たしている品目数÷管理品目数)×100%。倉庫シェア按分後のmin/maxで判定。","synonyms":"","prohibited":"定義なしに'健全'と使わない"},
    {"term_id":"T018","term":"調整Priority","definition":"Critical(3日)/High(7日)/Mid(14日)/Low(14日超)。指定納期までの日数で判定。","synonyms":"","prohibited":""},
    {"term_id":"T019","term":"調整Action","definition":"緊急発注/商社前倒し調整/1週間後再確認/状況モニタリング/発注抑制。","synonyms":"","prohibited":"自由記述ではなく定型で管理"},
]

# ══════════════════════════════════════════
# メトリクス定義 (gold_metric_definition)
# ══════════════════════════════════════════
METRICS = [
    {"metric_id":"M001","metric_name":"Critical Orders","formula":"指定納期まで3日以内のオーダー数","unit":"件","source_gold":"gold_order_commit_risk","screen":"Executive Control Tower"},
    {"metric_id":"M002","metric_name":"High Orders","formula":"指定納期まで7日以内のオーダー数","unit":"件","source_gold":"gold_order_commit_risk","screen":"Executive Control Tower"},
    {"metric_id":"M003","metric_name":"LT Escalation Items","formula":"N-3 or N-6比でLT↑の品目数","unit":"品目","source_gold":"gold_lt_escalation_items","screen":"Executive Control Tower, Lead Time Intelligence"},
    {"metric_id":"M004","metric_name":"3M Policy Breaches","formula":"直近3ヶ月以内にZERO/UNDER/OVERとなる品目数","unit":"品目","source_gold":"gold_inventory_policy_breach","screen":"Executive Control Tower, Inventory Policy"},
    {"metric_id":"M005","metric_name":"Projected Stockout Parts","formula":"3ヶ月以内に在庫ゼロ予測の品目数","unit":"品目","source_gold":"gold_inventory_policy_breach WHERE breach_type='ZERO'","screen":"Executive Control Tower"},
    {"metric_id":"M006","metric_name":"Excess Inventory Parts","formula":"3ヶ月以内にmax超過予測の品目数","unit":"品目","source_gold":"gold_inventory_policy_breach WHERE breach_type='OVER'","screen":"Executive Control Tower"},
    {"metric_id":"M007","metric_name":"Forecast Accuracy","formula":"FCST精度の月次平均(%)","unit":"%","source_gold":"gold_exec_summary_daily","screen":"Executive Control Tower"},
    {"metric_id":"M008","metric_name":"Warehouse Health","formula":"(min/max基準充足品目÷管理品目)×100%、倉庫シェア按分","unit":"%","source_gold":"gold_geo_warehouse_status","screen":"Executive Control Tower, Network & Warehouse Health"},
    {"metric_id":"M009","metric_name":"Coverage Weeks","formula":"現在在庫÷週間平均需要","unit":"週","source_gold":"gold_balance_projection_monthly","screen":"Commit & Supply Balance"},
    {"metric_id":"M010","metric_name":"Risk Score","formula":"納期接近度(0-100)。days_to_due×3を100から減算","unit":"点","source_gold":"gold_order_commit_risk","screen":"Commit & Supply Balance"},
]

# ══════════════════════════════════════════
# Genie用サンプルクエリ (gold_genie_semantic_examples)
# ══════════════════════════════════════════
GENIE_EXAMPLES = [
    {"example_id":"G001","question":"Critical Orderは何件ですか？","expected_sql":"SELECT COUNT(*) FROM gold_order_commit_risk WHERE priority_rank='Critical'","role":"調達,経営層","notes":"指定納期3日以内"},
    {"example_id":"G002","question":"LTが延長傾向にある部品は？","expected_sql":"SELECT item_code, item_name, latest_lt_weeks, trend_arrow_n3, trend_arrow_n6 FROM gold_lt_snapshot_current WHERE trend_arrow_n3='↑' OR trend_arrow_n6='↑'","role":"調達,SCM企画","notes":"N-3/N-6比較"},
    {"example_id":"G003","question":"3ヶ月以内に在庫ゼロになる部品は？","expected_sql":"SELECT DISTINCT item_code, product_name, breach_date FROM gold_inventory_policy_breach WHERE breach_type='ZERO'","role":"生産管理,調達","notes":"直近3ヶ月限定"},
    {"example_id":"G004","question":"浦和倉庫の健全性は？","expected_sql":"SELECT warehouse_name, health_score, zero_count, under_count, over_count FROM gold_geo_warehouse_status WHERE warehouse_name LIKE '%浦和%'","role":"倉庫長","notes":""},
    {"example_id":"G005","question":"過剰在庫の部品はどれですか？","expected_sql":"SELECT DISTINCT item_code, product_name FROM gold_inventory_policy_breach WHERE breach_type='OVER'","role":"SCM企画,倉庫長","notes":"max超過"},
    {"example_id":"G006","question":"今週対応すべきアクションは？","expected_sql":"SELECT * FROM gold_action_queue_daily ORDER BY urgency_rank LIMIT 20","role":"調達,SCM企画","notes":""},
    {"example_id":"G007","question":"FCST精度の推移を見せてください","expected_sql":"SELECT month, forecast_accuracy_pct FROM gold_exec_summary_daily","role":"SCM企画","notes":""},
    {"example_id":"G008","question":"16週超のLTを持つ部品は？","expected_sql":"SELECT item_code, item_name, manufacturer_name, latest_lt_weeks FROM gold_lt_snapshot_current WHERE latest_lt_weeks > 16","role":"調達","notes":""},
]


def build_glossary_df():
    import pandas as pd
    return pd.DataFrame(GLOSSARY)

def build_metrics_df():
    import pandas as pd
    return pd.DataFrame(METRICS)

def build_genie_examples_df():
    import pandas as pd
    return pd.DataFrame(GENIE_EXAMPLES)
