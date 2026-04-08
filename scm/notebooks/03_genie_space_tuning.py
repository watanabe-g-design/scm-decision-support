# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 SCM — Genie Space チューニングガイド
# MAGIC
# MAGIC Genie の応答精度は **Genie Space の設定** で 90% 決まります。
# MAGIC このノートブックは Genie Space に登録すべき内容を **コピペ可能なテキスト**として
# MAGIC 出力します。Databricks の Genie Space 設定 UI に貼り付けてください。
# MAGIC
# MAGIC ## 設定する3項目
# MAGIC
# MAGIC | # | 項目 | 設定場所 | 効果 |
# MAGIC |---|------|---------|------|
# MAGIC | 1 | **General Instructions** | Genie Space → Settings → Instructions | LLM の振る舞いを制御 (逆質問抑制、用語統一など) |
# MAGIC | 2 | **Sample Queries** (例題) | Genie Space → Sample queries → Add | LLM が SQL 生成パターンを学習 |
# MAGIC | 3 | **テーブル/カラム コメント** | Catalog → 各テーブル → Columns | LLM がスキーマ意味を理解 |
# MAGIC
# MAGIC > ⚠️ Databricks SDK には **Genie Space 設定の編集 API がまだ公開されていません**。
# MAGIC > そのため UI で手動コピペが必要です。1 回だけの作業です。
# MAGIC
# MAGIC ## このノートブックでやること
# MAGIC - セル 1: General Instructions 全文を出力 (コピーして Settings に貼る)
# MAGIC - セル 2: Sample Queries を 12 個出力 (1 つずつ Sample queries に追加する)
# MAGIC - セル 3: テーブル/カラムコメントを SQL で**実際に Catalog に書き込む** (これは自動)

# COMMAND ----------
# MAGIC %md ## ① General Instructions (Settings → Instructions に貼る)
# MAGIC
# MAGIC 下のセルの実行結果をすべてコピーして、Genie Space の **Settings → Instructions** に貼り付けてください。

# COMMAND ----------

INSTRUCTIONS = """\
You are an SCM (Supply Chain Management) decision-support assistant for a
semiconductor trading company. Your job is to convert Japanese natural language
questions into Spark SQL against the curated Gold tables and return data.

# 振る舞いのルール
- 常に SQL を生成し実行してください。曖昧な質問でも最善の解釈で SQL を作ってください
- ユーザーに逆質問しないでください。曖昧でも 1 つの解釈で結果を返す方が価値があります
- データが存在しない場合は空の結果を返してください
- 結果の自然言語要約は最小限にしてください (1 文で十分)。テーブルそのものが答えです

# 主要 Gold テーブルとその役割
- gold_exec_summary_daily: 経営 KPI サマリー (Critical/High/Mid/Low Order 件数、ZERO/UNDER/OVER 品目数、倉庫健全性、FCST 精度)
- gold_order_commit_risk: 受注明細 + 納期リスク (priority_rank: Critical=3日以内, High=7日, Mid=14日, Low=14日超)
- gold_lt_snapshot_current: 部品ごとの最新リードタイム + N-1/N-3/N-6 月前比較
- gold_lt_escalation_items: LT が 3ヶ月前 or 6ヶ月前と比較して延長している部品
- gold_balance_projection_monthly: 月次の顧客在庫予測 (policy_status: ZERO/UNDER/OVER/OK)
- gold_inventory_policy_breach: 在庫ポリシー逸脱品目 (breach_type: ZERO/UNDER/OVER)
- gold_geo_warehouse_status: 倉庫別の在庫健全性 (health_score)
- gold_action_queue_daily: 統合アクションキュー (urgency_rank 順)
- silver_components: 部品マスタ (part_number, component_name, supplier_id, base_lead_time_weeks, min_stock, max_stock)
- silver_warehouses: 倉庫マスタ (warehouse_id, warehouse_name, prefecture)
- silver_suppliers: メーカー (サプライヤー) マスタ (supplier_id, supplier_name)

# ドメイン用語の正規化
- 「Critical Order / クリティカルオーダー / 緊急オーダー」 → priority_rank = 'Critical'
- 「LT / リードタイム / 納入リードタイム」 → lead_time_weeks (週単位)
- 「LT 長期化 / LT エスカレーション」 → trend_arrow_n3='↑' OR trend_arrow_n6='↑'
- 「在庫ゼロ / 欠品 / 在庫切れ」 → breach_type = 'ZERO'
- 「過剰在庫 / オーバー / 在庫過多」 → breach_type = 'OVER'
- 「安全在庫割れ / アンダー / 在庫不足」 → breach_type = 'UNDER'
- 「メーカー / サプライヤー / 供給元」 → supplier_name (silver_suppliers)
- 「品番 / 部品番号 / Part Number」 → part_number
- 「拠点 / 倉庫 / DC」 → warehouse_name

# クエリのデフォルト動作
- COUNT 系の質問は SELECT COUNT(*) を返してください
- 「上位 N 件」と言われたら ORDER BY ... LIMIT N
- 件数指定がない場合は LIMIT 50 をデフォルトにしてください
- 部品を返す場合は最低限 part_number, component_name を含めてください
- 倉庫を返す場合は warehouse_name と prefecture を含めてください
- メーカーを返す場合は supplier_name を含めてください

# 禁止事項
- 推測でテーブル名やカラム名を作らないでください
- 上記に列挙していないテーブルは参照しないでください
- ユーザーに「もっと詳しく教えて」と聞き返さないでください
"""

print("=" * 80)
print("以下を Genie Space → Settings → Instructions にコピペしてください")
print("=" * 80)
print(INSTRUCTIONS)
print("=" * 80)

# COMMAND ----------
# MAGIC %md ## ② Sample Queries (Genie Space → Sample queries → Add で 1 つずつ追加)
# MAGIC
# MAGIC 下のセルが出力する 12 個のサンプル質問と SQL を、Genie Space の
# MAGIC **Sample queries** タブで 1 つずつ Add してください。
# MAGIC
# MAGIC > 💡 Sample queries は Genie が SQL 生成パターンを学ぶ一番強力な手段です。
# MAGIC > ここを充実させると応答品質が劇的に上がります。

# COMMAND ----------

# CATALOG/SCHEMA は Genie Space が選択したテーブル経由で自動解決されるため、
# Sample queries の SQL では schema 名は不要 (素のテーブル名で OK)

SAMPLE_QUERIES = [
    {
        "question": "Critical Order は何件ありますか?",
        "sql": "SELECT COUNT(*) AS critical_count FROM gold_order_commit_risk WHERE priority_rank = 'Critical'",
    },
    {
        "question": "Critical Order の品番、部品名、顧客名、指定納期を教えてください",
        "sql": "SELECT sales_order_id, part_number, component_name, customer_name, requested_delivery_date FROM gold_order_commit_risk WHERE priority_rank = 'Critical' ORDER BY requested_delivery_date",
    },
    {
        "question": "メーカー別に Critical Order の件数を集計してください",
        "sql": "SELECT supplier_name, COUNT(*) AS critical_count FROM gold_order_commit_risk WHERE priority_rank = 'Critical' GROUP BY supplier_name ORDER BY critical_count DESC",
    },
    {
        "question": "在庫が ZERO 予測の部品の品番一覧を教えてください",
        "sql": "SELECT DISTINCT item_code AS part_number, product_name AS component_name FROM gold_inventory_policy_breach WHERE breach_type = 'ZERO' ORDER BY item_code",
    },
    {
        "question": "OVER (過剰在庫) になっている部品の数を教えてください",
        "sql": "SELECT COUNT(DISTINCT item_id) AS over_part_count FROM gold_inventory_policy_breach WHERE breach_type = 'OVER'",
    },
    {
        "question": "LT が 16 週を超える部品の品番と部品名を教えてください",
        "sql": "SELECT item_code AS part_number, item_name AS component_name, latest_lt_weeks FROM gold_lt_snapshot_current WHERE latest_lt_weeks > 16 ORDER BY latest_lt_weeks DESC",
    },
    {
        "question": "LT が長期化傾向にある部品の品番と現在 LT を教えてください",
        "sql": "SELECT item_code AS part_number, item_name AS component_name, latest_lt_weeks, lt_n3_weeks, lt_n6_weeks, trend_arrow_n3, trend_arrow_n6 FROM gold_lt_escalation_items ORDER BY latest_lt_weeks DESC",
    },
    {
        "question": "倉庫別の健全性スコアを教えてください",
        "sql": "SELECT warehouse_name, prefecture, health_score, zero_count, under_count, over_count FROM gold_geo_warehouse_status ORDER BY health_score",
    },
    {
        "question": "今週の優先アクション上位 10 件を教えてください",
        "sql": "SELECT urgency_rank, risk_type, item_code, item_name, recommended_action, due_date FROM gold_action_queue_daily ORDER BY urgency_rank LIMIT 10",
    },
    {
        "question": "メーカー別に LT が長期化している部品の数を集計してください",
        "sql": "SELECT supplier_name, COUNT(*) AS escalation_count FROM gold_lt_escalation_items GROUP BY supplier_name ORDER BY escalation_count DESC",
    },
    {
        "question": "経営サマリーを表示してください",
        "sql": "SELECT critical_count, high_count, lt_escalation_item_count, zero_count, under_count, over_count, warehouse_health_score, forecast_accuracy_pct FROM gold_exec_summary_daily",
    },
    {
        "question": "Renesas または該当メーカーの部品で LT が最も長いトップ 5 を教えてください",
        "sql": "SELECT item_code AS part_number, item_name AS component_name, manufacturer_name, latest_lt_weeks FROM gold_lt_snapshot_current ORDER BY latest_lt_weeks DESC LIMIT 5",
    },
]

print("=" * 80)
print(f"Sample Queries: {len(SAMPLE_QUERIES)} 個")
print("=" * 80)
for i, q in enumerate(SAMPLE_QUERIES, 1):
    print(f"\n{'─' * 78}")
    print(f"#{i}")
    print(f"質問:\n  {q['question']}")
    print(f"SQL:\n  {q['sql']}")
print("\n" + "=" * 80)

# COMMAND ----------
# MAGIC %md ## ③ Gold テーブル/カラムコメント自動付与 (これは自動実行)
# MAGIC
# MAGIC Genie は Catalog のテーブルコメントとカラムコメントを LLM コンテキストに含めます。
# MAGIC ここでドメイン語で日本語コメントを上書きすることで、Genie の理解度が大幅に上がります。

# COMMAND ----------

dbutils.widgets.text("catalog", "supply_chain_management", "カタログ名")
dbutils.widgets.text("schema",  "main",                    "スキーマ名")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")

print(f"対象: {CATALOG}.{SCHEMA}")

# テーブルコメント
TABLE_COMMENTS = {
    "gold_exec_summary_daily":
        "経営ダッシュボードの単一行 KPI サマリー。"
        "critical_count/high_count/mid_count/low_count は priority_rank 別の Order 件数。"
        "zero_count/under_count/over_count は在庫ポリシー逸脱品目数。"
        "warehouse_health_score は全倉庫の平均健全性 (0-100%)。"
        "forecast_accuracy_pct は直近3ヶ月の予測精度 (0-100%)。",

    "gold_order_commit_risk":
        "受注明細 × 顧客在庫 × メーカー情報を結合した納期コミットリスク評価テーブル。"
        "priority_rank: Critical(3日以内), High(7日以内), Mid(14日以内), Low(14日超)。"
        "adjustment_action: 緊急発注/商社前倒し調整/1週間後再確認/状況モニタリング。"
        "risk_score は 0-100 の数値で、納期接近度を表す。",

    "gold_lt_snapshot_current":
        "部品ごとの最新リードタイム + N-1/N-3/N-6 月前比較。"
        "trend_arrow_n3/n6 は ↑(延長) ↓(短縮) →(変化なし) の文字列。"
        "latest_lt_weeks は週単位の最新 LT。"
        "item_code は part_number、item_name は component_name、manufacturer_name は supplier_name と同義。",

    "gold_lt_trend_monthly":
        "部品別リードタイムの月次推移。Streamlit の時系列チャートで使用。"
        "month は yyyy-MM 形式、lead_time_weeks は週単位の LT。",

    "gold_lt_escalation_items":
        "LT が 3ヶ月前 (N-3) または 6ヶ月前 (N-6) 比較で延長 (↑) している部品。"
        "escalation_reason に延長の理由 (3ヶ月前比 or 6ヶ月前比) を記載。"
        "gold_lt_snapshot_current のサブセット。",

    "gold_balance_projection_monthly":
        "部品ごとの月次顧客在庫予測。過去3ヶ月 + 将来 (2026-11 まで)。"
        "customer_stock_proj は予測在庫数量、min_qty/max_qty は安全在庫/上限。"
        "policy_status: ZERO(在庫0以下) / UNDER(min割れ) / OVER(max超過) / OK。"
        "item_code は part_number、product_name は component_name と同義。",

    "gold_inventory_policy_breach":
        "在庫ポリシーを逸脱している品目 (gold_balance_projection_monthly のサブセット)。"
        "breach_type: ZERO/UNDER/OVER。breach_date は逸脱発生月 (yyyy-MM)。"
        "first_breach は同部品の最初の逸脱月。priority_order: ZERO=1, UNDER=2, OVER=3。",

    "gold_geo_warehouse_status":
        "倉庫 (拠点) 別の在庫健全性スマリー。"
        "health_score = (OK 品目数 / 管理品目数) × 100 (0-100%)。"
        "geo_lat/geo_lon は地図表示用座標。zero_count/under_count/over_count は当該倉庫が管理する品目のうち各ステータスに該当する品目数。",

    "gold_data_pipeline_health":
        "Lakeflow Pipeline が取り込んだ Bronze テーブルのメタデータ。"
        "record_count は行数、quality_score は品質スコア (0-100)、success_flag はブール。",

    "gold_action_queue_daily":
        "Critical/High Order + LT 長期化 + 在庫 ZERO ブリーチを統合した優先アクションキュー。"
        "urgency_rank 昇順 (1=最優先)。risk_type で発生源 (納期Critical/LT長期化/在庫ZERO予測) を識別。"
        "recommended_action に推奨対応、rationale に判定根拠が入る。",

    "gold_business_glossary":
        "SCM ドメイン用語辞書。Genie の用語統一に使用。",

    "gold_metric_definition":
        "各 KPI の名称、計算式、単位、出典 Gold テーブル、表示画面のメタデータ。",

    "gold_genie_semantic_examples":
        "Genie 用のサンプル質問と期待 SQL のメタデータ (アプリ側で参照)。",
}

COL_COMMENTS = [
    # gold_order_commit_risk
    ("gold_order_commit_risk", "priority_rank", "納期リスク優先度: Critical(3日以内), High(7日以内), Mid(14日以内), Low(14日超)"),
    ("gold_order_commit_risk", "adjustment_action", "推奨される調整アクション (緊急発注/商社前倒し調整/1週間後再確認/状況モニタリング)"),
    ("gold_order_commit_risk", "risk_score", "納期リスクスコア 0-100 (100 が最も緊急)"),
    ("gold_order_commit_risk", "current_customer_stock", "顧客側の現在在庫数量"),
    ("gold_order_commit_risk", "supplier_name", "メーカー名 (sub-supplier ではなく直接の供給元)"),
    ("gold_order_commit_risk", "part_number", "部品番号 (品番)"),
    ("gold_order_commit_risk", "component_name", "部品名 (日本語)"),
    # gold_lt_snapshot_current
    ("gold_lt_snapshot_current", "latest_lt_weeks", "現在のリードタイム (週単位)"),
    ("gold_lt_snapshot_current", "lt_n3_weeks", "3ヶ月前のリードタイム (週単位)"),
    ("gold_lt_snapshot_current", "lt_n6_weeks", "6ヶ月前のリードタイム (週単位)"),
    ("gold_lt_snapshot_current", "trend_arrow_n3", "3ヶ月前比のトレンド: ↑延長 ↓短縮 →変化なし"),
    ("gold_lt_snapshot_current", "trend_arrow_n6", "6ヶ月前比のトレンド: ↑延長 ↓短縮 →変化なし"),
    ("gold_lt_snapshot_current", "lt_band", "LT バンド分類: 13週以内/14週〜半年/半年〜1年/1年〜1.5年/1.5年〜2年"),
    ("gold_lt_snapshot_current", "item_code", "部品番号 (= part_number)"),
    ("gold_lt_snapshot_current", "item_name", "部品名 (= component_name)"),
    ("gold_lt_snapshot_current", "manufacturer_name", "メーカー名 (= supplier_name)"),
    # gold_inventory_policy_breach
    ("gold_inventory_policy_breach", "breach_type", "逸脱種別: ZERO(在庫0以下) UNDER(安全在庫割れ) OVER(上限超過)"),
    ("gold_inventory_policy_breach", "breach_date", "逸脱発生月 (yyyy-MM 形式)"),
    ("gold_inventory_policy_breach", "projected_stock", "逸脱発生時点の予測在庫数量"),
    # gold_geo_warehouse_status
    ("gold_geo_warehouse_status", "health_score", "倉庫健全性スコア 0-100 (100 が最も健全)"),
    ("gold_geo_warehouse_status", "warehouse_name", "倉庫名 (拠点名)"),
    ("gold_geo_warehouse_status", "prefecture", "倉庫所在の都道府県"),
]

print("\n📝 テーブルコメントを付与中...")
for tname, comment in TABLE_COMMENTS.items():
    try:
        spark.sql(
            f"COMMENT ON TABLE `{CATALOG}`.`{SCHEMA}`.`{tname}` IS '{comment}'"
        )
        print(f"  ✅ {tname}")
    except Exception as e:
        print(f"  ⚠️ {tname}: {e}")

print("\n📝 カラムコメントを付与中...")
for tname, cname, comment in COL_COMMENTS:
    try:
        spark.sql(
            f"ALTER TABLE `{CATALOG}`.`{SCHEMA}`.`{tname}` "
            f"ALTER COLUMN `{cname}` COMMENT '{comment}'"
        )
        print(f"  ✅ {tname}.{cname}")
    except Exception as e:
        print(f"  ⚠️ {tname}.{cname}: {e}")

print("\n✅ コメント付与完了。Genie が次の質問から使えるようになります。")

# COMMAND ----------
# MAGIC %md ## 完了後の検証
# MAGIC
# MAGIC 1. Genie Space を開いて以下の質問を試す:
# MAGIC    - 「Critical Order は何件?」 → 数字 1 つが返るか
# MAGIC    - 「LT が 16 週を超える部品は?」 → テーブルが返るか
# MAGIC    - 「メーカー別 LT 長期化件数」 → 集計テーブルが返るか
# MAGIC 2. もし逆質問が返ってくる場合: Settings → Instructions の貼り付けが正しいか確認
# MAGIC 3. もし誤ったテーブルを参照する場合: 該当テーブルのコメントが正しく付いているか Catalog で確認
