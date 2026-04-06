# SCM判断支援アプリ — 現行仕様書

## 1. 概要

在庫過多の削減を主目的とし、欠品リスク・LT長期化・納期危険・在庫ポリシー逸脱を横断的に捉え、顧客側のSCM関係者が同じデータと同じ根拠で優先アクションを判断できる判断支援アプリ。

```
サマリー把握(5秒) → 対象絞り込み(30秒) → 根拠確認(3分) → 次アクション判断
```

### 技術スタック
- Streamlit (Multi-Page App) + Plotly (ダークテーマ) + Leaflet.js (地図)
- Databricks (Unity Catalog, Medallion Architecture, Apps, Genie)
- GitHub Git連携デプロイ

### 失敗条件
- 別のExcelが見られてこのアプリが使われなくなること
- KPIは出るが次アクション判断に繋がらないこと
- 部門ごとに数字の意味がずれて共通認識が作れないこと

---

## 2. 画面構成 (6モジュール)

| # | 画面名 | ファイル | 主な利用者 |
|---|--------|---------|-----------|
| A | 🏠 経営コントロールタワー | app.py | 経営層, SCM企画 |
| B | 📊 LTインテリジェンス | pages/1_lt_intelligence.py | 調達, SCM企画 |
| C | ⚖️ 納期コミット・需給バランス | pages/2_commit_supply_balance.py | 調達, 生産管理 |
| D | 📦 在庫基準逸脱レーダー | pages/3_inventory_policy.py | 生産管理, 倉庫長 |
| E | 🗺️ 拠点・倉庫健全性 | pages/4_network_warehouse.py | 倉庫長, SCM企画 |
| F | 🛠️ データ信頼性センター | pages/5_data_reliability.py | データ管理者 |

### 横断コンポーネント
- **Global Filter Bar** (components/global_filter.py): メーカー/カテゴリ/倉庫/危険度。画面間でフィルター状態を維持
- **Explain Panel** (components/explain_panel.py): 各KPI・各行の根拠を severity + rationale + action + due で表示
- **共通サイドバー** (components/sidebar.py): 全ページ統一の日本語ナビゲーション

---

## 3. Gold テーブル (14本)

| # | テーブル | 行数 | 用途 |
|---|---------|------|------|
| 1 | gold_exec_summary_daily | 1 | 経営KPIサマリー |
| 2 | gold_lt_snapshot_current | 90 | 最新LT + N-3/N-6比較 |
| 3 | gold_lt_trend_monthly | 3,330 | LT月次推移 |
| 4 | gold_lt_escalation_items | 10 | LT長期化アイテム |
| 5 | gold_order_commit_risk | 782 | オーダー納期危険度 |
| 6 | gold_requirement_timeline | 1,776 | 所要量一覧イベント |
| 7 | gold_balance_projection_monthly | 1,080 | 月末在庫予測 |
| 8 | gold_inventory_policy_breach | 442 | ZERO/UNDER/OVER判定 (6ヶ月) |
| 9 | gold_geo_warehouse_status | 10 | 倉庫ステータス |
| 10 | gold_data_pipeline_health | 12 | ETLパイプライン |
| 11 | gold_action_queue_daily | 37 | 今週の優先アクションキュー |
| 12 | gold_business_glossary | 19 | ドメイン用語辞書 |
| 13 | gold_metric_definition | 10 | メトリクス定義 |
| 14 | gold_genie_semantic_examples | 8 | Genieサンプルクエリ |

---

## 4. 判断ロジック

### 在庫ポリシー
```
ZERO:  在庫予測 ≤ 0
UNDER: 0 < 在庫予測 < min_stock
OVER:  在庫予測 > max_stock
OK:    min_stock ≤ 在庫予測 ≤ max_stock
```
- UI表示: デフォルト3ヶ月、オプション6ヶ月の二層

### オーダー優先度
```
Critical: 指定納期まで3日以内
High: 7日以内 / Mid: 14日以内 / Low: 14日超
```

### LT長期化
```
3ヶ月前(N-3) or 6ヶ月前(N-6)比でLT増加(↑)
```
前月比(N-1)は対象外

### 月次在庫プロジェクション
```
過去月: 在庫 = 現在スナップショット (変動なし)
未来月: 在庫[t+1] = 在庫[t] + PO入荷 - max(受注, FCST)
```

### 倉庫健全性
```
健全性 = (min/max基準を満たしている品目数 ÷ 管理品目数) × 100%
```
部品全体のmin/max vs 全倉庫合計在庫で判定

---

## 5. コード構成

```
scm/
├── app.py                          # 経営コントロールタワー
├── styles.py                       # ダークテーマCSS
├── app.yaml / requirements.txt     # Databricks Apps設定
├── .streamlit/config.toml          # Streamlit設定 (自動ナビ非表示)
│
├── pages/                          # 5ページ
│   ├── 1_lt_intelligence.py
│   ├── 2_commit_supply_balance.py
│   ├── 3_inventory_policy.py
│   ├── 4_network_warehouse.py
│   └── 5_data_reliability.py
│
├── logic/                          # ビジネスロジック
│   ├── gold_builder.py             # Gold 14テーブル生成
│   ├── glossary.py                 # 用語辞書 + メトリクス定義
│   ├── safety_stock.py             # 安全在庫計算
│   └── scoring.py                  # スコアリング
│
├── services/                       # データ取得
│   ├── config.py                   # デモ/Databricksモード切替
│   ├── database.py                 # Gold統一アクセスAPI
│   └── genie_client.py             # Genie抽象化
│
├── components/                     # UIコンポーネント
│   ├── sidebar.py                  # 共通サイドバー
│   ├── global_filter.py            # 全画面共通フィルター
│   ├── explain_panel.py            # 根拠説明パネル
│   └── japan_map.py                # Leaflet日本地図
│
├── notebooks/
│   └── 00_full_setup.py            # Databricks全自動セットアップ
│
├── data_generation/                # デモデータ生成 (Databricks不要)
│   ├── constants.py
│   └── gen_full.py
│
└── sample_data/                    # デモ用CSV (15ファイル)
```

Python 25ファイル、全29ファイル (データ/git除く)。

---

## 6. 現在のKPI値 (デモデータ)

| 指標 | 値 |
|------|-----|
| Critical Orders | 10件 |
| High Orders | 15件 |
| LT長期化品目 | 10品目 |
| ZERO予測 (6ヶ月) | 7品目 |
| UNDER予測 (6ヶ月) | 51品目 |
| OVER予測 (6ヶ月) | 56品目 |
| 倉庫平均健全性 | 77.2% |
| FCST精度 | 65.4% |

---

## 7. Databricksデプロイ

```
GitHub → Git Folder → 00_full_setup.py (Run All) → Databricks App (Path: scm)
環境変数: SCM_CATALOG=supply_chain_management, SCM_SCHEMA=main
```

### Databricks不要ファイル
- `data_generation/` — デモデータ生成用。Databricksでは不要
- `sample_data/` — デモモード用。Databricksでは Unity Catalog Gold を参照

---

## 8. 顧客視点の原則

- **主役は顧客在庫** (判断の中心)
- **商社在庫は補助情報** (引当可能在庫、補充余地)
- 全社名・製品名は**架空名** (デモ用)
- 「商社」= 当社
- read-only (閲覧・絞込・ドリルダウン・CSVエクスポートのみ)

---

## 9. Skills (.claude/)

| スキル | 用途 |
|--------|------|
| scm-databricks-app-architect | アーキテクチャ・画面設計・データモデル |
| dashboard-ux-governor | UXレビュー・チャート選定・アクセシビリティ |
| databricks-lakeflow-uc-implementer | Databricks実装・UC・パイプライン |
| genie-semantic-curator | Genie用語辞書・セマンティック・サンプルクエリ |
| app-quality-gate | リリース前品質チェック |
