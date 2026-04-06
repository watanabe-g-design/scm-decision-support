# Databricks notebook source
# MAGIC %md
# MAGIC # 🗄️ SCM — カタログ / ボリューム / CSV セットアップ
# MAGIC
# MAGIC Lakeflow Declarative Pipeline を実行する**前に 1 回だけ**走らせるノートブックです。
# MAGIC
# MAGIC ## このノートブックの役割
# MAGIC | ステップ | 内容 |
# MAGIC |---------|------|
# MAGIC | 1 | Unity Catalog `supply_chain_management` と schema `main` を作成 |
# MAGIC | 2 | Volume `scm_data` を作成 |
# MAGIC | 3 | Git Folder 内の 15 個の CSV を Volume にコピー |
# MAGIC | 4 | config.json の雛形を Volume に書き出し (catalog/schema のみ) |
# MAGIC
# MAGIC **Bronze / Silver / Gold テーブルはこのノートブックでは作りません。**
# MAGIC 代わりに `databricks bundle deploy` して Lakeflow Pipeline を実行してください。
# MAGIC
# MAGIC ## 実行後の手順
# MAGIC 1. ローカルで `databricks bundle deploy -t dev` を実行
# MAGIC 2. Workflows > Pipelines から `scm_decision_support_pipeline` を手動実行
# MAGIC 3. パイプライン成功後、`01_create_genie` ノートブックを実行して Genie スペースを作成
# MAGIC 4. Databricks App を Redeploy

# COMMAND ----------
# MAGIC %md ## 📝 パラメータ

# COMMAND ----------

dbutils.widgets.text("catalog", "supply_chain_management", "① カタログ名")
dbutils.widgets.text("schema",  "main",                    "② スキーマ名")

CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
VOLUME_NAME = "scm_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

print("=" * 60)
print("  SCM カタログセットアップ")
print("=" * 60)
print(f"  カタログ   : {CATALOG}")
print(f"  スキーマ   : {SCHEMA}")
print(f"  ボリューム : {VOLUME_PATH}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md ## ステップ 1/4: Unity Catalog リソース作成

# COMMAND ----------

print("📦 Unity Catalog リソースを作成中...")

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
print(f"  ✅ カタログ `{CATALOG}` 作成完了")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SCHEMA}`")
print(f"  ✅ スキーマ `{SCHEMA}` 作成完了")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME_NAME}`")
print(f"  ✅ ボリューム `{VOLUME_NAME}` 作成完了")

# COMMAND ----------
# MAGIC %md ## ステップ 2/4: CSV ファイルを Volume にコピー
# MAGIC
# MAGIC Git Folder (Workspace) 内の `scm/sample_data/` にある 15 個の CSV を
# MAGIC `{VOLUME_PATH}/csv/` にコピーします。

# COMMAND ----------

import os
import shutil

print("📂 CSV ファイルを Volume にコピー中...")

# Resolve sample_data path from the notebook's own location
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(f"  ノートブックパス: {notebook_path}")

# /Workspace/Users/.../scm-decision-support/scm/notebooks/00_setup_catalog
#  → parent x 2 = /Workspace/.../scm
repo_scm_path = str(os.path.dirname(os.path.dirname(f"/Workspace{notebook_path}")))
candidates = [
    f"{repo_scm_path}/sample_data",
    f"/Workspace{notebook_path}".rsplit("/", 2)[0] + "/sample_data",
]

sample_data_path = None
for cand in candidates:
    if os.path.isdir(cand):
        sample_data_path = cand
        break

if sample_data_path is None:
    print("  ❌ sample_data フォルダが見つかりません")
    print(f"  候補: {candidates}")
    print(f"  手動アップロード: Catalog → {CATALOG} → {SCHEMA} → Volumes → {VOLUME_NAME} → csv/")
    dbutils.notebook.exit("CSV source not found")

print(f"  📁 ソース: {sample_data_path}")

csv_dest = f"{VOLUME_PATH}/csv"
os.makedirs(csv_dest, exist_ok=True)

csv_files = sorted([f for f in os.listdir(sample_data_path) if f.endswith(".csv")])
for csv_file in csv_files:
    src = f"{sample_data_path}/{csv_file}"
    dst = f"{csv_dest}/{csv_file}"
    shutil.copyfile(src, dst)
    size_kb = os.path.getsize(src) / 1024
    print(f"  📄 {csv_file:<30} → Volume ({size_kb:>7.1f} KB)")

print(f"\n  ✅ {len(csv_files)} 個の CSV をコピー完了")

# COMMAND ----------
# MAGIC %md ## ステップ 3/4: config.json 雛形を書き出し
# MAGIC
# MAGIC Streamlit アプリが Volume から読み込む設定ファイルです。
# MAGIC `genie_space_id` と `warehouse_id` は `01_create_genie` ノートブックで
# MAGIC 後から上書きされます。

# COMMAND ----------

import json

config = {
    "catalog":        CATALOG,
    "schema":         SCHEMA,
    "warehouse_id":   None,   # filled by 01_create_genie
    "genie_space_id": None,   # filled by 01_create_genie
}

config_path = f"{VOLUME_PATH}/config.json"
dbutils.fs.put(config_path, json.dumps(config, ensure_ascii=False, indent=2), overwrite=True)

print(f"📄 config.json を書き出しました: {config_path}")
print(json.dumps(config, ensure_ascii=False, indent=2))

# COMMAND ----------
# MAGIC %md ## ステップ 4/4: 完了メッセージと次のステップ

# COMMAND ----------

print("=" * 60)
print("  ✅ カタログセットアップ完了!")
print("=" * 60)
print(f"""
次の手順:

 1. ローカル PC で:
      cd scm
      databricks bundle validate -t dev
      databricks bundle deploy   -t dev

 2. Workflows > Pipelines で 'scm_decision_support_pipeline' を手動実行
    (Unity Catalog の Lineage グラフが自動で生成されます)

 3. パイプライン成功後、このワークスペースの
      {notebook_path.rsplit('/', 1)[0]}/01_create_genie
    ノートブックを実行して Genie スペースを作成

 4. Databricks App を Redeploy
""")
