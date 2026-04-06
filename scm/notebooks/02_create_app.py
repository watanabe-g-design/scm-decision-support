# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 SCM — Databricks App 自動作成
# MAGIC
# MAGIC `01_create_genie` が完了した**後に**実行するノートブックです。
# MAGIC
# MAGIC ## このノートブックの役割
# MAGIC | ステップ | 内容 |
# MAGIC |---------|------|
# MAGIC | 1 | Git Folder から `scm` サブディレクトリを App のソースとして登録 |
# MAGIC | 2 | App `scm-decision-support` を作成 (存在する場合はスキップ) |
# MAGIC | 3 | ソースコードを deploy |
# MAGIC | 4 | サービスプリンシパルに UC 権限を付与 |
# MAGIC | 5 | アプリ URL を表示 |
# MAGIC
# MAGIC ## 前提
# MAGIC - `00_setup_catalog` と Lakeflow Pipeline が完了している
# MAGIC - `01_create_genie` が完了し、Genie スペースが存在する
# MAGIC - このノートブックを Git Folder 経由で開いている (ローカルパスではなく Workspace パス)
# MAGIC - 環境変数は `scm/app.yaml` に定義済み (SCM_CATALOG, SCM_SCHEMA)

# COMMAND ----------

dbutils.widgets.text("catalog",  "supply_chain_management", "① カタログ名")
dbutils.widgets.text("schema",   "main",                    "② スキーマ名")
dbutils.widgets.text("app_name", "scm-decision-support",    "③ App 名")

CATALOG  = dbutils.widgets.get("catalog")
SCHEMA   = dbutils.widgets.get("schema")
APP_NAME = dbutils.widgets.get("app_name")

print("=" * 60)
print("  SCM Databricks App 作成")
print("=" * 60)
print(f"  カタログ : {CATALOG}")
print(f"  スキーマ : {SCHEMA}")
print(f"  App 名   : {APP_NAME}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md ## ステップ 1/5: ソースコードパスを解決
# MAGIC
# MAGIC このノートブック自体のパスから遡って、Git Folder 内の `scm` ディレクトリを特定します。

# COMMAND ----------

import os

notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
print(f"  ノートブックパス: {notebook_path}")

# /Users/.../scm-decision-support/scm/notebooks/02_create_app
#   → parent x 2 = /Users/.../scm-decision-support/scm
workspace_source = "/Workspace" + notebook_path.rsplit("/", 2)[0]
print(f"  App ソースパス : {workspace_source}")

if not os.path.isdir(workspace_source):
    raise RuntimeError(f"ソースパスが見つかりません: {workspace_source}")

if not os.path.isfile(f"{workspace_source}/app.yaml"):
    raise RuntimeError(f"app.yaml が見つかりません: {workspace_source}/app.yaml")

if not os.path.isfile(f"{workspace_source}/app.py"):
    raise RuntimeError(f"app.py が見つかりません: {workspace_source}/app.py")

print("  ✅ app.yaml と app.py を確認しました")

# COMMAND ----------
# MAGIC %md ## ステップ 2/5: App を作成 (冪等)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App

w = WorkspaceClient()

# 既存チェック
try:
    existing = w.apps.get(name=APP_NAME)
    print(f"  ℹ️  App '{APP_NAME}' は既に存在します (status: {existing.compute_status.state if existing.compute_status else 'unknown'})")
    app = existing
except Exception:
    print(f"  🆕 App '{APP_NAME}' を新規作成中...")
    app = w.apps.create_and_wait(
        app=App(
            name=APP_NAME,
            description="SCM 判断支援アプリ (Lakeflow DLT + Unity Catalog)",
        )
    )
    print(f"  ✅ 作成完了")

# サービスプリンシパルIDを取得 (次のステップでGRANTに使う)
service_principal = app.service_principal_client_id or app.service_principal_id
app_url = app.url
print(f"  Service Principal: {service_principal}")
print(f"  App URL          : {app_url}")

# COMMAND ----------
# MAGIC %md ## ステップ 3/5: ソースコードを Deploy

# COMMAND ----------

from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode

print(f"🚀 App をデプロイ中... (ソース: {workspace_source})")

deployment = w.apps.deploy_and_wait(
    app_name=APP_NAME,
    app_deployment=AppDeployment(
        source_code_path=workspace_source,
        mode=AppDeploymentMode.SNAPSHOT,
    ),
)

print(f"  ✅ デプロイ完了")
print(f"  Deployment ID: {deployment.deployment_id}")
print(f"  Status       : {deployment.status.state if deployment.status else 'unknown'}")

# COMMAND ----------
# MAGIC %md ## ステップ 4/5: サービスプリンシパルに UC 権限を付与

# COMMAND ----------

# App のサービスプリンシパル名を取得
# Databricks Apps のサービスプリンシパルは app.service_principal_name または
# get() の結果に含まれる。最新の情報で取り直す。
app_fresh = w.apps.get(name=APP_NAME)
sp_name = (
    getattr(app_fresh, "service_principal_name", None)
    or getattr(app_fresh, "service_principal_client_id", None)
    or getattr(app_fresh, "service_principal_id", None)
)

if sp_name is None:
    print("⚠️ サービスプリンシパル名が取得できませんでした。以下のSQLを手動実行してください:")
    sp_display = "<サービスプリンシパル名>"
else:
    print(f"  Service Principal: {sp_name}")
    sp_display = str(sp_name)

grants = [
    f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO `{sp_display}`",
    f"GRANT USE SCHEMA  ON SCHEMA  `{CATALOG}`.`{SCHEMA}` TO `{sp_display}`",
    f"GRANT SELECT      ON SCHEMA  `{CATALOG}`.`{SCHEMA}` TO `{sp_display}`",
    f"GRANT READ VOLUME ON VOLUME  `{CATALOG}`.`{SCHEMA}`.`scm_data` TO `{sp_display}`",
]

print("\n🔐 権限を付与中...")
for g in grants:
    try:
        spark.sql(g)
        print(f"  ✅ {g}")
    except Exception as e:
        print(f"  ⚠️  {g}")
        print(f"      → {e}")

# COMMAND ----------
# MAGIC %md ## ステップ 5/5: 完了メッセージ

# COMMAND ----------

final = w.apps.get(name=APP_NAME)
state = final.compute_status.state if final.compute_status else "unknown"

print("=" * 60)
print("  ✅ Databricks App デプロイ完了")
print("=" * 60)
print(f"  App 名      : {APP_NAME}")
print(f"  Status      : {state}")
print(f"  URL         : {final.url}")
print(f"  ソース      : {workspace_source}")
print("=" * 60)
print("""
次の手順:
 1. 上記の URL をブラウザで開く
 2. トップ画面「経営コントロールタワー」が表示されれば完了
 3. エラーが出たら Databricks 左サイドバー > Apps > scm-decision-support > Logs でログ確認
""")
