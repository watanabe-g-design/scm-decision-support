# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 SCM — Databricks App 自動作成
# MAGIC
# MAGIC `01_create_genie` が完了した**後に**実行するノートブックです。
# MAGIC
# MAGIC ## このノートブックの役割
# MAGIC | ステップ | 内容 |
# MAGIC |---------|------|
# MAGIC | 0 | databricks-sdk を最新版にアップグレード (Apps API 対応版) |
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
# MAGIC %md ## ステップ 0/5: databricks-sdk をアップグレード
# MAGIC
# MAGIC クラスター既定の SDK には `databricks.sdk.service.apps` が含まれていない場合があるため、
# MAGIC 最新版をインストールして Python を再起動します。

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "databricks-sdk>=0.40.0"

# COMMAND ----------

dbutils.library.restartPython()

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

# notebook_path は /Workspace プレフィックスがある場合とない場合の両方ありうる
# 例:
#   /Users/foo/scm-decision-support/scm/notebooks/02_create_app
#   /Workspace/Users/foo/scm-decision-support/scm/notebooks/02_create_app
nb_path_norm = notebook_path
if nb_path_norm.startswith("/Workspace/"):
    nb_path_norm = nb_path_norm[len("/Workspace"):]

# notebooks/02_create_app から 2 階層上が scm/ ディレクトリ
scm_logical_path = nb_path_norm.rsplit("/", 2)[0]  # /Users/foo/scm-decision-support/scm

# Apps API に渡すソースパスは /Workspace プレフィックス付きの形
# (Apps プラットフォームは Workspace API 経由で読むので、ローカル FS の存在チェックは不要)
workspace_source = "/Workspace" + scm_logical_path

print(f"  App ソースパス (Apps API 用): {workspace_source}")
print(f"  論理パス (Workspace API 用):  {scm_logical_path}")

# Workspace API 経由でファイル存在を確認 (ローカル FS の os.path より信頼性が高い)
from databricks.sdk import WorkspaceClient as _PreCheckClient
_w_check = _PreCheckClient()


def _ws_file_exists(ws_path: str) -> bool:
    """/Users/... 形式の Workspace パスにファイルが存在するか確認"""
    try:
        info = _w_check.workspace.get_status(path=ws_path)
        return info is not None
    except Exception:
        return False


required_files = ["app.yaml", "app.py", "requirements.txt"]
missing = []
for fname in required_files:
    ws_file_path = f"{scm_logical_path}/{fname}"
    if _ws_file_exists(ws_file_path):
        print(f"  ✅ {fname} を確認 ({ws_file_path})")
    else:
        # ローカル FS でも一応試す (新しい Databricks ランタイムでは見える場合がある)
        if os.path.isfile(f"{workspace_source}/{fname}"):
            print(f"  ✅ {fname} を確認 (FUSE 経由)")
        else:
            missing.append(fname)
            print(f"  ⚠️ {fname} が見つかりません ({ws_file_path})")

if missing:
    print()
    print("⚠️ 必須ファイルが見つかりませんでした:", missing)
    print("以下を確認してください:")
    print("  1. Workspace > Git Folder で最新を Pull したか")
    print("  2. ファイルが scm/ 直下にあるか")
    print()
    print("ただし Apps プラットフォーム側では問題なく読める可能性があるため、")
    print("チェックを警告として扱い、デプロイは継続します。")
    print()

print("  ➡️  Apps デプロイへ進みます")

# COMMAND ----------
# MAGIC %md ## ステップ 2/5: App を作成 (冪等)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App

w = WorkspaceClient()

# 既存チェック (存在しない場合のみ NotFound 系の例外を捕捉)
existing = None
try:
    existing = w.apps.get(name=APP_NAME)
except Exception as e:
    err_text = str(e).lower()
    if "not_found" not in err_text and "does not exist" not in err_text and "404" not in err_text:
        # NotFound 以外の例外は再 raise (権限エラーなどを握り潰さない)
        print(f"  ⚠️ apps.get で予期しない例外: {e}")

if existing is not None:
    state = "unknown"
    try:
        if getattr(existing, "compute_status", None):
            state = str(existing.compute_status.state)
    except Exception:
        pass
    print(f"  ℹ️  App '{APP_NAME}' は既に存在します (status: {state})")
    app = existing
else:
    print(f"  🆕 App '{APP_NAME}' を新規作成中...")
    app = w.apps.create_and_wait(
        app=App(
            name=APP_NAME,
            description="SCM 判断支援アプリ (Lakeflow DLT + Unity Catalog)",
        )
    )
    print(f"  ✅ 作成完了")

# サービスプリンシパル情報を取得 (次のステップで GRANT に使う)
# SDK バージョンによって属性名が異なるため getattr で安全に取得
service_principal = (
    getattr(app, "service_principal_client_id", None)
    or getattr(app, "service_principal_id", None)
    or getattr(app, "service_principal_name", None)
)
app_url = getattr(app, "url", None) or "(URL は初回 deploy 後に確定)"
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
