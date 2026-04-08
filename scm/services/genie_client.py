"""
Genie / 生成AI 抽象化レイヤー
================================
**設計方針** (2026-04 改訂):
- LLM の自然言語要約はスキップし、SQL 実行結果の生 DataFrame を返す
- Genie が逆質問してきた場合 (= SQL を生成できなかった) は status='ng' を返す
- これにより応答時間を短縮し、Genie 本来の「データを返す」役割に絞る

将来 Deep Research Agent SDK が公開されたら query_research() を追加する設計
"""
import streamlit as st
import pandas as pd
from services.config import load_config, is_databricks_mode


class GenieClient:
    """Genie/AI abstraction - 将来的に API 差し替え可能"""

    def __init__(self):
        self._config = load_config()
        self._space_id = self._config.get("genie_space_id")

    @property
    def is_available(self) -> bool:
        return is_databricks_mode() and bool(self._space_id)

    # ── 内部ヘルパー ──────────────────────────────
    @staticmethod
    def _normalize_rows(raw_rows, schema_cols):
        """Statement Execution API の data_array をパースして DataFrame にする。
        SDK バージョン差分 (list[list[str]] vs list[Row]) を吸収。
        スキーマの type_name を見て正しい dtype に変換する。
        """
        rows = []
        for row in (raw_rows or []):
            if hasattr(row, "values"):
                rows.append([
                    cell.str_value if hasattr(cell, "str_value") else cell
                    for cell in row.values
                ])
            else:
                rows.append(list(row))

        cols = [c.name for c in schema_cols]
        df = pd.DataFrame(rows, columns=cols)

        INT_TYPES   = {"INT", "INTEGER", "BIGINT", "LONG", "SHORT", "SMALLINT", "TINYINT", "BYTE"}
        FLOAT_TYPES = {"DOUBLE", "FLOAT", "DECIMAL", "REAL"}
        BOOL_TYPES  = {"BOOLEAN", "BOOL"}
        DATE_TYPES  = {"DATE"}
        TS_TYPES    = {"TIMESTAMP", "TIMESTAMP_NTZ"}

        for col in schema_cols:
            type_name = getattr(col, "type_name", None)
            if type_name is None:
                continue
            if hasattr(type_name, "value"):
                type_str = str(type_name.value).upper()
            elif hasattr(type_name, "name"):
                type_str = str(type_name.name).upper()
            else:
                type_str = str(type_name).upper()

            cname = col.name
            if cname not in df.columns:
                continue

            try:
                if type_str in INT_TYPES:
                    df[cname] = pd.to_numeric(df[cname], errors="coerce").astype("Int64")
                elif type_str in FLOAT_TYPES:
                    df[cname] = pd.to_numeric(df[cname], errors="coerce")
                elif type_str in BOOL_TYPES:
                    df[cname] = df[cname].map(
                        lambda v: True if str(v).lower() == "true"
                        else (False if str(v).lower() == "false" else None)
                    )
                elif type_str in DATE_TYPES:
                    df[cname] = pd.to_datetime(df[cname], errors="coerce").dt.date
                elif type_str in TS_TYPES:
                    df[cname] = pd.to_datetime(df[cname], errors="coerce")
            except Exception:
                pass

        return df

    @staticmethod
    def _extract_query_result(result_obj):
        """SDK の get_message_attachment_query_result() レスポンスから
        DataFrame を取り出す。属性名のバージョン差を吸収。
        """
        # 多くのバージョン: result.statement_response.{manifest, result.data_array}
        stmt = (
            getattr(result_obj, "statement_response", None)
            or getattr(result_obj, "query_result", None)
            or result_obj
        )
        if stmt is None:
            return None

        manifest = getattr(stmt, "manifest", None)
        result = getattr(stmt, "result", None)
        if not manifest or not result:
            return None

        schema = getattr(manifest, "schema", None)
        if not schema:
            return None

        cols = getattr(schema, "columns", None) or []
        rows = getattr(result, "data_array", None) or []
        if not cols:
            return None

        return GenieClient._normalize_rows(rows, cols)

    # ── 公開メソッド ──────────────────────────────
    def query(self, prompt: str, context: dict = None) -> dict:
        """
        Genie に質問を送信し、SQL 実行結果の DataFrame を取得する

        Returns:
            {
              "status":     "ok" | "no_data" | "ng" | "error",
              "data":       DataFrame | None,
              "sql":        str | None,
              "message":    str,                # ユーザー向け一行メッセージ
              "genie_text": str | None,         # Genie が返した自然言語テキスト (生)
              "raw_message": dict | None,       # Genie の生レスポンス (デバッグ用)
              "elapsed":    float,
              "error":      str | None,
            }
        """
        import time
        start = time.monotonic()

        def _result(status, data=None, sql=None, message="", genie_text=None,
                    raw_message=None, error=None):
            return {
                "status":      status,
                "data":        data,
                "sql":         sql,
                "message":     message,
                "genie_text":  genie_text,
                "raw_message": raw_message,
                "elapsed":     round(time.monotonic() - start, 2),
                "error":       error,
            }

        if not self.is_available:
            return _result(
                "error",
                message="Genie 未接続: SCM_GENIE_SPACE_ID 環境変数を設定してください",
                error="not_available",
            )

        # コンテキスト注入
        enriched_prompt = prompt
        if context and context.get("warehouse_ids"):
            wh_list = ", ".join(context["warehouse_ids"])
            enriched_prompt += f"\n\n[コンテキスト: 対象倉庫 = {wh_list}]"

        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()

            # Step 1: 会話開始 → Genie が SQL を生成して実行
            conv = w.genie.start_conversation_and_wait(
                space_id=self._space_id,
                content=enriched_prompt,
            )

            conv_id = getattr(conv, "conversation_id", None) or (
                conv.conversation.id if getattr(conv, "conversation", None) else None
            )
            msg_id = getattr(conv, "message_id", None) or getattr(conv, "id", None)
            if not conv_id or not msg_id:
                if hasattr(conv, "messages") and conv.messages:
                    m = conv.messages[-1]
                    conv_id = conv_id or getattr(m, "conversation_id", None)
                    msg_id  = msg_id  or getattr(m, "id", None)

            if not (conv_id and msg_id):
                return _result(
                    "ng",
                    message="🔴 NG: Genie からメッセージ ID を取得できませんでした",
                )

            # Step 2: メッセージ詳細を取得して attachments を解析
            msg = w.genie.get_message(
                space_id=self._space_id,
                conversation_id=conv_id,
                message_id=msg_id,
            )

            # 生レスポンスを dict に変換 (デバッグ用)
            try:
                raw_msg = msg.as_dict() if hasattr(msg, "as_dict") else None
            except Exception:
                raw_msg = None

            sql_content = None
            attachment_id = None
            text_fallback = ""

            for att in (msg.attachments or []):
                # 自然言語の逆質問テキストがある場合 (= SQL 生成失敗のサイン)
                if hasattr(att, "text") and att.text:
                    txt = getattr(att.text, "content", None)
                    if txt and not text_fallback:
                        text_fallback = txt
                # SQL クエリの添付がある場合
                if hasattr(att, "query") and att.query:
                    sql_content = getattr(att.query, "query", None) or sql_content
                    attachment_id = (
                        getattr(att, "attachment_id", None)
                        or getattr(att, "id", None)
                        or attachment_id
                    )

            # Step 3a: SQL 添付がない → 逆質問のみ = NG (Genie のテキストはそのまま返す)
            if not sql_content:
                return _result(
                    "ng",
                    genie_text=text_fallback,
                    raw_message=raw_msg,
                    message=(
                        "🔴 **Genie が SQL を生成できませんでした。**\n\n"
                        "Genie の応答 (下記) を読んで、より具体的に質問し直してください。\n"
                        "Genie Space の Instructions / Sample queries が未設定だと曖昧な質問は逆質問になります。"
                    ),
                )

            # Step 3b: SQL 結果を取得
            df = None
            try:
                # 新しい SDK: get_message_attachment_query_result
                if attachment_id and hasattr(w.genie, "get_message_attachment_query_result"):
                    qr = w.genie.get_message_attachment_query_result(
                        space_id=self._space_id,
                        conversation_id=conv_id,
                        message_id=msg_id,
                        attachment_id=attachment_id,
                    )
                    df = self._extract_query_result(qr)

                # 旧 SDK フォールバック: get_message_query_result
                if df is None and hasattr(w.genie, "get_message_query_result"):
                    qr = w.genie.get_message_query_result(
                        space_id=self._space_id,
                        conversation_id=conv_id,
                        message_id=msg_id,
                    )
                    df = self._extract_query_result(qr)
            except Exception as e:
                # 取得失敗 → SQL を Statement Execution で直接実行する最終手段
                try:
                    cfg = self._config
                    res = w.statement_execution.execute_statement(
                        statement=sql_content,
                        warehouse_id=cfg["warehouse_id"],
                        wait_timeout="30s",
                    )
                    if res.status.state.value == "SUCCEEDED" and res.manifest and res.result:
                        df = self._normalize_rows(
                            res.result.data_array, res.manifest.schema.columns
                        )
                except Exception as e2:
                    return _result(
                        "error",
                        sql=sql_content,
                        message=f"⚠️ SQL 結果取得に失敗: {e2}",
                        error=str(e),
                    )

            # Step 4: 判定
            if df is None:
                return _result(
                    "ng",
                    sql=sql_content,
                    genie_text=text_fallback,
                    raw_message=raw_msg,
                    message="🔴 NG: SQL は生成されましたが結果を取得できませんでした",
                )
            if len(df) == 0:
                return _result(
                    "no_data",
                    data=df,
                    sql=sql_content,
                    genie_text=text_fallback,
                    raw_message=raw_msg,
                    message="🟡 該当データなし",
                )
            return _result(
                "ok",
                data=df,
                sql=sql_content,
                genie_text=text_fallback,
                raw_message=raw_msg,
                message=f"✅ {len(df)} 件",
            )

        except Exception as e:
            return _result(
                "error",
                message=f"⚠️ Genie 呼び出しエラー",
                error=f"{type(e).__name__}: {e}",
            )

    def generate_summary(self, data_context: str) -> str:
        return f"[AI要約] {data_context[:200]}..."


# サンプル質問集 — 具体的で SQL に変換しやすいものに統一
SAMPLE_QUERIES = [
    "Critical Order は何件ありますか?",
    "LT が 16 週を超える部品の品番と部品名を教えてください",
    "在庫が ZERO 予測の部品の品番一覧を教えてください",
    "メーカー別に Critical Order の件数を集計してください",
    "倉庫別の健全性スコアを教えてください",
    "OVER (過剰在庫) になっている部品の数を教えてください",
    "今週の優先アクション上位 10 件を教えてください",
    "LT 長期化品目の品番と現在 LT を教えてください",
]


def get_genie_client() -> GenieClient:
    """シングルトンでGenieクライアントを取得"""
    if "genie_client" not in st.session_state:
        st.session_state.genie_client = GenieClient()
    return st.session_state.genie_client
