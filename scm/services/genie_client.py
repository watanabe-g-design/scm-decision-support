"""
Genie / 生成AI 抽象化レイヤー
- Phase 1: Databricks Genie Space API (構造化データ探索)
- Phase 2: 生成AI要約 (rationale text 強化)
- Phase 3: 地図コンテキスト連携

接続先を簡単に差し替えられる構造
"""
import streamlit as st
from services.config import load_config, is_databricks_mode


class GenieClient:
    """Genie/AI abstraction - 将来的に API 差し替え可能"""

    def __init__(self):
        self._config = load_config()
        self._space_id = self._config.get("genie_space_id")

    @property
    def is_available(self) -> bool:
        return is_databricks_mode() and bool(self._space_id)

    def query(self, prompt: str, context: dict = None) -> dict:
        """
        Genie に質問を送信し、応答テキスト + 実行された SQL を取得する

        Args:
            prompt: 自然言語の質問
            context: 地図選択コンテキスト等
                {"warehouse_ids": [...], "route_ids": [...], "polygon_wkt": "..."}

        Returns:
            {"text": str, "sql": str|None, "error": str|None}
        """
        if not self.is_available:
            return {
                "text": "Genie は未接続です。SCM_GENIE_SPACE_ID 環境変数を設定してください。",
                "sql": None, "error": None,
            }

        # コンテキスト注入
        enriched_prompt = prompt
        if context:
            if context.get("warehouse_ids"):
                wh_list = ", ".join(context["warehouse_ids"])
                enriched_prompt += f"\n\n[コンテキスト: 対象倉庫 = {wh_list}]"

        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()

            # Step 1: 会話開始 (待機付き) — メッセージ ID を取得
            conv = w.genie.start_conversation_and_wait(
                space_id=self._space_id,
                content=enriched_prompt,
            )

            # 会話オブジェクトから conversation_id と message_id を取得
            conv_id = getattr(conv, "conversation_id", None) or (
                conv.conversation.id if getattr(conv, "conversation", None) else None
            )
            msg_id = getattr(conv, "message_id", None) or getattr(conv, "id", None)
            if not conv_id or not msg_id:
                # フォールバック: messages 配列から取得
                if hasattr(conv, "messages") and conv.messages:
                    msg = conv.messages[-1]
                    conv_id = conv_id or getattr(msg, "conversation_id", None)
                    msg_id  = msg_id  or getattr(msg, "id", None)

            text_content = ""
            sql_content = None

            # Step 2: メッセージを取得して attachments を解析
            if conv_id and msg_id:
                try:
                    msg = w.genie.get_message(
                        space_id=self._space_id,
                        conversation_id=conv_id,
                        message_id=msg_id,
                    )
                    for att in (msg.attachments or []):
                        # テキスト応答
                        if hasattr(att, "text") and att.text and getattr(att.text, "content", None):
                            text_content = att.text.content
                        # SQL クエリ
                        if hasattr(att, "query") and att.query:
                            sql_content = getattr(att.query, "query", None) or sql_content
                            # クエリ説明があればテキストに追加
                            desc = getattr(att.query, "description", None)
                            if desc and not text_content:
                                text_content = desc
                except Exception:
                    pass

            # フォールバック: conv オブジェクト直下の attachments もチェック
            if not text_content and not sql_content and hasattr(conv, "attachments"):
                for att in (conv.attachments or []):
                    if hasattr(att, "text") and att.text and getattr(att.text, "content", None):
                        text_content = att.text.content
                    if hasattr(att, "query") and att.query:
                        sql_content = getattr(att.query, "query", None) or sql_content

            if not text_content and not sql_content:
                text_content = "(Genie から応答が取得できませんでした。Space ID と権限設定を確認してください)"

            return {"text": text_content, "sql": sql_content, "error": None}
        except Exception as e:
            return {"text": "", "sql": None, "error": f"{type(e).__name__}: {e}"}

    def generate_summary(self, data_context: str) -> str:
        """
        生成AI要約 (将来的にはLLM直接呼び出し)
        Phase 1ではプレースホルダ
        """
        return f"[AI要約] {data_context[:200]}..."


# サンプルクエリ集
SAMPLE_QUERIES = [
    "今すぐ発注が必要なCRITICAL部品の一覧を教えてください",
    "過剰在庫になっている部品はどれですか？",
    "名古屋倉庫で安全在庫を割っている部品は？",
    "物流遅延率が最も高いメーカーはどこですか？",
    "今月のFCST精度が最も低いカテゴリは？",
    "横持ち候補になっている部品はありますか？",
    "リードタイムが最も長い部品トップ5を教えてください",
    "来月の需要予測に対して在庫は足りていますか？",
]


def get_genie_client() -> GenieClient:
    """シングルトンでGenieクライアントを取得"""
    if "genie_client" not in st.session_state:
        st.session_state.genie_client = GenieClient()
    return st.session_state.genie_client
