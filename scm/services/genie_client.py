"""
Genie / 生成AI 抽象化レイヤー
================================
- single-turn モード: 各質問を独立した会話として処理
- multi-turn モード (Research): 同じ conversation_id を保持し、Genie が前の質問の
  文脈を引き継ぐ。Databricks には現状「Deep Research Agent」を Genie Space に
  外部から呼び出す公開 API が無いため、multi-turn 会話で擬似的な深掘り体験を実現。
"""
import streamlit as st
from services.config import load_config, is_databricks_mode


class GenieClient:
    """Genie/AI abstraction - 将来的に API 差し替え可能"""

    def __init__(self):
        self._config = load_config()
        self._space_id = self._config.get("genie_space_id")
        # multi-turn モード時に保持する会話 ID
        self._conversation_id = None

    @property
    def is_available(self) -> bool:
        return is_databricks_mode() and bool(self._space_id)

    def reset_conversation(self):
        """会話履歴をリセット (新しい multi-turn セッション開始時に呼ぶ)"""
        self._conversation_id = None

    def query(self, prompt: str, context: dict = None, multi_turn: bool = False) -> dict:
        """
        Genie に質問を送信し、応答テキスト + 実行された SQL を取得する

        Args:
            prompt: 自然言語の質問
            context: 地図選択コンテキスト等
            multi_turn: True なら同じ conversation_id を継続して使う (リサーチモード)

        Returns:
            {"text": str, "sql": str|None, "error": str|None, "elapsed": float}
        """
        import time
        start = time.monotonic()

        if not self.is_available:
            return {
                "text": "Genie は未接続です。SCM_GENIE_SPACE_ID 環境変数を設定してください。",
                "sql": None, "error": None, "elapsed": 0.0,
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

            conv_id = None
            msg_id = None

            if multi_turn and self._conversation_id:
                # 既存の会話に follow-up メッセージを追加
                msg = w.genie.create_message_and_wait(
                    space_id=self._space_id,
                    conversation_id=self._conversation_id,
                    content=enriched_prompt,
                )
                conv_id = self._conversation_id
                msg_id = getattr(msg, "message_id", None) or getattr(msg, "id", None)
            else:
                # 新規会話開始
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

                # multi-turn モードのために会話 ID を保存
                if multi_turn and conv_id:
                    self._conversation_id = conv_id

            text_content = ""
            sql_content = None

            if conv_id and msg_id:
                try:
                    msg = w.genie.get_message(
                        space_id=self._space_id,
                        conversation_id=conv_id,
                        message_id=msg_id,
                    )
                    for att in (msg.attachments or []):
                        if hasattr(att, "text") and att.text and getattr(att.text, "content", None):
                            text_content = att.text.content
                        if hasattr(att, "query") and att.query:
                            sql_content = getattr(att.query, "query", None) or sql_content
                            desc = getattr(att.query, "description", None)
                            if desc and not text_content:
                                text_content = desc
                except Exception:
                    pass

            if not text_content and not sql_content:
                text_content = "(Genie から応答が取得できませんでした。Space ID と権限設定を確認してください)"

            return {
                "text":    text_content,
                "sql":     sql_content,
                "error":   None,
                "elapsed": round(time.monotonic() - start, 2),
            }
        except Exception as e:
            return {
                "text":    "",
                "sql":     None,
                "error":   f"{type(e).__name__}: {e}",
                "elapsed": round(time.monotonic() - start, 2),
            }

    def generate_summary(self, data_context: str) -> str:
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
