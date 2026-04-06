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
        Genie に質問を送信

        Args:
            prompt: 自然言語の質問
            context: 地図選択コンテキスト等
                {"warehouse_ids": [...], "route_ids": [...], "polygon_wkt": "..."}

        Returns:
            {"text": str, "sql": str|None, "error": str|None}
        """
        if not self.is_available:
            return {
                "text": "Genie は Databricks 接続時に利用可能です。接続後にお試しください。",
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
            conv = w.genie.start_conversation_and_wait(
                space_id=self._space_id, content=enriched_prompt)

            text_content = ""
            sql_content = None
            for msg in (conv.messages or []):
                if msg.role and msg.role.value == "ASSISTANT":
                    for att in (msg.attachments or []):
                        if att.text:  text_content = att.text.content or ""
                        if att.query: sql_content  = att.query.query or None

            return {"text": text_content, "sql": sql_content, "error": None}
        except Exception as e:
            return {"text": "", "sql": None, "error": str(e)}

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
