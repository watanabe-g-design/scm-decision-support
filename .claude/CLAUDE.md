# Project rules: SCM / Manufacturing demo on Databricks Apps

## Mission
Build a broadly reusable SCM and manufacturing demo on Databricks Apps.
Treat Macnica as the trading company / distributor role.
All company names in the demo must be anonymous or fictional.

## Product intent
This is not a generic BI dashboard.
It is a decision-support application for procurement, SCM, manufacturing control, executives, DX teams, and IT.

## Core product principles
- Customer inventory is the primary viewpoint.
- Trading-company inventory is supporting information only.
- Every top-level screen must help users move from signal -> evidence -> action.
- The app must be explainable by non-technical stakeholders.
- AI is a supporting layer, not the primary UI.
- Design for Databricks-native governance, not bolt-on governance.

## Domain guardrails
- Keep glossary terms consistent across UI, SQL, documentation, and Genie prompts.
- Never expose real company names, product names, order IDs, or customer identifiers in demo assets.
- Use explicit metric definitions for every KPI.
- Avoid ambiguous terms such as "risk" or "health" without formula or definition.

## Databricks guardrails
- Default target runtime is Databricks Apps + Unity Catalog + Databricks SQL.
- Prefer Unity Catalog Volumes for non-tabular files.
- Prefer Lakeflow / `pyspark.pipelines` patterns for new pipeline implementations.
- Keep UI code thin; keep business logic and semantic logic outside page files.
- Separate demo-mode adapters from Databricks-mode adapters.

## UX guardrails
- The home page is a prompt dashboard, not an exploration dashboard.
- Show the most important exceptions first.
- Do not rely on color alone.
- Preserve filter state across pages.
- Every alert should have: severity, rationale, due timing, and suggested action.
- Every AI answer must include scope, filters, and data period when available.

## Current screen structure (6 modules)
Each screen has a distinct main entity (axis). Same Gold tables can be referenced
from multiple screens but the **subject of the question** must be different.
- A. 経営コントロールタワー (app.py) — 横断軸
- B. LTインテリジェンス (pages/1_lt_intelligence.py) — 時間軸 (LT)
- C. 納期コミットリスク (pages/2_commit_supply_balance.py) — オーダー軸 (sales_order_id)
- D. 在庫基準逸脱レーダー (pages/3_inventory_policy.py) — 品目軸 (component_id)
- E. 拠点・倉庫健全性 (pages/4_network_warehouse.py) — 拠点軸 (warehouse_id)
- F. データ信頼性センター (pages/5_data_reliability.py) — メタ軸 (Pipeline)

### MECE constraints
- C must NOT show monthly balance projection (that is D's job)
- D is the SOLE owner of monthly balance projection and ZERO/UNDER/OVER classification
- C may show requirement timeline ONLY as a per-order drill-down evidence panel
- Any cross-axis information should be implemented as a `st.page_link` to the owning screen

### Streamlit Apps constraints (must respect)
- `st.chat_input` cannot be used inside `st.expander`, `st.tabs`, `st.columns`, or any container.
  Use `st.form` + `st.text_input` + `st.form_submit_button` inside containers instead.
- `st.status` cannot be nested inside `st.expander`. Use `st.empty()` + `st.spinner` instead.
- `st.expander` cannot be nested inside another `st.expander` or `st.chat_message`.

## Gold tables (14)
gold_exec_summary_daily, gold_lt_snapshot_current, gold_lt_trend_monthly,
gold_lt_escalation_items, gold_order_commit_risk, gold_requirement_timeline,
gold_balance_projection_monthly, gold_inventory_policy_breach,
gold_geo_warehouse_status, gold_data_pipeline_health,
gold_action_queue_daily, gold_business_glossary, gold_metric_definition,
gold_genie_semantic_examples

## Delivery format
When generating code or specs:
1. state assumptions,
2. identify risks and alternatives,
3. produce implementation-ready output,
4. include test/checklist items,
5. note unresolved gaps explicitly.
