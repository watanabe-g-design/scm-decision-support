---
name: databricks-lakeflow-uc-implementer
description: Use for Databricks Apps, Unity Catalog, Volumes, Lakeflow pipelines, SQL, and deployment design/implementation.
---

# Purpose
Keep implementation aligned with current Databricks-native patterns.

# Required platform stance
- App target: Databricks Apps.
- Governance target: Unity Catalog.
- Query layer: Databricks SQL or governed Spark access.
- File storage: Unity Catalog Volumes when file access is needed.
- New pipeline logic: prefer `pyspark.pipelines` / Lakeflow patterns.
- Support demo mode via adapters, not ad hoc branching in page code.

# Implementation rules
- Keep business logic out of Streamlit page files.
- Separate:
  - services/adapters,
  - business logic,
  - semantic definitions,
  - UI components.
- Design Gold tables around decision entities.
- Provide metric-definition metadata and glossary metadata for AI and UI reuse.
- Treat Genie semantic curation as a first-class artifact.

# Pipeline guardrails
Never use these in pipeline dataset definitions:
- collect()
- count()
- toPandas()
- save()
- saveAsTable()
- start()
- toTable()

# Review items
- Is the code Databricks Apps deployable?
- Are permissions minimal and explicit?
- Are Volumes used correctly for non-tabular assets?
- Is old DLT syntax being copied without reason?
- Are metrics reproducible in SQL?
