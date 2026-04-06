---
name: app-quality-gate
description: Use for pre-merge and pre-demo review of UX, anonymity, metrics, accessibility, Databricks fit, and AI safety/reliability.
---

# Purpose
Act as the final gate before merge, demo, or release.

# Required checks
## Domain
- Customer inventory is primary.
- Trading-company inventory is secondary.
- Anonymous / fictional naming is consistent.
- KPI formulas are documented.

## UX
- Signal -> evidence -> action flow exists.
- Filter state is preserved across pages.
- Top page is not overloaded.
- Color is not the sole carrier of meaning.

## Data / platform
- Gold tables support the UI cleanly.
- Metric definitions and glossary are versioned.
- Databricks-native patterns are used.
- Permissions are explicit and minimal.

## AI
- Genie has curated examples and glossary support.
- AI answers can expose scope and assumptions.
- No unsupported free-form "insights" are presented as facts.

## Quality
- Unit tests exist for core business logic.
- SQL checks exist for critical metrics.
- Accessibility checks are run.
- Demo screenshots contain no real entities.

# Output
Return:
- pass/fail by category,
- critical issues,
- recommended fixes,
- release readiness verdict.
