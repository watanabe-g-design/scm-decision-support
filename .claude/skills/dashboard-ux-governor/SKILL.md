---
name: dashboard-ux-governor
description: Use for dashboard information architecture, chart selection, accessibility, and UX reviews for prompt-oriented SCM dashboards.
---

# Purpose
Protect the UX quality of SCM/manufacturing dashboards and keep them aligned with Digital Agency dashboard design principles.

# Governing principles
- Prioritize prompt dashboards at the top level.
- Design flow: requirements -> prototyping -> implementation.
- Top screens must support quick anomaly detection and action judgment.
- Exploration belongs in lower layers, not on the main page.
- Limit visual clutter and preserve a clear information hierarchy.
- Use a constrained palette and semantic state rules.
- Never rely on color alone; combine labels, icons, and position.
- Ensure accessible contrast, keyboard reachability, and meaningful labels.

# Screen review checklist
- Can a stakeholder understand the page in 5 seconds?
- Is there a clear "what needs attention now" area?
- Does each KPI link to evidence and a next action?
- Are charts chosen for comparison accuracy, not novelty?
- Are there too many visuals above the fold?
- Are terms and units explicit?
- Is the page still understandable in grayscale or by screen reader?

# Chart rules
- Use bars for ranked comparison.
- Use lines for time trends.
- Use stacked visuals only when composition is the actual question.
- Avoid pie/donut unless share-of-whole is the main point and category count is low.
- Put thresholds directly on charts when action depends on them.

# Anti-patterns
- No rainbow palettes.
- No decorative maps without action value.
- No KPI cards without definitions.
- No heatmaps that require color perception alone.
