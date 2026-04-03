---
name: literature-paragraph-planning
description: Decompose section-level outline into paragraph-level writing plans before prose drafting. Use when Codex should convert section_plan and claims into auditable paragraph units with purpose, core claim, and evidence IDs.
---

# Literature Paragraph Planning

## Overview

Build a deterministic paragraph planning layer between argument outline and LaTeX drafting.

## Workflow

1. Load required inputs.
- `references/index/argument_graph.json`
- `references/index/claims.jsonl`
- `outline/generated_outline.md` (fallback only)

2. Decompose each section into paragraph units.
- Use `argument_graph.section_plan` as primary section source.
- Link claims by `subquestion_id`.
- Apply adaptive paragraph template with fixed types:
- `背景段`
- `定义段`
- `比较段`
- `方法段`
- `机制解释段`
- `争议/局限段`
- `小结段`

3. Produce per-section outputs.
- `draft/paragraph_plans/sec_XXX_<slug>.json`
- `draft/paragraph_plans/sec_XXX_<slug>.md`

## Output Contract

Each paragraph row includes at least:
- `paragraph_id`
- `paragraph_type`
- `purpose`
- `core_claim_id`
- `core_claim_text`
- `required_evidence_ids` (claim IDs)
- `supporting_candidate_ids`
- `section_id`
- `section_title`
- `subquestion_id`

## Guardrails

- Do not download PDFs in this stage.
- Do not write files under `references/library/`.
- Do not mutate `references/index` artifacts.

## Reference

Use [references/paragraph-template.md](references/paragraph-template.md) for template and schema examples.

