---
name: literature-screening
description: Screen literature candidates at title+abstract level using cards.jsonl and candidates.csv. Use when Codex should assign include/exclude/unsure decisions with fixed reason codes, produce auditable screening outputs, and update screening states without downloading PDFs.
---

# Literature Screening

## Overview

Run a structured first-pass screening step after cardification. This skill produces auditable decisions and a clean include list for downstream writing.

## Workflow

1. Load inputs.
- `references/index/candidates.csv`
- `references/index/cards.jsonl`

2. Apply screening scope.
- Process only `dedup_status=unique` candidates.
- Keep duplicate rows outside decision loop.

3. Produce decisions.
- `decision`: `include|exclude|unsure`
- `reason_code`: `R1..R6`
- `reason_note`: short rationale

4. Persist outputs.
- `references/index/screening_decisions.csv`
- `references/index/included_candidates.csv`

5. Backwrite states.
- Update `candidates.csv` with `screen_state` and `screen_decision`.
- Update `cards.jsonl` with screening fields.

## Decision Codes

- `R1`: relevance weak
- `R2`: evidence insufficient
- `R3`: method mismatch or method info not sufficient
- `R4`: scope mismatch
- `R5`: duplicate/suboptimal version
- `R6`: missing critical information

## Guardrails

- Do not download PDFs.
- Do not write any files into `references/library/`.
- Do not run full-text screening in this skill.
- Keep this stage focused on title+abstract first pass.

## Reference

Use [references/screening-template.md](references/screening-template.md) for schema and output examples.
