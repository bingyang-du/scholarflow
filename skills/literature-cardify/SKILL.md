---
name: literature-cardify
description: Convert literature candidates into structured evidence cards before drafting prose. Use when Codex should read candidate metadata/abstracts, generate standardized card fields, score topic relevance, suggest body inclusion, and write cards.jsonl without downloading PDFs.
---

# Literature Cardify

## Overview

Generate one structured card per candidate paper. Keep this skill focused on evidence extraction and formatting, not final writing decisions.

## Workflow

1. Load candidate inputs.
- Read `references/index/candidates.csv`.
- Optionally merge metadata from `references/index/records.jsonl`.

2. Create one card per candidate.
- Fill required fields using title, venue, IDs, and abstract when available.
- Use conservative placeholders when abstract is missing.

3. Assign evidence and recommendation signals.
- `topic_relevance_score`: 0/1/2/3.
- `body_inclusion`: `yes/no/maybe`.
- `body_inclusion_reason`: brief rationale.
- `card_status`: `completed` or `needs_review`.

4. Persist and link outputs.
- Write cards to `references/index/cards.jsonl`.
- Update `candidates.csv` with `card_status`.
- Keep `dedup_status` and `screen_state` untouched.

## Card Schema

Each JSONL row must include:
- `card_id,candidate_id,title,year,doi,arxiv_id,source_db,query_id`
- `research_question,method,data,main_findings,limitations,citable_points`
- `topic_relevance_score`
- `body_inclusion`
- `body_inclusion_reason`
- `evidence_level`
- `card_status`
- `created_at,updated_at`

## Guardrails

- Do not download PDFs.
- Do not write files into `references/library/`.
- Do not perform final include/exclude screening.
- Keep this stage as structured pre-writing evidence preparation.

## Reference

Use [references/card-template.md](references/card-template.md) for field-level templates and scoring rules.
