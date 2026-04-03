---
name: literature-argument-outline
description: Build a claim-to-evidence argument graph and research-question-driven chapter outline from screened evidence. Use when Codex should transform included candidates and cards into structured claims, cluster evidence by sub-question, and generate section/paragraph skeletons before drafting prose.
---

# Literature Argument Outline

## Overview

Produce a structured middle layer between screening and writing:
- claim classification
- evidence mapping
- chapter and paragraph skeleton

## Workflow

1. Load required inputs.
- `topic_frame` (`TFR-1`)
- `references/index/included_candidates.csv`
- `references/index/cards.jsonl`

2. Extract and classify claims.
- Claim sources: `main_findings`, `citable_points`, `method`, `limitations`.
- Fixed claim taxonomy:
- `mechanism`
- `observation`
- `condition`
- `comparison`
- `limitation`

3. Build mapping and evidence clusters.
- Scope gate: only candidates listed in `included_candidates.csv`.
- Link candidate to card by `candidate_id`.
- Map each claim to a sub-question under the research-question tree.
- Cluster key: `subquestion_id + claim_type + normalized_topic_terms`.

4. Produce outputs.
- `references/index/claims.jsonl`
- `references/index/argument_graph.json`
- `outline/generated_outline.md`

## Output Contract

`claims.jsonl` row fields:
- `claim_id,candidate_id,card_id,claim_type,claim_text,evidence_snippet,subquestion_id,confidence`

`argument_graph.json` fields:
- `topic,primary_rq,subquestions[],claim_nodes[],evidence_edges[],clusters[],section_plan[]`

`generated_outline.md` rule:
- Render from `section_plan` only.
- Paragraph pattern: `主张 -> 证据句 -> 局限/边界`.

## Guardrails

- Do not download PDFs.
- Do not write files into `references/library/`.
- Do not skip structured outputs and jump directly to long-form writing.

## Reference

Use [references/argument-template.md](references/argument-template.md) for schema snippets and section templates.
