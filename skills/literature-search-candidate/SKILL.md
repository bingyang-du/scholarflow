---
name: literature-search-candidate
description: Build an auditable candidate literature pool from a topic-framing payload (TFR-1) without downloading PDFs. Use when Codex should generate search queries, record search sources, save candidate metadata, and deduplicate results before screening.
---

# Literature Search Candidate

## Overview

Transform `TFR-1` into a reproducible candidate list for screening. This skill only handles query planning and metadata collection.

## Workflow

1. Validate `TFR-1` input.
- Required fields: `topic`, `research_questions`, `keywords`, `search_constraints`.
- Optional fields: `output.type`, `exclusions`.

2. Generate query set.
- Build multiple query variants from keyword groups.
- Keep query strings and applied constraints traceable.

3. Execute backend-neutral search adapters.
- Use ordered backends: `openalex -> crossref -> arxiv -> mcp(if configured)`.
- Record success/failure per query and per backend.

4. Normalize and deduplicate candidates.
- Normalize core metadata: title, authors, year, venue, DOI/arXiv ID, URL.
- Dedup priority: `doi > arxiv_id > normalized_title+year`.

5. Persist auditable outputs.
- Write `references/index/search_sources.csv`.
- Write `references/index/candidates.csv`.
- Write run manifest at `draft/runs/run_*/manifest.json`.

## Output Contract

`search_sources.csv` columns:
- `query_id,source_db,query_string,filters,time_range,language_range,venue_preference,retrieved_at,result_count,status,error`

`candidates.csv` columns:
- `candidate_id,title,year,authors,venue,doi,arxiv_id,url,source_db,query_id,retrieved_at,dedup_key,dedup_status,screen_state`

## Guardrails

- Do not download PDFs in this skill.
- Do not write any files into `references/library/`.
- Do not make include/exclude screening decisions.
- Keep `screen_state` default as `unreviewed`.

## Execution Notes

- Primary exchange format is CSV.
- If all backends fail, still emit source logs and manifest with failure details.
- Keep outputs consumable by downstream screening skill.

## Reference

Use [references/search-template.md](references/search-template.md) for the input and output skeletons.
