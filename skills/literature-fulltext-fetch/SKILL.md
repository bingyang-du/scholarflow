---
name: literature-fulltext-fetch
description: Download fulltext PDFs only for included candidates after screening and before outline/writing. Use when Codex should resolve PDF URLs from records/cards, store files in references/library, and write auditable fetch logs while continuing on per-paper failures.
---

# Literature Fulltext Fetch

## Overview

Fetch fulltext files as a dedicated post-screen stage.

## Workflow

1. Load inputs.
- `references/index/included_candidates.csv`
- `references/index/cards.jsonl`
- `references/index/records.jsonl`

2. Resolve source URL per included candidate.
- Priority:
- `records.jsonl.pdf_url`
- `cards.jsonl.pdf_url` (if present)
- constructed arXiv PDF URL from `arxiv_id`

3. Download with retry and continue policy.
- Process only include pool.
- On single-paper failure, record and continue next paper.

4. Persist outputs.
- `references/index/fulltext_fetch_log.csv`
- `references/index/downloaded_fulltexts.csv`
- files to `references/library/`

## Output Contract

`fulltext_fetch_log.csv` columns:
- `candidate_id,card_id,doi,arxiv_id,source_url,target_path,status,http_code,error,retried,retrieved_at`

Status values:
- `downloaded|exists|failed|no_url`

`downloaded_fulltexts.csv` columns:
- `candidate_id,card_id,target_path,source_url,retrieved_at`

## Guardrails

- Do not download candidates outside `included_candidates.csv`.
- Do not modify screening outputs.
- Do not write outside `references/library/` and `references/index/` for this stage.

## Reference

Use [references/fetch-template.md](references/fetch-template.md) for examples and failure handling notes.
