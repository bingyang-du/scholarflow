---
name: literature-section-citation-audit
description: Audit citation-evidence integrity for a single drafted section before moving to the next chapter.
---

# Literature Section Citation Audit

## Overview

Run a chapter-local citation audit immediately after section drafting/consistency revision.

## Workflow

1. Load required inputs.
- `draft/latex/sections/<section>.tex`
- `draft/latex/references.bib`
- `draft/evidence_packets/<section_stem>/*.json`
- `references/index/claims.jsonl`
- `references/index/records.jsonl`

2. Resolve paragraph alignment.
- Prefer latest `draft/runs/run_*/section_drafts/<section>.json`.
- Fallback to paragraph order when section_drafts are unavailable.

3. Execute deterministic checks.
- `coverage`
- `support`
- `isolated_citation`
- `cited_not_used`
- `strong_claim_weak_evidence`
- `overgeneralization`

4. Persist outputs.
- `draft/latex/audit/section_<section_stem>_audit.json`
- `draft/latex/audit/section_<section_stem>_audit.md`

## Guardrails

- Do not modify `references/index/*`.
- Do not download PDFs.
- Do not rewrite section tex in this stage.

## Reference

Use [references/section-audit-template.md](references/section-audit-template.md) for the output schema and severity guidance.
