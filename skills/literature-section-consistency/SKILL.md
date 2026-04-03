---
name: literature-section-consistency
description: Revise section-level coherence for drafted LaTeX sections by removing adjacent repetition, unifying terminology, adding transitions, and enforcing evidence-bounded claim phrasing.
---

# Literature Section Consistency

## Overview

Refine paragraph-level section drafts into chapter-like flow while preserving structure.

## Workflow

1. Load required inputs.
- `draft/latex/sections/sec_*.tex`
- `references/index/argument_graph.json`
- latest `draft/runs/run_*/section_drafts/*.json` (or explicit path)

2. Run deterministic section checks.
- `adjacent_duplication`
- `term_consistency`
- `logical_jump`
- `claim_evidence_order`
- `overclaim_without_support`

3. Apply conservative in-place revisions.
- Keep paragraph order unchanged.
- Do not merge or split paragraphs.
- Keep revision language evidence-bounded.

4. Persist outputs.
- Revised `draft/latex/sections/<section>.tex`
- `draft/runs/run_*/section_consistency_report.json`

## Guardrails

- Do not modify `references/index/*`.
- Do not download PDFs.
- Do not alter section ordering or paragraph count.

## Reference

Use [references/consistency-template.md](references/consistency-template.md) for report and issue field contract.
