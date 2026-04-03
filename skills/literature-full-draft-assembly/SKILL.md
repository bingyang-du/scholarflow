---
name: literature-full-draft-assembly
description: Assemble section drafts into a full manuscript draft, apply conservative global polishing, and emit review artifacts for final human judgment.
---

# Literature Full Draft Assembly

## Overview

Build a complete draft from chapter outputs after section-level writing and audit.

## Workflow

1. Load required inputs.
- `draft/latex/sections/sec_*.tex`
- `draft/latex/audit/section_*_audit.json`
- `draft/latex/references.bib`
- optional templates:
  - `draft/templates/abstract.tex`
  - `draft/templates/conclusion.tex`

2. Assemble full draft body.
- Keep chapter order by `sec_XXX_*.tex`.
- Inject Introduction opening/closing bridge text.
- Append Conclusion callback text.
- Use template fallback placeholders when missing.

3. Run conservative global checks.
- terminology consistency normalization (case/hyphen variants)
- missing cross-reference labels (`\ref`/`\autoref`/`\cref`)
- figure/table isolated labels and missing targets
- citation keys used in text but missing from `.bib`

4. Persist outputs.
- `draft/main.tex`
- `draft/full_draft_v1.tex`
- `draft/reports/full_draft_review.md`
- run manifest under `draft/runs/run_*/manifest.json`

## Guardrails

- Do not modify `references/index/*`.
- Do not download PDFs.
- Do not rewrite `draft/latex/sections/*` in this stage.

## Reference

Use [references/full-draft-template.md](references/full-draft-template.md) for output/report contract.
