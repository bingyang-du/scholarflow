# Full Draft Assembly Template

## Inputs

- `draft/latex/sections/sec_*.tex`
- `draft/latex/audit/section_*_audit.json`
- `draft/latex/references.bib`
- optional:
  - `draft/templates/abstract.tex`
  - `draft/templates/conclusion.tex`

## Outputs

- `draft/main.tex`
- `draft/full_draft_v1.tex`
- `draft/reports/full_draft_review.md`

## Global Revision Contract

- Keep section order fixed by `sec_XXX` index.
- Only conservative edits at manuscript level:
  - intro opening/closing bridge
  - conclusion callback
  - terminology form normalization
  - cross-reference consistency checks
  - figure/table reference checks
- Missing templates fallback to TODO placeholders with warnings.

## Report Contract

`full_draft_review.md` should include:

- overall score and risk level
- section audit aggregation
- unresolved high-risk findings count
- terminology normalization summary
- cross-reference findings
- figure/table reference findings
- citation key mismatch findings
