---
name: latex-build-qa
description: Run LaTeX manuscript QA checks for refs/cites and optional compile diagnostics.
---

# LaTeX Build QA

## Inputs

- `draft/main.tex` or `draft/full_draft_v1.tex`
- `draft/latex/references.bib`

## Outputs

- `draft/reports/latex_build_report.md`
- `draft/reports/latex_build_log.txt`

## Rules

- Detect unresolved `\ref` targets.
- Detect citation keys not present in bib.
- Optionally run `pdflatex` if available and requested.
