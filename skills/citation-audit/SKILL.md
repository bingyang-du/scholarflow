---
name: citation-audit
description: Audit conclusion-citation-evidence integrity for LaTeX drafts after generation. Use when Codex should check claim coverage, citation support, bib field completeness, and text/bib consistency, then emit machine-readable and human-readable audit reports.
---

# Citation Audit

## Overview

Run a dedicated citation integrity gate after LaTeX draft generation.

## Workflow

1. Load audit inputs.
- `draft/latex/main.tex`
- `draft/latex/outline.tex`
- `draft/latex/sections/*.tex`
- `draft/latex/references.bib`
- `references/index/argument_graph.json`
- `references/index/claims.jsonl`
- `references/index/records.jsonl`

2. Detect important claims.
- Prioritize `\paragraph` blocks and `\textbf{Claim.}` lines.
- Include assertion-keyword sentences.
- Apply optional overrides from `draft/latex/audit_overrides.json`.

3. Run checks.
- coverage: important claim has citation
- support: citation evidence overlaps with claim evidence pool
- bib_fields: minimum required fields by bib type
- text_only: cite key in text missing from bib
- bib_unused: bib entry not cited in text

4. Emit reports.
- `draft/latex/audit/citation_audit.json`
- `draft/latex/audit/citation_findings.csv`
- `draft/latex/audit/citation_audit_report.md`

## Guardrails

- Keep this stage read-only over `references/index` artifacts.
- Do not rewrite manuscript text in this stage.
- Report findings and scores without silently dropping high-risk issues.

## Reference

Use [references/audit-template.md](references/audit-template.md) for output schema and severity guidance.
