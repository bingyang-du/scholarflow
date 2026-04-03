---
name: literature-latex-draft
description: Generate a modular LaTeX draft project after argument outlining. Use when Codex should create main.tex plus section files, references.bib, figure/table include fragments, and table source wiring instead of writing one giant TeX file.
---

# Literature LaTeX Draft

## Overview

Build a multi-file LaTeX writing project for iterative drafting and review.

## Workflow

1. Load structured inputs.
- `references/index/argument_graph.json`
- `references/index/claims.jsonl`
- `references/index/included_candidates.csv`
- `references/index/records.jsonl`
- supplemental `draft/latex/refs.bib`

2. Generate modular TeX layout.
- `draft/latex/main.tex`
- `draft/latex/outline.tex`
- `draft/latex/sections/sec_XXX_<slug>.tex`
- `draft/latex/sections/figures.tex`
- `draft/latex/sections/tables.tex`

3. Build bibliography and data fragments.
- `draft/latex/references.bib` from include pool.
- scan `figures/` for figure skeleton references.
- convert `tables/*.csv` to `draft/latex/tables/*.tex`.

4. Preserve manual edits by default.
- Skip existing files unless explicit overwrite is requested.

## Guardrails

- Do not overwrite manual section files by default.
- Do not mutate `references/index` intermediate artifacts in this stage.
- Do not collapse all content into one large `main.tex`.

## Reference

Use [references/latex-template.md](references/latex-template.md) for section and fragment templates.
