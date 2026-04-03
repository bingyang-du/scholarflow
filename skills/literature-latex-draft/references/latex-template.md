# LaTeX Draft Template

## Target Structure

- `draft/latex/main.tex`
- `draft/latex/outline.tex`
- `draft/latex/sections/sec_XXX_<slug>.tex`
- `draft/latex/sections/figures.tex`
- `draft/latex/sections/tables.tex`
- `draft/latex/tables/*.tex`
- `draft/latex/references.bib`

## Main File Pattern

- Keep `main.tex` thin.
- Include only preamble, title, TOC, `\input{outline.tex}`, and bibliography print.

## Section File Pattern

- One sub-question per section file.
- Paragraph skeleton:
- Claim
- Evidence
- Limitation/Boundary
- Related citations

## Figure/Tables Pattern

- Figures: scan files under project-root `figures/`.
- Tables: convert each CSV under project-root `tables/` into one TeX fragment.
- Keep source CSV and generated TeX separated.
