---
name: figure-table-grounding
description: Audit figure/table grounding to claims and reference usage.
---

# Figure Table Grounding

## Inputs

- `draft/latex/sections/sec_*.tex`
- `figures/`
- `tables/`
- `draft/evidence_packets/`

## Outputs

- `draft/reports/figure_table_grounding.md`
- `draft/reports/figure_table_manifest.json`

## Rules

- Check missing label targets for referenced figure/table keys.
- Check unreferenced labels and orphan assets.
- Check whether figure/table references appear in claim-bearing paragraphs.
