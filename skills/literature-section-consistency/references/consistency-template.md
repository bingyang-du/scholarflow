# Section Consistency Template

## Inputs

- `draft/latex/sections/sec_*.tex`
- `references/index/argument_graph.json`
- `draft/runs/run_*/section_drafts/*.json`

## Deterministic Checks

- `adjacent_duplication`: remove duplicated adjacent sentence from later paragraph.
- `term_consistency`: unify inconsistent case/hyphen variants to a single section form.
- `logical_jump`: insert short transition sentence when paragraph bridge is missing.
- `claim_evidence_order`: downgrade conclusion-first phrasing to evidence-led phrasing.
- `overclaim_without_support`: append uncertainty and boundary safeguards for weak/missing evidence.

## Report Contract

`section_consistency_report.json`

- `summary`
- `section_count`
- `paragraph_count`
- `issue_counts_by_type`
- `auto_fixed_count`
- `manual_review_count`
- `score`
- `risk_level`

- `sections[]`
- `section_stem`
- `section_title`
- `issues[]`
- `section_score`
- `section_risk_level`

- `issues[]` fields
- `issue_id`
- `type`
- `severity`
- `paragraph_id`
- `message`
- `before_excerpt`
- `after_excerpt`
- `auto_fixed`
