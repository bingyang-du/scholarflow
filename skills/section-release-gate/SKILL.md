---
name: section-release-gate
description: Decide section-level go/revise/block using consistency and citation audit outputs.
---

# Section Release Gate

## Inputs

- `draft/runs/run_*/section_consistency_report.json`
- `draft/latex/audit/section_<stem>_audit.json`

## Outputs

- `draft/gates/section_<stem>_gate.json`
- `draft/gates/section_<stem>_fixlist.md`

## Rules

- Aggregate high/medium/low issues into score and risk.
- Decision mapping: `block` > `revise` > `go`.
- `strictness=hard` with non-`go` decision returns failed status.
