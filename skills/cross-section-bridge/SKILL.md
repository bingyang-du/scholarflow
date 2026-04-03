---
name: cross-section-bridge
description: Build conservative inter-section transition plan and bridge tex fragment.
---

# Cross Section Bridge

## Inputs

- `draft/latex/sections/sec_*.tex`
- `references/index/argument_graph.json`

## Outputs

- `draft/bridges/bridge_plan.json`
- `draft/latex/sections/bridges.tex`

## Rules

- Keep section order from argument graph when available.
- Only add conservative bridge sentences; do not rewrite section bodies.
