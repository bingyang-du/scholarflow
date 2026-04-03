---
name: literature-evidence-packeting
description: Assemble paragraph-level evidence packets after paragraph planning and before prose drafting. Use when Codex should prepare claim support, conflict evidence, and strength/missing markers for each paragraph.
---

# Literature Evidence Packeting

## Overview

Create auditable evidence packets per paragraph unit before writing prose.

## Workflow

1. Load required inputs.
- `draft/paragraph_plans/sec_*.json`
- `references/index/claims.jsonl`
- `references/index/cards.jsonl`
- `references/index/included_candidates.csv`
- `draft/latex/references.bib`

2. Assemble per-paragraph packets.
- Use paragraph `required_evidence_ids` as main claim chain.
- Gate by include scope.
- Attach citation keys, support points, conflict evidence, and strength labels.
- Mark missing evidence explicitly.

3. Emit packet artifacts.
- `draft/evidence_packets/<section_stem>/<paragraph_id>.json`
- optional overrides from `draft/evidence_packets/packet_overrides.json`

## Guardrails

- Do not download PDFs in this stage.
- Do not write files under `references/library/`.
- Do not mutate `references/index` intermediate artifacts.

## Reference

Use [references/evidence-packet-template.md](references/evidence-packet-template.md) for schema and override examples.

