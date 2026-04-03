---
name: claim-trace-matrix
description: Export claim-level traceability matrix linking claim to paragraph, citation key, and candidate evidence.
---

# Claim Trace Matrix

## Inputs

- `references/index/claims.jsonl`
- `draft/paragraph_plans/sec_*.json`
- `draft/evidence_packets/**/*.json`
- `draft/latex/references.bib`

## Outputs

- `draft/audit/claim_trace_matrix.csv`
- `draft/audit/claim_trace_matrix.json`

## Rules

- Each claim should map to `paragraph_id + citation_key + candidate_id`.
- Missing links are reported as `gap`.
- `strictness=hard` fails when gaps exist.
