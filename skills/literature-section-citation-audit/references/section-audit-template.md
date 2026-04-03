# Section Citation Audit Template

## Inputs

- `draft/latex/sections/<section>.tex`
- `draft/latex/references.bib`
- `draft/evidence_packets/<section_stem>/*.json`
- `references/index/claims.jsonl`
- `references/index/records.jsonl`

## Deterministic Checks

- `coverage`: key claim without citation.
- `support`: cited source does not overlap with claim evidence.
- `isolated_citation`: citation appears without key claim anchor.
- `cited_not_used`: cited key does not support any key claim in this section.
- `strong_claim_weak_evidence`: strong assertion paired with weak packet strength.
- `overgeneralization`: absolute claim language without boundary/uncertainty qualifiers.

## JSON Output Contract

- `summary`
- `section_stem`
- `key_claim_count`
- `finding_counts_by_type`
- `score`
- `risk_level`

- `findings[]`
- `finding_id`
- `type`
- `severity`
- `paragraph_id`
- `claim_text`
- `citation_keys`
- `message`
- `evidence_refs`

- `citation_sets`
- `cited_keys`
- `supported_keys`
- `cited_not_used_keys`
