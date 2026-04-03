# Evidence Packet Template

## Inputs

- `draft/paragraph_plans/sec_*.json`
- `references/index/claims.jsonl`
- `references/index/cards.jsonl`
- `references/index/included_candidates.csv`
- `draft/latex/references.bib`
- optional: `draft/evidence_packets/packet_overrides.json`

## Packet JSON Skeleton

```json
{
  "section_id": "SEC001",
  "section_title": "Mechanism pathways",
  "subquestion_id": "SQ001",
  "paragraph_id": "SEC001-P01",
  "paragraph_type": "机制解释段",
  "claim": {
    "core_claim_id": "CLM000001",
    "core_claim_text": "Selectivity increases under pathway A."
  },
  "supporting_references": [
    {
      "claim_id": "CLM000001",
      "candidate_id": "C000001",
      "citation_key": "smith2024_pathway",
      "doi": "10.1000/a",
      "arxiv_id": "",
      "support_points": [
        "Selectivity increases under pathway A."
      ]
    }
  ],
  "conflicting_evidence": [],
  "strength": {
    "score": 79,
    "label": "strong"
  },
  "missing_evidence": {
    "is_missing": false,
    "missing_claim_ids": [],
    "reason": ""
  },
  "provenance": {
    "generated_at": "2026-04-03T00:00:00",
    "inputs": {}
  }
}
```

## Override Schema (Optional)

```json
{
  "ignore_claim_ids": [
    {"paragraph_id": "SEC001-P01", "claim_id": "CLM000099"},
    "CLM000100"
  ],
  "force_support": [
    {
      "paragraph_id": "SEC001-P01",
      "claim_id": "CLM000002",
      "candidate_id": "C000002",
      "citation_key": "lee2023_shift",
      "support_points": ["Manual support point"]
    }
  ],
  "force_conflict": [
    {
      "paragraph_id": "SEC002-P01",
      "claim_id": "CLM000001",
      "candidate_id": "C000001",
      "conflict_point": "Manual conflict point"
    }
  ]
}
```

