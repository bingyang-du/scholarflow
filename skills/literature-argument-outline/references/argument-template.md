# Argument Graph Template

## Inputs

- `topic_frame` (`TFR-1`)
- `references/index/included_candidates.csv`
- `references/index/cards.jsonl`

## Claim Taxonomy

- `mechanism`: pathway or mechanism-oriented claim
- `observation`: result or phenomenon claim
- `condition`: scope or condition-sensitive claim
- `comparison`: relative comparison claim
- `limitation`: limitation or uncertainty claim

## Claim JSONL Row Example

```json
{
  "claim_id": "CLM000001",
  "candidate_id": "C000001",
  "card_id": "CARD_C000001",
  "claim_type": "mechanism",
  "claim_text": "Under low temperature conditions, pathway A dominates.",
  "evidence_snippet": "Under low temperature conditions, pathway A dominates.",
  "subquestion_id": "SQ001",
  "confidence": 0.9
}
```

## Argument Graph JSON Skeleton

```json
{
  "topic": "",
  "primary_rq": "",
  "subquestions": [
    {"subquestion_id": "SQ001", "text": ""}
  ],
  "claim_nodes": [],
  "evidence_edges": [],
  "clusters": [],
  "section_plan": []
}
```

## Section Plan Rendering Template

For each section:
- Section title should align with one sub-question.
- Paragraph points should follow:
- `主张`
- `证据`
- `局限/边界`
