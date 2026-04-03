# Literature Card Template

## Input Sources

- `references/index/candidates.csv`
- `references/index/records.jsonl` (optional metadata/abstract enhancement)

## Card JSONL Template

```json
{
  "card_id": "CARD_000001",
  "candidate_id": "C000001",
  "title": "",
  "year": "",
  "doi": "",
  "arxiv_id": "",
  "source_db": "",
  "query_id": "",
  "research_question": "",
  "method": "",
  "data": "",
  "main_findings": "",
  "limitations": "",
  "citable_points": [],
  "topic_relevance_score": 0,
  "body_inclusion": "no",
  "body_inclusion_reason": "",
  "evidence_level": "metadata_only",
  "card_status": "needs_review",
  "created_at": "",
  "updated_at": ""
}
```

## Scoring and Label Rules

- `topic_relevance_score`
- `0`: not relevant
- `1`: weakly relevant
- `2`: moderately relevant
- `3`: strongly relevant

- `body_inclusion`
- `yes`: score >= 2 and has usable citable points
- `maybe`: score == 1 or evidence is weak
- `no`: score == 0

## Missing Abstract Rule

- If abstract is unavailable, use conservative descriptions.
- Set `evidence_level=metadata_only`.
- Set `card_status=needs_review`.

## Candidate Linking Rule

- Ensure `candidates.csv` contains `card_status` column.
- Update per candidate:
- `completed` when card is generated with sufficient evidence.
- `needs_review` when evidence is missing or weak.
- `not_started` for rows not yet cardified.
