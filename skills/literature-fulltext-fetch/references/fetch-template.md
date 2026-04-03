# Fulltext Fetch Template

## Inputs

- `references/index/included_candidates.csv`
- `references/index/cards.jsonl`
- `references/index/records.jsonl`

## URL Resolution Priority

1. `records.jsonl` matched record `pdf_url`
2. `cards.jsonl` card-level `pdf_url` (if present)
3. Constructed arXiv URL from `arxiv_id`: `https://arxiv.org/pdf/<id>.pdf`

## Fetch Log Row Example

```csv
candidate_id,card_id,doi,arxiv_id,source_url,target_path,status,http_code,error,retried,retrieved_at
C000001,CARD_C000001,10.1000/a,,https://example.org/a.pdf,references/library/C000001__paper.pdf,downloaded,200,,0,2026-04-03T10:00:00
```

## Failure Handling

- `failed`: HTTP/network error after retry budget.
- `no_url`: no resolvable fulltext URL.
- Keep batch running after per-paper failure and report all outcomes in manifest.
