# Screening Template

## Inputs

- `references/index/candidates.csv`
- `references/index/cards.jsonl`

## Decision Output (`screening_decisions.csv`)

Columns:
- `candidate_id,card_id,decision,reason_code,reason_note,reviewer_mode,screened_at,topic_relevance_score,body_inclusion,evidence_level`

## Include Output (`included_candidates.csv`)

Columns:
- `candidate_id,card_id,title,year,doi,arxiv_id,source_db,query_id,include_reason,screened_at`

## Decision Labels

- `include`: suitable for main-body evidence pool
- `exclude`: not suitable in current scope
- `unsure`: needs manual recheck

## Reason Codes

- `R1`: relevance weak
- `R2`: evidence insufficient
- `R3`: method mismatch or unclear
- `R4`: scope mismatch
- `R5`: duplicate/suboptimal
- `R6`: missing critical info

## Backwrite Rules

- `candidates.csv`: update `screen_state=screened` and `screen_decision`.
- `cards.jsonl`: update `screen_decision`, `screen_reason_code`, `screen_reason_note`, `screened_at`.
- Do not alter `dedup_status`.
