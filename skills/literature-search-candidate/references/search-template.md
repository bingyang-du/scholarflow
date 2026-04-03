# Search Candidate Template

## Expected Input (TFR-1)

```yaml
topic_frame:
  version: "TFR-1"
  topic: ""
  research_questions:
    primary: ""
    sub_questions: []
  keywords:
    core_concepts: []
    domain_terms: []
    methods_or_mechanisms: []
    bilingual_synonyms: []
  search_constraints:
    time_range:
      enabled: true
      start_year: 2015
      end_year: 2026
    language_range:
      enabled: true
      languages: ["en"]
    venue_preference:
      mode: "balanced"
      prioritize: ["journal", "conference"]
```

## Required Outputs

1. `references/index/search_sources.csv`
2. `references/index/candidates.csv`
3. `draft/runs/run_*/manifest.json`

## Screening Handoff Rule

- Keep all initial candidates with `screen_state=unreviewed`.
- Use `dedup_status` to separate unique records from duplicates.
- Downstream screening should focus on `dedup_status=unique` first.

## Query Quality Checklist

- Each query must be reproducible and logged.
- Avoid a single over-broad query.
- Include at least one query with mechanism/method terms.
- Respect time/language/venue constraints from input.
