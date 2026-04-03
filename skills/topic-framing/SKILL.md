---
name: topic-framing
description: Frame a research topic before literature collection by producing a structured brief with research questions, scope boundaries, keyword groups, exclusion criteria, output type, and search constraints (time/language/venue). Use when a user gives a topic and wants to define search strategy and writing intent before any paper retrieval.
---

# Topic Framing

## Overview

Produce a decision-ready framing brief before running literature search. Do not retrieve papers in this skill.

## Workflow

1. Normalize topic and objective.
- Extract topic statement, target audience, and intended deliverable.
- Detect missing high-impact constraints.

2. Produce a `Topic Framing Brief (TFR-1)`.
- Define 1 primary research question and 2-5 sub-questions.
- Define scope boundaries with explicit in-scope and out-of-scope bullets.
- Build keyword groups for later retrieval.
- Define exclusion criteria to control noise.
- Define output type and expected structure.
- Define search constraints: time window, language range, venue preference.

3. Mark assumptions and handoff contract.
- Record defaults when user has not decided.
- Provide machine-readable handoff payload for downstream skills.

## Output Contract (TFR-1)

Always return two parts:

1. Human-readable summary with these headings in order:
- `Research Questions`
- `Scope Boundaries`
- `Keyword Groups`
- `Exclusions`
- `Output Type`
- `Search Constraints`
- `Assumptions`

2. Structured payload:

```yaml
topic_frame:
  version: "TFR-1"
  topic: ""
  objective: ""
  research_questions:
    primary: ""
    sub_questions: []
  scope:
    in_scope: []
    out_of_scope: []
  keywords:
    core_concepts: []
    domain_terms: []
    methods_or_mechanisms: []
    bilingual_synonyms: []
  exclusions:
    topics: []
    document_types: []
    low_priority_terms: []
  output:
    type: "review|course_report|experiment_report|design_proposal|thesis_chapter"
    expected_sections: []
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
  assumptions: []
```

## Guardrails

- Do not run keyword search, DOI lookup, arXiv lookup, or PDF download here.
- Avoid broad keywords without qualifiers.
- Keep exclusions specific and testable.
- Keep output type explicit; avoid mixed deliverable types unless user requests multi-output.
- Keep assumptions visible and minimal.

## Default Decision Rules

Apply these defaults only when user does not specify:
- Output type: `review`.
- Time range: recent 10 years.
- Language range: English only.
- Venue preference: balanced journal and conference.
- Keyword groups: include both canonical terms and common synonyms.

## Reference

Use [references/framing-template.md](references/framing-template.md) for:
- fast response template,
- output-type section presets,
- keyword quality checklist.
