# Section Draft Template

## Inputs

- `draft/paragraph_plans/sec_*.json`
- `draft/evidence_packets/<section>/<paragraph>.json`
- optional `draft/section_roles.json`

## Rationale Record (run snapshot)

For each paragraph include:
- `paragraph_id`
- `question_to_answer`
- `main_conclusion`
- `evidence_used` (`claim_id`, `citation_key`, `support_points`)
- `uncertainties`
- `overclaim_guardrails`
- `section_role`

## LaTeX Paragraph Pattern

```tex
\paragraph{Paragraph N}
\textbf{Claim.} <main conclusion>\\
<role/question sentence>
<evidence sentence 1> \cite{key1}
<evidence sentence 2> \cite{key2}
Uncertainty note: <bounded uncertainty>
Scope guardrail: <anti-overclaim sentence>
```

## Section Template Placeholders (optional)

- `{{SECTION_TITLE}}`
- `{{SECTION_LABEL}}`
- `{{PARAGRAPH_BLOCKS}}`

