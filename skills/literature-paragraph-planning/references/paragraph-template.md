# Paragraph Plan Template

## Inputs

- `references/index/argument_graph.json`
- `references/index/claims.jsonl`
- `outline/generated_outline.md` (fallback only when `section_plan` missing)

## Paragraph Type Set

- `背景段`: context and scope setup
- `定义段`: key concept or term definition
- `比较段`: contrast among approaches/evidence
- `方法段`: method and evidence-source explanation
- `机制解释段`: mechanism-level causal explanation
- `争议/局限段`: uncertainty, boundary, controversy
- `小结段`: section summary and transition

## Per-Section JSON Example

```json
{
  "section_id": "SEC001",
  "section_title": "Mechanism pathways",
  "subquestion_id": "SQ001",
  "section_file_stem": "sec_001_mechanism-pathways",
  "paragraphs": [
    {
      "paragraph_id": "SEC001-P01",
      "paragraph_no": 1,
      "paragraph_type": "背景段",
      "purpose": "交代本节子问题与证据边界，建立阅读上下文。",
      "core_claim_id": "CLM000001",
      "core_claim_text": "Pathway A dominates under mild conditions on ZSM-5.",
      "required_evidence_ids": ["CLM000001"],
      "supporting_candidate_ids": ["C000001"],
      "section_id": "SEC001",
      "section_title": "Mechanism pathways",
      "subquestion_id": "SQ001"
    }
  ]
}
```

## Markdown Rendering Skeleton

For each paragraph:
- paragraph id and type
- purpose
- core claim (`claim_id + text`)
- required evidence IDs (`claim_id` list)
- supporting candidate IDs

