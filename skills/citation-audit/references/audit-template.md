# Citation Audit Template

## Findings Categories

- `coverage`: important claim has no citation
- `support`: cited references do not support claim evidence
- `bib_fields`: bib entry misses required fields
- `text_only`: citation key appears in text but missing in bib
- `bib_unused`: bib entry exists but never cited

## Severity Guidance

- `high`: coverage, support, text_only
- `medium`: bib_fields, bib_unused
- `low`: optional informational notices

## Output Files

- `citation_audit.json`: full machine-readable payload
- `citation_findings.csv`: flat findings table
- `citation_audit_report.md`: reviewer-facing summary

## Suggested Overrides JSON

```json
{
  "force_important": ["critical sentence snippet"],
  "ignore": ["boilerplate sentence snippet"]
}
```
