---
name: literature-section-drafting
description: Generate paragraph-level section drafts from paragraph plans and evidence packets. Use when Codex should write section tex content paragraph by paragraph with explicit evidence grounding and uncertainty guardrails.
---

# Literature Section Drafting

## Overview

Produce section drafts by paragraph units, not chapter-wide free writing.

## Workflow

1. Load required inputs.
- `draft/paragraph_plans/sec_*.json`
- `draft/evidence_packets/<section>/<paragraph>.json`
- optional `draft/section_roles.json`

2. Generate hidden rationale records first.
- For each paragraph, record:
- question to answer
- main conclusion
- evidence used
- uncertainties
- overclaim guardrails

3. Render LaTeX section draft.
- Output `draft/latex/sections/<section_stem>.tex`
- Keep evidence sentence citations explicit (`\cite{...}`)
- Use cautious language for missing/weak evidence.

4. Persist run snapshot.
- `draft/runs/run_*/section_drafts/<section_stem>.json`

## Guardrails

- Do not modify `references/index/*` in this stage.
- Do not download PDFs.
- Keep generated text evidence-bounded and avoid overclaiming.

## Reference

Use [references/section-draft-template.md](references/section-draft-template.md) for structure hints.

