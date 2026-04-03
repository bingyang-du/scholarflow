# LaTeX Build QA Template

- Inputs:
  - target tex (`main` or `full`)
  - bibliography file
- Checks:
  - missing `\ref` labels
  - missing `\cite` keys in bib
  - optional compile result
- Outputs:
  - report markdown
  - build log text
