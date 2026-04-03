import argparse
import csv
import importlib.util
import json
import shutil
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "reference_pipeline.py"
SPEC = importlib.util.spec_from_file_location("reference_pipeline", SCRIPT_PATH)
assert SPEC and SPEC.loader
reference_pipeline = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = reference_pipeline
SPEC.loader.exec_module(reference_pipeline)


class CitationAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_citation_audit_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "sections").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        self.write_argument_graph()
        self.write_claims()
        self.write_records()
        self.write_included()
        self.write_latex_project()

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_argument_graph(self) -> None:
        payload = {
            "section_plan": [
                {"section_id": "SEC001", "section_title": "Section 1", "subquestion_id": "SQ001"},
                {"section_id": "SEC002", "section_title": "Section 2", "subquestion_id": "SQ002"},
            ]
        }
        (self.root / "references" / "index" / "argument_graph.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def write_claims(self) -> None:
        rows = [
            {
                "claim_id": "CLM000001",
                "candidate_id": "C000001",
                "card_id": "CARD_C000001",
                "claim_type": "mechanism",
                "claim_text": "Pathway A dominates under low temperature.",
                "evidence_snippet": "Pathway A dominates under low temperature.",
                "subquestion_id": "SQ001",
                "confidence": 0.9,
            },
            {
                "claim_id": "CLM000002",
                "candidate_id": "C000002",
                "card_id": "CARD_C000002",
                "claim_type": "observation",
                "claim_text": "Feed composition shifts alter selectivity profile.",
                "evidence_snippet": "Feed composition shifts alter selectivity profile.",
                "subquestion_id": "SQ002",
                "confidence": 0.8,
            },
        ]
        path = self.root / "references" / "index" / "claims.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def write_records(self) -> None:
        rows = [
            {
                "paper_id": "smith2024_pathway",
                "citation_key": "smith2024_pathway",
                "doi": "10.1000/a",
                "title": "Pathway A dominates under low temperature",
                "authors": ["Smith, Alice"],
                "year": 2024,
                "venue": "Catalysis Today",
                "abstract": "Pathway A dominates under low temperature with stable selectivity.",
            },
            {
                "paper_id": "lee2023_boundary",
                "citation_key": "lee2023_boundary",
                "doi": "",
                "arxiv_id": "2401.12345",
                "title": "Feed composition shifts alter selectivity",
                "authors": ["Lee, Bob"],
                "year": 2023,
                "venue": "arXiv",
                "abstract": "Feed composition shifts alter selectivity profile.",
            },
        ]
        path = self.root / "references" / "index" / "records.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def write_included(self) -> None:
        path = self.root / "references" / "index" / "included_candidates.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=reference_pipeline.INCLUDED_CANDIDATE_COLUMNS)
            writer.writeheader()
            writer.writerow(
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                }
            )
            writer.writerow(
                {
                    "candidate_id": "C000002",
                    "card_id": "CARD_C000002",
                    "title": "Paper B",
                    "year": "2023",
                    "doi": "",
                    "arxiv_id": "2401.12345",
                    "source_db": "arxiv",
                    "query_id": "Q2",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                }
            )

    def write_latex_project(self) -> None:
        latex_dir = self.root / "draft" / "latex"
        (latex_dir / "main.tex").write_text(
            "\\documentclass{article}\n\\begin{document}\n\\input{outline.tex}\n\\end{document}\n",
            encoding="utf-8",
        )
        (latex_dir / "outline.tex").write_text(
            "\\input{sections/sec_001_section-1.tex}\n\\input{sections/sec_002_section-2.tex}\n",
            encoding="utf-8",
        )
        (latex_dir / "sections" / "sec_001_section-1.tex").write_text(
            "\\section{Section 1}\n"
            "\\paragraph{Point 1}\n"
            "\\textbf{Claim.} Pathway A dominates under low temperature.\\\\\n"
            "\\textit{Related citations:} \\cite{smith2024_pathway}\n",
            encoding="utf-8",
        )
        (latex_dir / "sections" / "sec_002_section-2.tex").write_text(
            "\\section{Section 2}\n"
            "\\paragraph{Point 1}\n"
            "\\textbf{Claim.} Feed composition shifts alter selectivity profile.\\\\\n"
            "\\textit{Related citations:} \\cite{lee2023_boundary}\n",
            encoding="utf-8",
        )
        (latex_dir / "references.bib").write_text(
            "@article{smith2024_pathway,\n"
            "  title = {Pathway A dominates under low temperature},\n"
            "  author = {Smith, Alice},\n"
            "  year = {2024},\n"
            "  journal = {Catalysis Today}\n"
            "}\n\n"
            "@misc{lee2023_boundary,\n"
            "  title = {Feed composition shifts alter selectivity},\n"
            "  author = {Lee, Bob},\n"
            "  year = {2023},\n"
            "  eprint = {2401.12345},\n"
            "  archivePrefix = {arXiv}\n"
            "}\n",
            encoding="utf-8",
        )

    def latest_manifest(self) -> dict:
        runs_dir = self.root / "draft" / "runs"
        run_dirs = sorted([path for path in runs_dir.glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs, "Expected at least one run directory")
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists(), "Expected manifest.json in latest run")
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def run_audit(self, strictness: str = "soft") -> tuple[dict, dict]:
        args = argparse.Namespace(
            base_dir=str(self.root),
            latex_dir="",
            main_tex="",
            outline_tex="",
            bib_path="",
            argument_graph_json="",
            claims_jsonl="",
            records_jsonl="",
            included_candidates_csv="",
            audit_overrides_json="",
            audit_output_dir="",
            strictness=strictness,
        )
        code = reference_pipeline.citation_audit(args)
        self.assertEqual(code, 0)
        manifest = self.latest_manifest()
        audit_json_path = Path(manifest["outputs"]["citation_audit_json"])
        audit_payload = json.loads(audit_json_path.read_text(encoding="utf-8"))
        return manifest, audit_payload

    def test_happy_path_outputs_reports_and_scores(self) -> None:
        manifest, payload = self.run_audit(strictness="soft")
        self.assertEqual(manifest["status"], "ok")
        self.assertEqual(payload["overall"]["findings_count"], 0)
        self.assertEqual(payload["overall"]["score"], 100)
        output_dir = self.root / "draft" / "latex" / "audit"
        self.assertTrue((output_dir / "citation_audit.json").exists())
        self.assertTrue((output_dir / "citation_findings.csv").exists())
        self.assertTrue((output_dir / "citation_audit_report.md").exists())

    def test_coverage_finding_when_important_claim_has_no_citation(self) -> None:
        section_path = self.root / "draft" / "latex" / "sections" / "sec_001_section-1.tex"
        section_path.write_text(
            "\\section{Section 1}\n\\paragraph{Point 1}\n\\textbf{Claim.} Pathway A dominates under low temperature.\\\\\n",
            encoding="utf-8",
        )
        _, payload = self.run_audit(strictness="soft")
        categories = [finding["category"] for finding in payload["findings"]]
        self.assertIn("coverage", categories)

    def test_support_finding_when_citation_does_not_support_claim(self) -> None:
        section_path = self.root / "draft" / "latex" / "sections" / "sec_001_section-1.tex"
        section_path.write_text(
            "\\section{Section 1}\n"
            "\\paragraph{Point 1}\n"
            "\\textbf{Claim.} Quantum plasmonic resonance dominates photonic lattice coupling.\\\\\n"
            "\\textit{Related citations:} \\cite{smith2024_pathway}\n",
            encoding="utf-8",
        )
        _, payload = self.run_audit(strictness="soft")
        categories = [finding["category"] for finding in payload["findings"]]
        self.assertIn("support", categories)

    def test_bib_missing_fields_are_reported(self) -> None:
        bib_path = self.root / "draft" / "latex" / "references.bib"
        bib_path.write_text(
            "@article{smith2024_pathway,\n"
            "  title = {Pathway A dominates under low temperature},\n"
            "  author = {Smith, Alice},\n"
            "  year = {2024}\n"
            "}\n",
            encoding="utf-8",
        )
        _, payload = self.run_audit(strictness="soft")
        categories = [finding["category"] for finding in payload["findings"]]
        self.assertIn("bib_fields", categories)

    def test_text_only_and_bib_unused_are_reported(self) -> None:
        section_path = self.root / "draft" / "latex" / "sections" / "sec_002_section-2.tex"
        section_path.write_text(
            "\\section{Section 2}\n"
            "\\paragraph{Point 1}\n"
            "\\textbf{Claim.} Feed composition shifts alter selectivity profile.\\\\\n"
            "\\textit{Related citations:} \\cite{missing_key}\n",
            encoding="utf-8",
        )
        bib_path = self.root / "draft" / "latex" / "references.bib"
        bib_path.write_text(
            "@article{smith2024_pathway,\n"
            "  title = {Pathway A dominates under low temperature},\n"
            "  author = {Smith, Alice},\n"
            "  year = {2024},\n"
            "  journal = {Catalysis Today}\n"
            "}\n\n"
            "@misc{unused2020,\n"
            "  title = {Unused Ref},\n"
            "  author = {Unused, User},\n"
            "  year = {2020},\n"
            "  url = {https://example.org}\n"
            "}\n",
            encoding="utf-8",
        )
        _, payload = self.run_audit(strictness="soft")
        categories = [finding["category"] for finding in payload["findings"]]
        self.assertIn("text_only", categories)
        self.assertIn("bib_unused", categories)

    def test_overrides_force_and_ignore_are_applied(self) -> None:
        section_path = self.root / "draft" / "latex" / "sections" / "sec_001_section-1.tex"
        section_path.write_text(
            "\\section{Section 1}\n"
            "\\paragraph{Point 1}\n"
            "\\textbf{Claim.} Pathway A dominates under low temperature.\\\\\n"
            "\\paragraph{Point 2}\n"
            "Potential.\n",
            encoding="utf-8",
        )
        overrides = {
            "ignore": ["Pathway A dominates under low temperature"],
            "force_important": ["Potential."],
        }
        (self.root / "draft" / "latex" / "audit_overrides.json").write_text(
            json.dumps(overrides, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _, payload = self.run_audit(strictness="soft")
        coverage_findings = [f for f in payload["findings"] if f["category"] == "coverage"]
        self.assertEqual(len(coverage_findings), 1)
        self.assertIn("Potential", coverage_findings[0]["claim_text"])

    def test_soft_policy_reports_issues_without_failing_status(self) -> None:
        section_path = self.root / "draft" / "latex" / "sections" / "sec_001_section-1.tex"
        section_path.write_text(
            "\\section{Section 1}\n\\paragraph{Point 1}\n\\textbf{Claim.} Pathway A dominates under low temperature.\\\\\n",
            encoding="utf-8",
        )
        manifest, payload = self.run_audit(strictness="soft")
        self.assertEqual(manifest["status"], "ok")
        self.assertGreater(payload["overall"]["findings_count"], 0)


if __name__ == "__main__":
    unittest.main()
