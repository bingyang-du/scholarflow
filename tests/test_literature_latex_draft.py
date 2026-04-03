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


class LiteratureLatexDraftTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_latex_draft_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "figures").mkdir(parents=True, exist_ok=True)
        (self.root / "outline").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        (self.root / "draft" / "latex" / "refs.bib").write_text("", encoding="utf-8")

        self.write_argument_graph()
        self.write_claims()
        self.write_included()
        self.write_records()

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_argument_graph(self) -> Path:
        path = self.root / "references" / "index" / "argument_graph.json"
        payload = {
            "topic": "Pentene isomerization over ZSM-5",
            "primary_rq": "How to explain pathway and boundary conditions?",
            "subquestions": [
                {"subquestion_id": "SQ001", "text": "Mechanism pathways"},
                {"subquestion_id": "SQ002", "text": "Boundary limitations"},
            ],
            "section_plan": [
                {
                    "section_id": "SEC001",
                    "subquestion_id": "SQ001",
                    "section_title": "子问题1：Mechanism pathways",
                    "paragraph_points": [
                        {
                            "claim": "Pathway A dominates under low temperature.",
                            "evidence": "Observed conversion/selectivity supports pathway A.",
                            "limitation_or_boundary": "High pressure extrapolation remains uncertain.",
                        }
                    ],
                },
                {
                    "section_id": "SEC002",
                    "subquestion_id": "SQ002",
                    "section_title": "子问题2：Boundary limitations",
                    "paragraph_points": [
                        {
                            "claim": "Feed composition shifts alter selectivity profile.",
                            "evidence": "Comparative run shows drift in selectivity.",
                            "limitation_or_boundary": "Dataset is small.",
                        }
                    ],
                },
            ],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def write_claims(self) -> Path:
        path = self.root / "references" / "index" / "claims.jsonl"
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
                "claim_type": "limitation",
                "claim_text": "Dataset is small and boundary uncertain.",
                "evidence_snippet": "Dataset is small and boundary uncertain.",
                "subquestion_id": "SQ002",
                "confidence": 0.75,
            },
        ]
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        return path

    def write_included(self) -> Path:
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
        return path

    def write_records(self) -> Path:
        path = self.root / "references" / "index" / "records.jsonl"
        rows = [
            {
                "paper_id": "smith2024_pathway",
                "citation_key": "smith2024_pathway",
                "doi": "10.1000/a",
                "title": "Paper A",
                "authors": ["Smith, Alice"],
                "year": 2024,
                "venue": "Catalysis Today",
                "abstract": "A",
                "pdf_url": "https://example.org/a.pdf",
            },
            {
                "doi": "",
                "arxiv_id": "2401.12345",
                "title": "Paper B",
                "authors": ["Lee, Bob"],
                "year": 2023,
                "venue": "arXiv",
                "abstract": "B",
                "pdf_url": "https://arxiv.org/pdf/2401.12345.pdf",
            },
        ]
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        return path

    def latest_manifest(self) -> dict:
        runs_dir = self.root / "draft" / "runs"
        run_dirs = sorted([path for path in runs_dir.glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs, "Expected at least one run directory")
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists(), "Expected manifest.json in latest run")
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def run_generate(self, overwrite: bool = False) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            argument_graph_json="",
            claims_jsonl="",
            included_candidates_csv="",
            records_jsonl="",
            refs_bib="",
            latex_dir="",
            figures_dir="",
            tables_dir="",
            overwrite=overwrite,
        )
        return reference_pipeline.generate_latex_draft(args)

    def test_happy_path_generates_modular_latex_outputs(self) -> None:
        code = self.run_generate()
        self.assertEqual(code, 0)
        self.assertTrue((self.root / "draft" / "latex" / "main.tex").exists())
        self.assertTrue((self.root / "draft" / "latex" / "outline.tex").exists())
        self.assertTrue((self.root / "draft" / "latex" / "references.bib").exists())
        self.assertTrue((self.root / "draft" / "latex" / "sections").exists())
        section_files = list((self.root / "draft" / "latex" / "sections").glob("sec_*.tex"))
        self.assertEqual(len(section_files), 2)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")

    def test_main_tex_stays_thin_and_sections_hold_content(self) -> None:
        self.run_generate()
        main_text = (self.root / "draft" / "latex" / "main.tex").read_text(encoding="utf-8")
        self.assertIn("\\input{outline.tex}", main_text)
        self.assertNotIn("Pathway A dominates under low temperature.", main_text)
        section_text = "".join(
            path.read_text(encoding="utf-8")
            for path in sorted((self.root / "draft" / "latex" / "sections").glob("sec_*.tex"))
        )
        self.assertIn("Pathway A dominates under low temperature.", section_text)

    def test_missing_argument_graph_or_claims_marks_failed(self) -> None:
        (self.root / "references" / "index" / "argument_graph.json").unlink()
        self.run_generate()
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertTrue(any(err["source"] == "argument_graph_json" for err in manifest["errors"]))

    def test_references_bib_contains_include_pool_entries(self) -> None:
        self.run_generate()
        refs_text = (self.root / "draft" / "latex" / "references.bib").read_text(encoding="utf-8")
        self.assertIn("smith2024_pathway", refs_text)
        self.assertIn("C000002", refs_text)

    def test_figures_are_scanned_and_fragment_generated(self) -> None:
        (self.root / "figures" / "pathway.png").write_bytes(b"fake")
        self.run_generate()
        figures_tex = (self.root / "draft" / "latex" / "sections" / "figures.tex").read_text(encoding="utf-8")
        self.assertIn("includegraphics", figures_tex)
        self.assertIn("pathway.png", figures_tex)

    def test_tables_csv_are_exported_to_latex_tables(self) -> None:
        tables_dir = self.root / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)
        (tables_dir / "metrics.csv").write_text("metric,value\nconversion,0.91\n", encoding="utf-8")
        self.run_generate()
        self.assertTrue((self.root / "draft" / "latex" / "tables" / "metrics.tex").exists())
        tables_tex = (self.root / "draft" / "latex" / "sections" / "tables.tex").read_text(encoding="utf-8")
        self.assertIn("\\input{tables/metrics.tex}", tables_tex)

    def test_idempotent_without_overwrite_preserves_manual_edits(self) -> None:
        self.run_generate()
        section_file = sorted((self.root / "draft" / "latex" / "sections").glob("sec_*.tex"))[0]
        section_file.write_text("% MANUAL_EDIT\n", encoding="utf-8")
        self.run_generate(overwrite=False)
        self.assertEqual(section_file.read_text(encoding="utf-8"), "% MANUAL_EDIT\n")

    def test_guardrail_does_not_mutate_references_index(self) -> None:
        target_files = [
            self.root / "references" / "index" / "argument_graph.json",
            self.root / "references" / "index" / "claims.jsonl",
            self.root / "references" / "index" / "included_candidates.csv",
            self.root / "references" / "index" / "records.jsonl",
        ]
        before = {path: path.read_text(encoding="utf-8") for path in target_files}
        self.run_generate()
        after = {path: path.read_text(encoding="utf-8") for path in target_files}
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
