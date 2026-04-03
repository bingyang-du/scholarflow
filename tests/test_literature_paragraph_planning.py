import argparse
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


class LiteratureParagraphPlanningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_paragraph_planning_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "paragraph_plans").mkdir(parents=True, exist_ok=True)
        (self.root / "outline").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        self.argument_graph_path = self.root / "references" / "index" / "argument_graph.json"
        self.claims_path = self.root / "references" / "index" / "claims.jsonl"
        self.outline_path = self.root / "outline" / "generated_outline.md"

        self.write_argument_graph()
        self.write_claims()
        self.write_outline()

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_argument_graph(self) -> None:
        payload = {
            "topic": "Pentene isomerization over ZSM-5",
            "primary_rq": "How to explain pathway and boundary conditions?",
            "subquestions": [
                {"subquestion_id": "SQ001", "text": "Mechanism pathways"},
                {"subquestion_id": "SQ002", "text": "Comparative performance"},
            ],
            "section_plan": [
                {
                    "section_id": "SEC001",
                    "subquestion_id": "SQ001",
                    "section_title": "Mechanism pathways",
                    "paragraph_points": [
                        {
                            "claim": "Pathway A dominates under mild conditions.",
                            "evidence": "Conversion/selectivity trend supports pathway A.",
                            "limitation_or_boundary": "High-pressure extrapolation is uncertain.",
                        }
                    ],
                },
                {
                    "section_id": "SEC002",
                    "subquestion_id": "SQ002",
                    "section_title": "Comparative performance",
                    "paragraph_points": [
                        {
                            "claim": "Catalyst X outperforms baseline catalyst.",
                            "evidence": "Higher selectivity is reported in comparative runs.",
                            "limitation_or_boundary": "",
                        }
                    ],
                },
            ],
        }
        self.argument_graph_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def write_claims(self) -> None:
        rows = [
            {
                "claim_id": "CLM000001",
                "candidate_id": "C000001",
                "card_id": "CARD_C000001",
                "claim_type": "mechanism",
                "claim_text": "Pathway A dominates under mild conditions on ZSM-5.",
                "evidence_snippet": "Pathway A dominates under mild conditions.",
                "subquestion_id": "SQ001",
                "confidence": 0.92,
            },
            {
                "claim_id": "CLM000002",
                "candidate_id": "C000001",
                "card_id": "CARD_C000001",
                "claim_type": "limitation",
                "claim_text": "High-pressure interpretation remains uncertain.",
                "evidence_snippet": "Evidence is limited at high pressure.",
                "subquestion_id": "SQ001",
                "confidence": 0.72,
            },
            {
                "claim_id": "CLM000003",
                "candidate_id": "C000002",
                "card_id": "CARD_C000002",
                "claim_type": "comparison",
                "claim_text": "Catalyst X performs better than baseline catalyst.",
                "evidence_snippet": "Selectivity is higher than baseline.",
                "subquestion_id": "SQ002",
                "confidence": 0.88,
            },
            {
                "claim_id": "CLM000004",
                "candidate_id": "C000003",
                "card_id": "CARD_C000003",
                "claim_type": "observation",
                "claim_text": "A kinetic model quantifies conversion trends.",
                "evidence_snippet": "The method relies on kinetic fitting.",
                "subquestion_id": "SQ002",
                "confidence": 0.79,
            },
        ]
        with self.claims_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def write_outline(self) -> None:
        self.outline_path.write_text(
            "# Wrong fallback outline\n\n## This title should not win when argument_graph exists\n",
            encoding="utf-8",
        )

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
            outline_markdown="",
            argument_graph_json="",
            claims_jsonl="",
            paragraph_plans_dir="",
            overwrite=overwrite,
        )
        return reference_pipeline.generate_paragraph_plans(args)

    def section_json_paths(self) -> list[Path]:
        return sorted((self.root / "draft" / "paragraph_plans").glob("sec_*.json"))

    def section_md_paths(self) -> list[Path]:
        return sorted((self.root / "draft" / "paragraph_plans").glob("sec_*.md"))

    def test_happy_path_generates_json_and_markdown_outputs(self) -> None:
        code = self.run_generate()
        self.assertEqual(code, 0)

        json_paths = self.section_json_paths()
        md_paths = self.section_md_paths()
        self.assertEqual(len(json_paths), 2)
        self.assertEqual(len(md_paths), 2)

        payload = json.loads(json_paths[0].read_text(encoding="utf-8"))
        self.assertIn("paragraphs", payload)
        self.assertTrue(payload["paragraphs"])
        required_fields = {
            "paragraph_id",
            "paragraph_type",
            "purpose",
            "core_claim_id",
            "core_claim_text",
            "required_evidence_ids",
            "supporting_candidate_ids",
            "section_id",
            "section_title",
            "subquestion_id",
        }
        self.assertTrue(required_fields.issubset(set(payload["paragraphs"][0].keys())))

        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertEqual(manifest["outputs"]["section_count"], 2)

    def test_input_precedence_prefers_argument_graph_over_outline_markdown(self) -> None:
        self.run_generate()
        manifest = self.latest_manifest()
        self.assertEqual(manifest["outputs"]["section_source"], "argument_graph")
        payload = json.loads(self.section_json_paths()[0].read_text(encoding="utf-8"))
        self.assertEqual(payload["section_title"], "Mechanism pathways")

    def test_adaptive_template_triggers_only_relevant_paragraph_types(self) -> None:
        self.run_generate()
        first_section = json.loads(self.section_json_paths()[0].read_text(encoding="utf-8"))
        paragraph_types = [row["paragraph_type"] for row in first_section["paragraphs"]]
        self.assertIn("背景段", paragraph_types)
        self.assertIn("机制解释段", paragraph_types)
        self.assertIn("争议/局限段", paragraph_types)
        self.assertIn("小结段", paragraph_types)
        self.assertNotIn("比较段", paragraph_types)

    def test_required_evidence_ids_are_valid_and_stable(self) -> None:
        self.run_generate()
        claim_ids = {
            "CLM000001",
            "CLM000002",
            "CLM000003",
            "CLM000004",
        }
        first_run = [path.read_text(encoding="utf-8") for path in self.section_json_paths()]
        for content in first_run:
            payload = json.loads(content)
            for paragraph in payload["paragraphs"]:
                ids = paragraph["required_evidence_ids"]
                self.assertEqual(ids, list(dict.fromkeys(ids)))
                self.assertTrue(set(ids).issubset(claim_ids))

        self.run_generate()
        second_run = [path.read_text(encoding="utf-8") for path in self.section_json_paths()]
        self.assertEqual(first_run, second_run)

    def test_missing_dependencies_marks_failed_with_auditable_error(self) -> None:
        self.claims_path.unlink()
        self.run_generate()
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertTrue(any(err["source"] == "claims_jsonl" for err in manifest["errors"]))

    def test_idempotent_without_overwrite_preserves_manual_edits(self) -> None:
        self.run_generate()
        md_path = self.section_md_paths()[0]
        md_path.write_text("MANUAL_EDIT\n", encoding="utf-8")
        self.run_generate(overwrite=False)
        self.assertEqual(md_path.read_text(encoding="utf-8"), "MANUAL_EDIT\n")

    def test_guardrail_does_not_mutate_references_index_or_library(self) -> None:
        index_files = [
            self.argument_graph_path,
            self.claims_path,
            self.root / "outline" / "generated_outline.md",
        ]
        before = {path: path.read_text(encoding="utf-8") for path in index_files}
        self.run_generate()
        after = {path: path.read_text(encoding="utf-8") for path in index_files}
        self.assertEqual(before, after)

        library_files = [p for p in (self.root / "references" / "library").rglob("*") if p.is_file()]
        self.assertEqual(library_files, [])


if __name__ == "__main__":
    unittest.main()

