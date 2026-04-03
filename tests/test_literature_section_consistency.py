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


class LiteratureSectionConsistencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_section_consistency_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "sections").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        self.write_argument_graph()
        self.write_reference_index_placeholders()
        self.write_section_tex()
        self.write_section_drafts_record()

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_argument_graph(self) -> None:
        payload = {
            "topic": "Pentene over ZSM-5",
            "primary_rq": "How do pathways change under process conditions?",
            "section_plan": [
                {
                    "section_id": "SEC001",
                    "section_title": "Mechanism pathways",
                    "subquestion_id": "SQ001",
                }
            ],
        }
        (self.root / "references" / "index" / "argument_graph.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def write_reference_index_placeholders(self) -> None:
        for name in ("claims.jsonl", "cards.jsonl", "included_candidates.csv", "records.jsonl"):
            (self.root / "references" / "index" / name).write_text("placeholder\n", encoding="utf-8")

    def write_section_tex(self) -> None:
        text = (
            "\\section{Mechanism pathways}\n"
            "\\label{sec:mechanism-pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} Mechanism-A dominates under mild temperature.\\\\\n"
            "Mechanism-A dominates under mild temperature with stable conversion. \\cite{smith2024_pathway}\n"
            "\\paragraph{Paragraph 2}\n"
            "\\textbf{Claim.} mechanism-a dominates under mild temperature.\\\\\n"
            "Mechanism-A dominates under mild temperature with stable conversion. \\cite{smith2024_pathway}\n"
            "Selectivity remains sensitive to pressure windows.\n"
        )
        self.section_tex_path().write_text(text, encoding="utf-8")

    def write_section_drafts_record(self, missing_evidence_second: bool = False) -> None:
        run_dir = self.root / "draft" / "runs" / "run_20260403_010101"
        section_drafts_dir = run_dir / "section_drafts"
        section_drafts_dir.mkdir(parents=True, exist_ok=True)

        payload = {
            "section_id": "SEC001",
            "section_title": "Mechanism pathways",
            "section_stem": "sec_001_mechanism-pathways",
            "section_role": "mechanism_explanation",
            "generated_at": "2026-04-03T00:00:00",
            "paragraphs": [
                {
                    "paragraph_id": "SEC001-P01",
                    "paragraph_type": "mechanism_explanation",
                    "question_to_answer": "What pathway dominates?",
                    "main_conclusion": "Mechanism-A dominates under mild temperature.",
                    "evidence_used": [{"citation_key": "smith2024_pathway", "claim_id": "CLM000001", "support_points": []}],
                    "uncertainties": [],
                    "overclaim_guardrails": [],
                    "section_role": "mechanism_explanation",
                    "core_claim_id": "CLM000001",
                    "strength_label": "strong",
                    "missing_evidence": False,
                    "has_conflict": False,
                },
                {
                    "paragraph_id": "SEC001-P02",
                    "paragraph_type": "comparison",
                    "question_to_answer": "How does this compare at boundaries?",
                    "main_conclusion": "mechanism-a dominates under mild temperature.",
                    "evidence_used": [{"citation_key": "smith2024_pathway", "claim_id": "CLM000002", "support_points": []}],
                    "uncertainties": [],
                    "overclaim_guardrails": [],
                    "section_role": "comparison_analysis",
                    "core_claim_id": "CLM000002",
                    "strength_label": "medium",
                    "missing_evidence": missing_evidence_second,
                    "has_conflict": False,
                },
            ],
        }
        self.section_record_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def update_section_record_missing_evidence(self, enabled: bool) -> None:
        payload = json.loads(self.section_record_path().read_text(encoding="utf-8"))
        payload["paragraphs"][1]["missing_evidence"] = enabled
        self.section_record_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def section_tex_path(self) -> Path:
        return self.root / "draft" / "latex" / "sections" / "sec_001_mechanism-pathways.tex"

    def section_record_path(self) -> Path:
        return self.root / "draft" / "runs" / "run_20260403_010101" / "section_drafts" / "sec_001_mechanism-pathways.json"

    def run_revision(self, overwrite: bool = True, strictness: str = "soft", section_drafts_dir: str = "") -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            latex_sections_dir="",
            argument_graph_json="",
            section_drafts_dir=section_drafts_dir,
            consistency_report_json="",
            strictness=strictness,
            overwrite=overwrite,
        )
        return reference_pipeline.revise_section_consistency(args)

    def latest_manifest(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs)
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists())
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def latest_report(self) -> dict:
        manifest = self.latest_manifest()
        report_path = Path(manifest["outputs"]["section_consistency_report_json"])
        self.assertTrue(report_path.exists())
        return json.loads(report_path.read_text(encoding="utf-8"))

    def test_happy_path_generates_report_and_revised_tex(self) -> None:
        code = self.run_revision(overwrite=True)
        self.assertEqual(code, 0)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        report = self.latest_report()
        self.assertEqual(report["summary"]["section_count"], 1)
        self.assertIn("\\textbf{Claim.} Evidence suggests that", self.section_tex_path().read_text(encoding="utf-8"))

    def test_adjacent_duplication_is_removed(self) -> None:
        self.run_revision(overwrite=True)
        tex = self.section_tex_path().read_text(encoding="utf-8")
        self.assertEqual(tex.count("with stable conversion. \\cite{smith2024_pathway}"), 1)

    def test_term_consistency_is_unified(self) -> None:
        self.run_revision(overwrite=True)
        tex = self.section_tex_path().read_text(encoding="utf-8")
        self.assertNotIn("mechanism-a dominates under mild temperature", tex)
        self.assertIn("Mechanism-A dominates under mild temperature", tex)

    def test_logical_jump_gets_transition_sentence(self) -> None:
        self.run_revision(overwrite=True)
        tex = self.section_tex_path().read_text(encoding="utf-8")
        self.assertIn("By contrast, the following evidence clarifies comparative performance.", tex)

    def test_claim_evidence_order_is_downgraded_to_evidence_led(self) -> None:
        self.run_revision(overwrite=True)
        tex = self.section_tex_path().read_text(encoding="utf-8")
        self.assertGreaterEqual(tex.count("\\textbf{Claim.} Evidence suggests that"), 2)

    def test_missing_evidence_paragraph_adds_uncertainty_and_boundary(self) -> None:
        tex = self.section_tex_path().read_text(encoding="utf-8")
        tex = tex.replace(" \\cite{smith2024_pathway}", "")
        self.section_tex_path().write_text(tex, encoding="utf-8")
        self.update_section_record_missing_evidence(True)

        self.run_revision(overwrite=True)
        updated = self.section_tex_path().read_text(encoding="utf-8")
        self.assertIn(reference_pipeline.CONSISTENCY_BOUNDARY_SENTENCE, updated)
        self.assertIn(reference_pipeline.CONSISTENCY_UNCERTAINTY_SENTENCE, updated)

    def test_idempotent_without_overwrite_preserves_manual_edits(self) -> None:
        self.run_revision(overwrite=True)
        self.section_tex_path().write_text("% MANUAL_EDIT\n", encoding="utf-8")
        self.run_revision(overwrite=False)
        self.assertEqual(self.section_tex_path().read_text(encoding="utf-8"), "% MANUAL_EDIT\n")

    def test_missing_argument_graph_fails_with_auditable_error(self) -> None:
        (self.root / "references" / "index" / "argument_graph.json").unlink()
        code = self.run_revision(overwrite=True)
        self.assertEqual(code, 0)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")
        error_sources = [row["source"] for row in manifest["errors"]]
        self.assertIn("argument_graph_json", error_sources)

    def test_guardrail_does_not_mutate_references_index_or_library(self) -> None:
        watched = [
            self.root / "references" / "index" / "argument_graph.json",
            self.root / "references" / "index" / "claims.jsonl",
            self.root / "references" / "index" / "cards.jsonl",
            self.root / "references" / "index" / "included_candidates.csv",
        ]
        before = {path: path.read_text(encoding="utf-8") for path in watched}
        self.run_revision(overwrite=True)
        after = {path: path.read_text(encoding="utf-8") for path in watched}
        self.assertEqual(before, after)
        library_files = [path for path in (self.root / "references" / "library").rglob("*") if path.is_file()]
        self.assertEqual(library_files, [])


if __name__ == "__main__":
    unittest.main()
