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


class LiteratureSectionDraftingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_section_drafting_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "paragraph_plans").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "evidence_packets" / "sec_001_mechanism-pathways").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "sections").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        self.write_paragraph_plan()
        self.write_evidence_packets()
        self.write_reference_index_inputs()

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_paragraph_plan(self) -> None:
        payload = {
            "section_id": "SEC001",
            "section_title": "Mechanism pathways",
            "subquestion_id": "SQ001",
            "section_file_stem": "sec_001_mechanism-pathways",
            "paragraphs": [
                {
                    "paragraph_id": "SEC001-P01",
                    "paragraph_no": 1,
                    "paragraph_type": "机制解释段",
                    "purpose": "Explain dominant pathway evidence.",
                    "core_claim_id": "CLM000001",
                    "core_claim_text": "Pathway A dominates under mild temperature.",
                    "required_evidence_ids": ["CLM000001"],
                    "supporting_candidate_ids": ["C000001"],
                    "section_id": "SEC001",
                    "section_title": "Mechanism pathways",
                    "subquestion_id": "SQ001",
                },
                {
                    "paragraph_id": "SEC001-P02",
                    "paragraph_no": 2,
                    "paragraph_type": "争议/局限段",
                    "purpose": "State uncertainty and boundary.",
                    "core_claim_id": "CLM000002",
                    "core_claim_text": "Boundary remains uncertain at high pressure.",
                    "required_evidence_ids": ["CLM000002"],
                    "supporting_candidate_ids": [],
                    "section_id": "SEC001",
                    "section_title": "Mechanism pathways",
                    "subquestion_id": "SQ001",
                },
            ],
        }
        (self.root / "draft" / "paragraph_plans" / "sec_001_mechanism-pathways.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def write_evidence_packets(self) -> None:
        packet_1 = {
            "section_id": "SEC001",
            "section_title": "Mechanism pathways",
            "subquestion_id": "SQ001",
            "paragraph_id": "SEC001-P01",
            "paragraph_type": "机制解释段",
            "claim": {
                "core_claim_id": "CLM000001",
                "core_claim_text": "Pathway A dominates under mild temperature.",
            },
            "supporting_references": [
                {
                    "claim_id": "CLM000001",
                    "candidate_id": "C000001",
                    "citation_key": "smith2024_pathway",
                    "doi": "10.1000/a",
                    "arxiv_id": "",
                    "support_points": ["Pathway A dominates under mild temperature and stable conversion."],
                }
            ],
            "conflicting_evidence": [],
            "strength": {"score": 82, "label": "strong"},
            "missing_evidence": {"is_missing": False, "missing_claim_ids": [], "reason": ""},
            "provenance": {},
        }
        packet_2 = {
            "section_id": "SEC001",
            "section_title": "Mechanism pathways",
            "subquestion_id": "SQ001",
            "paragraph_id": "SEC001-P02",
            "paragraph_type": "争议/局限段",
            "claim": {
                "core_claim_id": "CLM000002",
                "core_claim_text": "Boundary remains uncertain at high pressure.",
            },
            "supporting_references": [],
            "conflicting_evidence": [
                {
                    "claim_id": "CLM000003",
                    "candidate_id": "C000002",
                    "citation_key": "lee2023_boundary",
                    "doi": "",
                    "arxiv_id": "2401.12345",
                    "conflict_point": "Selectivity decreases at higher pressure.",
                }
            ],
            "strength": {"score": 35, "label": "weak"},
            "missing_evidence": {
                "is_missing": True,
                "missing_claim_ids": ["CLM000002"],
                "reason": "No direct supporting references resolved.",
            },
            "provenance": {},
        }
        base = self.root / "draft" / "evidence_packets" / "sec_001_mechanism-pathways"
        (base / "SEC001-P01.json").write_text(json.dumps(packet_1, ensure_ascii=False, indent=2), encoding="utf-8")
        (base / "SEC001-P02.json").write_text(json.dumps(packet_2, ensure_ascii=False, indent=2), encoding="utf-8")

    def write_reference_index_inputs(self) -> None:
        claims_path = self.root / "references" / "index" / "claims.jsonl"
        claims_path.write_text(
            json.dumps(
                {
                    "claim_id": "CLM000001",
                    "candidate_id": "C000001",
                    "claim_type": "mechanism",
                    "claim_text": "Pathway A dominates under mild temperature.",
                    "subquestion_id": "SQ001",
                    "confidence": 0.9,
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        for path in [
            self.root / "references" / "index" / "cards.jsonl",
            self.root / "references" / "index" / "included_candidates.csv",
        ]:
            path.write_text("placeholder\n", encoding="utf-8")

    def run_generate(self, overwrite: bool = False, section_roles_json: str = "", latex_template_path: str = "") -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            paragraph_plans_dir="",
            evidence_packets_dir="",
            section_roles_json=section_roles_json,
            latex_sections_dir="",
            latex_template_path=latex_template_path,
            overwrite=overwrite,
        )
        return reference_pipeline.generate_section_drafts(args)

    def latest_manifest(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs)
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists())
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def latest_section_record(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs)
        record_path = run_dirs[-1] / "section_drafts" / "sec_001_mechanism-pathways.json"
        self.assertTrue(record_path.exists())
        return json.loads(record_path.read_text(encoding="utf-8"))

    def section_tex_text(self) -> str:
        path = self.root / "draft" / "latex" / "sections" / "sec_001_mechanism-pathways.tex"
        self.assertTrue(path.exists())
        return path.read_text(encoding="utf-8")

    def test_happy_path_generates_tex_and_section_record(self) -> None:
        code = self.run_generate()
        self.assertEqual(code, 0)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertEqual(manifest["outputs"]["section_count"], 1)
        self.assertEqual(manifest["outputs"]["paragraph_count"], 2)
        self.assertTrue((self.root / "draft" / "latex" / "sections" / "sec_001_mechanism-pathways.tex").exists())
        self.assertTrue(self.latest_section_record()["paragraphs"])

    def test_explanation_record_is_written_before_body_contract(self) -> None:
        self.run_generate()
        record = self.latest_section_record()
        first = record["paragraphs"][0]
        required = {
            "paragraph_id",
            "question_to_answer",
            "main_conclusion",
            "evidence_used",
            "uncertainties",
            "overclaim_guardrails",
            "section_role",
        }
        self.assertTrue(required.issubset(set(first.keys())))
        tex = self.section_tex_text()
        self.assertIn(first["main_conclusion"], tex)

    def test_role_source_prefers_explicit_file_then_fallback_inference(self) -> None:
        roles_path = self.root / "draft" / "section_roles.json"
        roles_path.write_text(
            json.dumps({"sections": {"sec_001_mechanism-pathways": "custom_role"}}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.run_generate(section_roles_json=str(roles_path))
        record = self.latest_section_record()
        self.assertEqual(record["section_role"], "custom_role")
        self.assertEqual(record["paragraphs"][0]["section_role"], "custom_role")

        roles_path.unlink()
        self.run_generate(overwrite=True)
        record_fallback = self.latest_section_record()
        self.assertEqual(record_fallback["section_role"], "mechanism_explanation")

    def test_evidence_sentence_must_include_citation(self) -> None:
        self.run_generate()
        tex = self.section_tex_text()
        self.assertIn("\\cite{smith2024_pathway}", tex)

    def test_missing_evidence_paragraph_uses_cautious_language(self) -> None:
        self.run_generate()
        tex = self.section_tex_text()
        self.assertIn("Available evidence suggests", tex)
        record = self.latest_section_record()
        second = [row for row in record["paragraphs"] if row["paragraph_id"] == "SEC001-P02"][0]
        self.assertTrue(second["missing_evidence"])
        self.assertTrue(second["uncertainties"])

    def test_idempotent_without_overwrite_preserves_manual_edits(self) -> None:
        self.run_generate()
        path = self.root / "draft" / "latex" / "sections" / "sec_001_mechanism-pathways.tex"
        path.write_text("% MANUAL_EDIT\n", encoding="utf-8")
        self.run_generate(overwrite=False)
        self.assertEqual(path.read_text(encoding="utf-8"), "% MANUAL_EDIT\n")

    def test_guardrail_does_not_mutate_references_index_or_library(self) -> None:
        watched = [
            self.root / "references" / "index" / "claims.jsonl",
            self.root / "references" / "index" / "cards.jsonl",
            self.root / "references" / "index" / "included_candidates.csv",
        ]
        before = {path: path.read_text(encoding="utf-8") for path in watched}
        self.run_generate()
        after = {path: path.read_text(encoding="utf-8") for path in watched}
        self.assertEqual(before, after)
        library_files = [p for p in (self.root / "references" / "library").rglob("*") if p.is_file()]
        self.assertEqual(library_files, [])


if __name__ == "__main__":
    unittest.main()

