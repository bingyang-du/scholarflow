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


class SectionReleaseGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_section_release_gate_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "audit").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs" / "run_20260403_010101").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index" / "claims.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "cards.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "records.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "included_candidates.csv").write_text(
            "candidate_id,card_id,title,year,doi,arxiv_id,source_db,query_id,include_reason,screened_at\n",
            encoding="utf-8",
        )

        report = {
            "sections": [
                {
                    "section_stem": "sec_001_mechanism-pathways",
                    "section_title": "Mechanism pathways",
                    "issues": [{"severity": "medium", "message": "Need stronger transition."}],
                }
            ]
        }
        (self.root / "draft" / "runs" / "run_20260403_010101" / "section_consistency_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        section_audit = {
            "summary": {"section_stem": "sec_001_mechanism-pathways", "score": 82, "risk_level": "medium"},
            "findings": [{"severity": "high", "message": "Key claim missing citation."}],
        }
        (self.root / "draft" / "latex" / "audit" / "section_sec_001_mechanism-pathways_audit.json").write_text(
            json.dumps(section_audit, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def run_gate(self, strictness: str = "soft", overwrite: bool = True) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            section_stem="sec_001_mechanism-pathways",
            section_consistency_report_json="",
            section_audit_json="",
            gate_output_json="",
            gate_fixlist_md="",
            strictness=strictness,
            overwrite=overwrite,
        )
        return reference_pipeline.section_release_gate(args)

    def latest_manifest(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs)
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists())
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def test_happy_path_generates_gate_outputs(self) -> None:
        code = self.run_gate(strictness="soft", overwrite=True)
        self.assertEqual(code, 0)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertTrue(Path(manifest["outputs"]["gate_output_json"]).exists())
        self.assertTrue(Path(manifest["outputs"]["gate_fixlist_md"]).exists())
        self.assertIn(manifest["outputs"]["decision"], {"go", "revise", "block"})

    def test_missing_dependency_fails(self) -> None:
        (self.root / "draft" / "runs" / "run_20260403_010101" / "section_consistency_report.json").unlink()
        self.run_gate(strictness="soft", overwrite=True)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertTrue(any(err["source"] == "section_consistency_report_json" for err in manifest["errors"]))

    def test_soft_and_hard_behavior(self) -> None:
        self.run_gate(strictness="soft", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "ok")
        self.run_gate(strictness="hard", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "failed")

    def test_idempotent_without_overwrite_preserves_manual_edit(self) -> None:
        self.run_gate(strictness="soft", overwrite=True)
        manifest = self.latest_manifest()
        gate_path = Path(manifest["outputs"]["gate_fixlist_md"])
        gate_path.write_text("MANUAL\n", encoding="utf-8")
        self.run_gate(strictness="soft", overwrite=False)
        self.assertEqual(gate_path.read_text(encoding="utf-8"), "MANUAL\n")

    def test_guardrail_does_not_modify_references_index(self) -> None:
        watched = [
            self.root / "references" / "index" / "claims.jsonl",
            self.root / "references" / "index" / "cards.jsonl",
            self.root / "references" / "index" / "records.jsonl",
            self.root / "references" / "index" / "included_candidates.csv",
        ]
        before = {path: path.read_text(encoding="utf-8") for path in watched}
        self.run_gate(strictness="soft", overwrite=True)
        after = {path: path.read_text(encoding="utf-8") for path in watched}
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
