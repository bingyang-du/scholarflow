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


class ClaimTraceMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_claim_trace_matrix_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "paragraph_plans").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "evidence_packets" / "sec_001_a").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        claims = [
            {
                "claim_id": "CLM000001",
                "candidate_id": "C000001",
                "claim_text": "A dominates.",
                "subquestion_id": "SQ001",
                "confidence": 0.9,
            },
            {
                "claim_id": "CLM000002",
                "candidate_id": "C000002",
                "claim_text": "Boundary uncertain.",
                "subquestion_id": "SQ001",
                "confidence": 0.8,
            },
        ]
        with (self.root / "references" / "index" / "claims.jsonl").open("w", encoding="utf-8") as handle:
            for row in claims:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

        (self.root / "draft" / "paragraph_plans" / "sec_001_a.json").write_text(
            json.dumps(
                {
                    "paragraphs": [
                        {
                            "paragraph_id": "SEC001-P01",
                            "required_evidence_ids": ["CLM000001"],
                            "supporting_candidate_ids": ["C000001"],
                        }
                    ]
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.root / "draft" / "evidence_packets" / "sec_001_a" / "SEC001-P01.json").write_text(
            json.dumps(
                {
                    "paragraph_id": "SEC001-P01",
                    "claim": {"core_claim_id": "CLM000001"},
                    "supporting_references": [
                        {
                            "claim_id": "CLM000001",
                            "candidate_id": "C000001",
                            "citation_key": "smith2024_pathway",
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.root / "draft" / "latex" / "references.bib").write_text(
            "@article{smith2024_pathway,title={A},author={B},year={2024},journal={J}}\n",
            encoding="utf-8",
        )
        (self.root / "references" / "index" / "cards.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "records.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "included_candidates.csv").write_text(
            "candidate_id,card_id,title,year,doi,arxiv_id,source_db,query_id,include_reason,screened_at\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def run_cmd(self, strictness: str = "soft", overwrite: bool = True) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            claims_jsonl="",
            paragraph_plans_dir="",
            evidence_packets_dir="",
            bib_path="",
            claim_trace_matrix_csv="",
            claim_trace_matrix_json="",
            strictness=strictness,
            overwrite=overwrite,
        )
        return reference_pipeline.export_claim_trace_matrix(args)

    def latest_manifest(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        manifest_path = run_dirs[-1] / "manifest.json"
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def test_happy_path_generates_trace_outputs(self) -> None:
        self.run_cmd(strictness="soft", overwrite=True)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertTrue(Path(manifest["outputs"]["claim_trace_matrix_csv"]).exists())
        self.assertTrue(Path(manifest["outputs"]["claim_trace_matrix_json"]).exists())
        with Path(manifest["outputs"]["claim_trace_matrix_csv"]).open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertGreaterEqual(len(rows), 1)

    def test_soft_and_hard_behavior(self) -> None:
        self.run_cmd(strictness="soft", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "ok")
        self.run_cmd(strictness="hard", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "failed")

    def test_missing_dependency_fails(self) -> None:
        (self.root / "references" / "index" / "claims.jsonl").unlink()
        self.run_cmd(strictness="soft", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "failed")

    def test_idempotent_without_overwrite_preserves_manual_edit(self) -> None:
        self.run_cmd(strictness="soft", overwrite=True)
        manifest = self.latest_manifest()
        out_json = Path(manifest["outputs"]["claim_trace_matrix_json"])
        out_json.write_text("MANUAL\n", encoding="utf-8")
        self.run_cmd(strictness="soft", overwrite=False)
        self.assertEqual(out_json.read_text(encoding="utf-8"), "MANUAL\n")

    def test_guardrail_does_not_modify_references_index(self) -> None:
        watched = [
            self.root / "references" / "index" / "claims.jsonl",
            self.root / "references" / "index" / "cards.jsonl",
            self.root / "references" / "index" / "records.jsonl",
            self.root / "references" / "index" / "included_candidates.csv",
        ]
        before = {path: path.read_text(encoding="utf-8") for path in watched}
        self.run_cmd(strictness="soft", overwrite=True)
        after = {path: path.read_text(encoding="utf-8") for path in watched}
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
