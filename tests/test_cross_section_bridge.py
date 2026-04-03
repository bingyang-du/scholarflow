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


class CrossSectionBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_cross_section_bridge_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "sections").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index" / "claims.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "cards.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "records.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "included_candidates.csv").write_text(
            "candidate_id,card_id,title,year,doi,arxiv_id,source_db,query_id,include_reason,screened_at\n",
            encoding="utf-8",
        )
        (self.root / "draft" / "latex" / "sections" / "sec_001_a.tex").write_text("\\section{A}\n", encoding="utf-8")
        (self.root / "draft" / "latex" / "sections" / "sec_002_b.tex").write_text("\\section{B}\n", encoding="utf-8")
        (self.root / "references" / "index" / "argument_graph.json").write_text(
            json.dumps(
                {"section_plan": [{"section_title": "Section A"}, {"section_title": "Section B"}]},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def run_cmd(self, overwrite: bool = True) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            latex_sections_dir="",
            argument_graph_json="",
            bridge_plan_json="",
            bridges_tex="",
            overwrite=overwrite,
        )
        return reference_pipeline.generate_cross_section_bridges(args)

    def latest_manifest(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs)
        manifest_path = run_dirs[-1] / "manifest.json"
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def test_happy_path_generates_bridge_outputs(self) -> None:
        self.run_cmd(overwrite=True)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertTrue(Path(manifest["outputs"]["bridge_plan_json"]).exists())
        self.assertTrue(Path(manifest["outputs"]["bridges_tex"]).exists())
        self.assertEqual(manifest["outputs"]["bridge_count"], 1)

    def test_missing_dependency_fails(self) -> None:
        (self.root / "references" / "index" / "argument_graph.json").unlink()
        self.run_cmd(overwrite=True)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")

    def test_idempotent_without_overwrite_preserves_manual_file(self) -> None:
        self.run_cmd(overwrite=True)
        manifest = self.latest_manifest()
        bridges_path = Path(manifest["outputs"]["bridges_tex"])
        bridges_path.write_text("MANUAL\n", encoding="utf-8")
        self.run_cmd(overwrite=False)
        self.assertEqual(bridges_path.read_text(encoding="utf-8"), "MANUAL\n")

    def test_guardrail_does_not_modify_references_index(self) -> None:
        watched = [
            self.root / "references" / "index" / "claims.jsonl",
            self.root / "references" / "index" / "cards.jsonl",
            self.root / "references" / "index" / "records.jsonl",
            self.root / "references" / "index" / "included_candidates.csv",
        ]
        before = {path: path.read_text(encoding="utf-8") for path in watched}
        self.run_cmd(overwrite=True)
        after = {path: path.read_text(encoding="utf-8") for path in watched}
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
