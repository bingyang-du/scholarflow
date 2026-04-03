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


class FigureTableGroundingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_figure_table_grounding_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "sections").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "evidence_packets").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "figures").mkdir(parents=True, exist_ok=True)
        (self.root / "tables").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index" / "claims.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "cards.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "records.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "included_candidates.csv").write_text(
            "candidate_id,card_id,title,year,doi,arxiv_id,source_db,query_id,include_reason,screened_at\n",
            encoding="utf-8",
        )
        (self.root / "draft" / "latex" / "sections" / "sec_001_a.tex").write_text(
            "\\section{A}\n"
            "\\paragraph{P1}\n"
            "\\textbf{Claim.} Some claim.\\\\\n"
            "See Figure \\ref{fig:pathway}.\\n"
            "\\begin{figure}[htbp]\n\\caption{Pathway}\\label{fig:pathway}\\end{figure}\n",
            encoding="utf-8",
        )
        (self.root / "figures" / "pathway.png").write_bytes(b"x")
        (self.root / "tables" / "summary.csv").write_text("k,v\nx,1\n", encoding="utf-8")

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def run_cmd(self, strictness: str = "soft", overwrite: bool = True) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            latex_sections_dir="",
            figures_dir="",
            tables_dir="",
            evidence_packets_dir="",
            figure_table_grounding_md="",
            figure_table_manifest_json="",
            strictness=strictness,
            overwrite=overwrite,
        )
        return reference_pipeline.ground_figure_table_links(args)

    def latest_manifest(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        manifest_path = run_dirs[-1] / "manifest.json"
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def test_happy_path_generates_outputs(self) -> None:
        self.run_cmd(strictness="soft", overwrite=True)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertTrue(Path(manifest["outputs"]["figure_table_grounding_md"]).exists())
        self.assertTrue(Path(manifest["outputs"]["figure_table_manifest_json"]).exists())

    def test_soft_and_hard_behavior(self) -> None:
        self.run_cmd(strictness="soft", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "ok")
        self.run_cmd(strictness="hard", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "failed")

    def test_missing_dependency_fails(self) -> None:
        shutil.rmtree(self.root / "draft" / "latex" / "sections")
        self.run_cmd(strictness="soft", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "failed")

    def test_idempotent_without_overwrite(self) -> None:
        self.run_cmd(strictness="soft", overwrite=True)
        manifest = self.latest_manifest()
        report_path = Path(manifest["outputs"]["figure_table_grounding_md"])
        report_path.write_text("MANUAL\n", encoding="utf-8")
        self.run_cmd(strictness="soft", overwrite=False)
        self.assertEqual(report_path.read_text(encoding="utf-8"), "MANUAL\n")

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
