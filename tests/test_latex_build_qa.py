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


class LatexBuildQATests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_latex_build_qa_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index" / "claims.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "cards.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "records.jsonl").write_text("", encoding="utf-8")
        (self.root / "references" / "index" / "included_candidates.csv").write_text(
            "candidate_id,card_id,title,year,doi,arxiv_id,source_db,query_id,include_reason,screened_at\n",
            encoding="utf-8",
        )
        (self.root / "draft" / "main.tex").write_text(
            "\\section{A}\n\\label{sec:a}\nSee \\ref{sec:a}. \\cite{k1}\n",
            encoding="utf-8",
        )
        (self.root / "draft" / "full_draft_v1.tex").write_text(
            "\\section{B}\n\\label{sec:b}\nSee \\ref{sec:not_found}. \\cite{k_missing}\n",
            encoding="utf-8",
        )
        (self.root / "draft" / "latex" / "references.bib").write_text(
            "@article{k1,title={A},author={B},year={2024},journal={J}}\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def run_cmd(self, target: str = "full", strictness: str = "soft", overwrite: bool = True) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            target=target,
            main_tex="",
            full_draft_tex="",
            bib_path="",
            latex_build_report_md="",
            latex_build_log_txt="",
            run_compiler=False,
            strictness=strictness,
            overwrite=overwrite,
        )
        return reference_pipeline.latex_build_qa(args)

    def latest_manifest(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        manifest_path = run_dirs[-1] / "manifest.json"
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def test_happy_path_generates_outputs(self) -> None:
        self.run_cmd(target="main", strictness="soft", overwrite=True)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertTrue(Path(manifest["outputs"]["latex_build_report_md"]).exists())
        self.assertTrue(Path(manifest["outputs"]["latex_build_log_txt"]).exists())
        self.assertEqual(manifest["outputs"]["missing_ref_count"], 0)

    def test_soft_and_hard_behavior(self) -> None:
        self.run_cmd(target="full", strictness="soft", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "ok")
        self.run_cmd(target="full", strictness="hard", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "failed")

    def test_missing_dependency_fails(self) -> None:
        (self.root / "draft" / "full_draft_v1.tex").unlink()
        self.run_cmd(target="full", strictness="soft", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "failed")

    def test_idempotent_without_overwrite(self) -> None:
        self.run_cmd(target="main", strictness="soft", overwrite=True)
        manifest = self.latest_manifest()
        report_path = Path(manifest["outputs"]["latex_build_report_md"])
        report_path.write_text("MANUAL\n", encoding="utf-8")
        self.run_cmd(target="main", strictness="soft", overwrite=False)
        self.assertEqual(report_path.read_text(encoding="utf-8"), "MANUAL\n")

    def test_guardrail_does_not_modify_references_index(self) -> None:
        watched = [
            self.root / "references" / "index" / "claims.jsonl",
            self.root / "references" / "index" / "cards.jsonl",
            self.root / "references" / "index" / "records.jsonl",
            self.root / "references" / "index" / "included_candidates.csv",
        ]
        before = {path: path.read_text(encoding="utf-8") for path in watched}
        self.run_cmd(target="main", strictness="soft", overwrite=True)
        after = {path: path.read_text(encoding="utf-8") for path in watched}
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
