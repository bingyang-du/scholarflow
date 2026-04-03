import argparse
import importlib.util
import json
import shutil
import sys
import unittest
from pathlib import Path
from typing import Any


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "reference_pipeline.py"
SPEC = importlib.util.spec_from_file_location("reference_pipeline", SCRIPT_PATH)
assert SPEC and SPEC.loader
reference_pipeline = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = reference_pipeline
SPEC.loader.exec_module(reference_pipeline)


STAGE_FUNC_BY_NAME = {
    "search-candidates": "search_candidates",
    "cardify-candidates": "cardify_candidates",
    "screen-candidates": "screen_candidates",
    "fetch-fulltext": "fetch_fulltext",
    "outline-from-evidence": "outline_from_evidence",
    "generate-paragraph-plans": "generate_paragraph_plans",
    "assemble-evidence-packets": "assemble_evidence_packets",
    "generate-section-drafts": "generate_section_drafts",
    "revise-section-consistency": "revise_section_consistency",
    "section-citation-audit": "section_citation_audit",
    "section-release-gate": "section_release_gate",
    "generate-cross-section-bridges": "generate_cross_section_bridges",
    "export-claim-trace-matrix": "export_claim_trace_matrix",
    "ground-figure-table-links": "ground_figure_table_links",
    "generate-latex-draft": "generate_latex_draft",
    "assemble-full-draft": "assemble_full_draft",
    "citation-audit": "citation_audit",
    "latex-build-qa": "latex_build_qa",
}


class PipelineRunEntryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_pipeline_run_entry_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "sections").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "audit").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)
        (self.root / "config").mkdir(parents=True, exist_ok=True)

        self.topic_frame_path = self.root / "config" / "topic_frame.json"
        self.topic_frame_path.write_text(json.dumps({"topic_frame": {"topic": "Test topic"}}, ensure_ascii=False), encoding="utf-8")

        (self.root / "draft" / "latex" / "sections" / "sec_001_alpha.tex").write_text("\\section{Alpha}\n", encoding="utf-8")
        (self.root / "draft" / "latex" / "sections" / "sec_002_beta.tex").write_text("\\section{Beta}\n", encoding="utf-8")

        self.call_records: list[dict[str, Any]] = []
        self.status_overrides: dict[Any, str] = {}
        self.original_funcs: dict[str, Any] = {}
        for stage_name, func_name in STAGE_FUNC_BY_NAME.items():
            self.original_funcs[func_name] = getattr(reference_pipeline, func_name)
            setattr(reference_pipeline, func_name, self.make_stage_stub(stage_name))

    def tearDown(self) -> None:
        for func_name, original in self.original_funcs.items():
            setattr(reference_pipeline, func_name, original)
        if self.root.exists():
            shutil.rmtree(self.root)

    def make_stage_stub(self, stage_name: str):
        def _stub(args: argparse.Namespace) -> int:
            target = ""
            section_tex = getattr(args, "section_tex", "")
            section_stem = getattr(args, "section_stem", "")
            if section_tex:
                target = Path(section_tex).stem
            elif section_stem:
                target = str(section_stem)

            self.call_records.append({"stage": stage_name, "target": target, "args": args})

            base_dir = Path(args.base_dir).resolve() if getattr(args, "base_dir", "") else self.root
            paths = reference_pipeline.resolve_paths(base_dir=base_dir)
            reference_pipeline.ensure_workspace(paths)
            run_id, run_dir = reference_pipeline.create_run_dir(paths.runs_dir)

            status = self.status_overrides.get((stage_name, target), self.status_overrides.get(stage_name, "ok"))
            outputs: dict[str, Any] = {"stub_stage": stage_name}

            if stage_name == "revise-section-consistency":
                report_path = run_dir / "section_consistency_report.json"
                report_path.write_text(json.dumps({"sections": []}, ensure_ascii=False), encoding="utf-8")
                outputs["section_consistency_report_json"] = str(report_path)

            if stage_name == "section-citation-audit":
                stem = target or "sec_000_unknown"
                audit_json_path = paths.root / "draft" / "latex" / "audit" / f"section_{stem}_audit.json"
                audit_json_path.parent.mkdir(parents=True, exist_ok=True)
                audit_json_path.write_text(
                    json.dumps({"summary": {"section_stem": stem}, "findings": []}, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                outputs["section_audit_json"] = str(audit_json_path)

            manifest = {
                "run_id": run_id,
                "timestamp": "2026-04-03T00:00:00",
                "status": status,
                "outputs": outputs,
                "warnings": [],
                "errors": [],
            }
            reference_pipeline.write_manifest(run_dir, manifest)
            return 0

        return _stub

    def find_pipeline_manifest(self) -> dict[str, Any]:
        candidates: list[tuple[float, dict[str, Any]]] = []
        for manifest_path in (self.root / "draft" / "runs").glob("run_*/manifest.json"):
            if not manifest_path.exists():
                continue
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            outputs = payload.get("outputs", {}) if isinstance(payload, dict) else {}
            if isinstance(outputs, dict) and outputs.get("pipeline_run_summary_json"):
                candidates.append((manifest_path.stat().st_mtime, payload))
        self.assertTrue(candidates, "No pipeline run manifest found")
        return sorted(candidates, key=lambda item: item[0])[-1][1]

    def run_entry(
        self,
        *,
        from_stage: str = "search-candidates",
        to_stage: str = "full-draft",
        with_fulltext: bool = False,
        strictness: str = "soft",
        overwrite: bool = False,
        run_compiler: bool = False,
        continue_on_error: bool = False,
        topic_frame_json: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        args = argparse.Namespace(
            base_dir=str(self.root),
            topic_frame_json=str(self.topic_frame_path) if topic_frame_json is None else topic_frame_json,
            from_stage=from_stage,
            to_stage=to_stage,
            with_fulltext=with_fulltext,
            strictness=strictness,
            overwrite=overwrite,
            run_compiler=run_compiler,
            continue_on_error=continue_on_error,
        )
        code = reference_pipeline.run_pipeline(args)
        self.assertEqual(code, 0)
        manifest = self.find_pipeline_manifest()
        summary_path = Path(manifest["outputs"]["pipeline_run_summary_json"])
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        return manifest, summary

    def test_default_happy_path_runs_to_full_draft_without_fetch(self) -> None:
        manifest, summary = self.run_entry()
        self.assertEqual(manifest["status"], "ok")
        self.assertEqual(summary["selected_stages"][0], "search-candidates")
        self.assertEqual(summary["selected_stages"][-1], "assemble-full-draft")
        self.assertNotIn("fetch-fulltext", summary["selected_stages"])
        self.assertGreater(summary["executed_stage_count"], 0)

    def test_with_fulltext_includes_fetch_stage(self) -> None:
        _, summary = self.run_entry(with_fulltext=True)
        self.assertIn("fetch-fulltext", summary["selected_stages"])

    def test_from_to_slice_executes_expected_subset(self) -> None:
        _, summary = self.run_entry(
            from_stage="generate-paragraph-plans",
            to_stage="generate-section-drafts",
            topic_frame_json="",
        )
        self.assertEqual(
            summary["selected_stages"],
            ["generate-paragraph-plans", "assemble-evidence-packets", "generate-section-drafts"],
        )

    def test_section_stages_are_auto_batched_per_section(self) -> None:
        _, summary = self.run_entry(
            from_stage="section-citation-audit",
            to_stage="section-release-gate",
            topic_frame_json="",
        )
        audit_rows = [row for row in summary["stage_results"] if row.get("stage") == "section-citation-audit"]
        gate_rows = [row for row in summary["stage_results"] if row.get("stage") == "section-release-gate"]
        self.assertEqual(len(audit_rows), 2)
        self.assertEqual(len(gate_rows), 2)
        self.assertEqual({row.get("target") for row in audit_rows}, {"sec_001_alpha", "sec_002_beta"})

    def test_fail_fast_and_continue_on_error(self) -> None:
        self.status_overrides["assemble-evidence-packets"] = "failed"
        _, summary_fail_fast = self.run_entry(
            from_stage="generate-paragraph-plans",
            to_stage="generate-latex-draft",
            topic_frame_json="",
            continue_on_error=False,
        )
        stages_fail_fast = [row.get("stage") for row in summary_fail_fast["stage_results"]]
        self.assertEqual(summary_fail_fast["status"], "failed")
        self.assertNotIn("generate-section-drafts", stages_fail_fast)

        self.call_records.clear()
        _, summary_continue = self.run_entry(
            from_stage="generate-paragraph-plans",
            to_stage="generate-latex-draft",
            topic_frame_json="",
            continue_on_error=True,
        )
        stages_continue = [row.get("stage") for row in summary_continue["stage_results"]]
        self.assertEqual(summary_continue["status"], "failed")
        self.assertIn("generate-section-drafts", stages_continue)

    def test_parameter_passthrough_to_supported_stages(self) -> None:
        self.run_entry(
            from_stage="revise-section-consistency",
            to_stage="latex-build-qa",
            topic_frame_json="",
            strictness="hard",
            overwrite=True,
            run_compiler=True,
            continue_on_error=True,
        )
        revise_call = next(row for row in self.call_records if row["stage"] == "revise-section-consistency")
        latex_build_call = next(row for row in self.call_records if row["stage"] == "latex-build-qa")
        self.assertEqual(revise_call["args"].strictness, "hard")
        self.assertTrue(revise_call["args"].overwrite)
        self.assertEqual(latex_build_call["args"].strictness, "hard")
        self.assertTrue(latex_build_call["args"].overwrite)
        self.assertTrue(latex_build_call["args"].run_compiler)

    def test_missing_topic_frame_fails_when_required(self) -> None:
        _, summary = self.run_entry(
            from_stage="search-candidates",
            to_stage="screen-candidates",
            topic_frame_json="",
        )
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["executed_stage_count"], 0)
        self.assertTrue(any(row["source"] == "topic_frame_json" for row in summary["errors"]))


if __name__ == "__main__":
    unittest.main()
