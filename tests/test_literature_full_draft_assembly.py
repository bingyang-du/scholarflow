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


class LiteratureFullDraftAssemblyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_full_draft_assembly_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "sections").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "audit").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "templates").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        self.write_reference_index_placeholders()
        self.write_bib()
        self.write_sections()
        self.write_section_audits()
        self.write_templates()

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_reference_index_placeholders(self) -> None:
        (self.root / "references" / "index" / "argument_graph.json").write_text("{}", encoding="utf-8")
        (self.root / "references" / "index" / "claims.jsonl").write_text("{}", encoding="utf-8")
        (self.root / "references" / "index" / "cards.jsonl").write_text("{}", encoding="utf-8")
        (self.root / "references" / "index" / "records.jsonl").write_text("{}", encoding="utf-8")
        (self.root / "references" / "index" / "included_candidates.csv").write_text(
            "candidate_id,card_id,title,year,doi,arxiv_id,source_db,query_id,include_reason,screened_at\n"
            "C1,CARD1,Paper A,2024,10.1000/a,,crossref,Q1,ok,2026-04-03T00:00:00\n",
            encoding="utf-8",
        )

    def write_bib(self) -> None:
        (self.root / "draft" / "latex" / "references.bib").write_text(
            "@article{smith2024_pathway,\n"
            "  title = {Pathway A dominates under mild temperature},\n"
            "  author = {Smith, Alice},\n"
            "  year = {2024},\n"
            "  journal = {Catalysis Today}\n"
            "}\n\n"
            "@article{lee2023_boundary,\n"
            "  title = {Boundary limitations in pathway response},\n"
            "  author = {Lee, Bob},\n"
            "  year = {2023},\n"
            "  journal = {Catalysis Letters}\n"
            "}\n",
            encoding="utf-8",
        )

    def section_one_path(self) -> Path:
        return self.root / "draft" / "latex" / "sections" / "sec_001_mechanism-pathways.tex"

    def section_two_path(self) -> Path:
        return self.root / "draft" / "latex" / "sections" / "sec_002_boundary-limitations.tex"

    def write_sections(self) -> None:
        self.section_one_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\label{sec:mechanism-pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} Mechanism-A dominates under mild temperature.\\\\\n"
            "Evidence in reported windows supports this trend. \\cite{smith2024_pathway}\n"
            "See Figure \\ref{fig:pathway} for pathway structure.\n"
            "\\begin{figure}[htbp]\n"
            "\\centering\n"
            "\\caption{Pathway overview}\n"
            "\\label{fig:pathway}\n"
            "\\end{figure}\n",
            encoding="utf-8",
        )
        self.section_two_path().write_text(
            "\\section{Boundary limitations}\n"
            "\\label{sec:boundary-limitations}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} mechanism-a remains sensitive at boundaries.\\\\\n"
            "Boundary observations remain condition-dependent. \\cite{lee2023_boundary}\n"
            "This extends Section \\ref{sec:mechanism-pathways}.\n",
            encoding="utf-8",
        )

    def write_section_audits(self) -> None:
        self.write_section_audit(
            "sec_001_mechanism-pathways",
            "Mechanism pathways",
            90,
            "low",
            [],
        )
        self.write_section_audit(
            "sec_002_boundary-limitations",
            "Boundary limitations",
            84,
            "medium",
            [],
        )

    def write_section_audit(
        self,
        section_stem: str,
        section_title: str,
        score: int,
        risk_level: str,
        findings: list[dict],
    ) -> None:
        finding_counts: dict[str, int] = {}
        for row in findings:
            if not isinstance(row, dict):
                continue
            finding_type = str(row.get("type", "")).strip()
            if not finding_type:
                continue
            finding_counts[finding_type] = finding_counts.get(finding_type, 0) + 1
        payload = {
            "summary": {
                "section_stem": section_stem,
                "section_title": section_title,
                "key_claim_count": 2,
                "finding_counts_by_type": finding_counts,
                "score": score,
                "risk_level": risk_level,
            },
            "findings": findings,
        }
        path = self.root / "draft" / "latex" / "audit" / f"section_{section_stem}_audit.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def write_templates(self) -> None:
        (self.root / "draft" / "templates" / "abstract.tex").write_text(
            "This abstract captures objective, evidence scope, and limitations.",
            encoding="utf-8",
        )
        (self.root / "draft" / "templates" / "conclusion.tex").write_text(
            "The current evidence points to pathway-dependent behavior under bounded conditions.",
            encoding="utf-8",
        )

    def run_assemble(self, strictness: str = "soft", overwrite: bool = True) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            latex_sections_dir="",
            section_audit_dir="",
            bib_path="",
            abstract_template="",
            conclusion_template="",
            output_main_tex="",
            output_full_draft_tex="",
            full_draft_review_md="",
            strictness=strictness,
            overwrite=overwrite,
        )
        return reference_pipeline.assemble_full_draft(args)

    def latest_manifest(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs)
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists())
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def outputs(self) -> dict:
        manifest = self.latest_manifest()
        return manifest.get("outputs", {})

    def test_happy_path_generates_full_draft_outputs(self) -> None:
        code = self.run_assemble(strictness="soft", overwrite=True)
        self.assertEqual(code, 0)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        out = manifest["outputs"]
        self.assertTrue(Path(out["output_main_tex"]).exists())
        self.assertTrue(Path(out["output_full_draft_tex"]).exists())
        self.assertTrue(Path(out["full_draft_review_md"]).exists())
        self.assertEqual(out["section_count"], 2)
        self.assertEqual(out["section_audit_count"], 2)

    def test_missing_templates_fall_back_to_placeholder_with_warning(self) -> None:
        (self.root / "draft" / "templates" / "abstract.tex").unlink()
        (self.root / "draft" / "templates" / "conclusion.tex").unlink()
        self.run_assemble(strictness="soft", overwrite=True)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        warning_sources = [row.get("source", "") for row in manifest.get("warnings", [])]
        self.assertIn("abstract_template", warning_sources)
        self.assertIn("conclusion_template", warning_sources)
        full_text = Path(manifest["outputs"]["output_full_draft_tex"]).read_text(encoding="utf-8")
        self.assertIn("TODO: Summarize research objective", full_text)
        self.assertIn("TODO: Synthesize chapter-level evidence", full_text)

    def test_intro_and_conclusion_callback_are_injected(self) -> None:
        self.run_assemble(strictness="soft", overwrite=True)
        full_text = Path(self.outputs()["output_full_draft_tex"]).read_text(encoding="utf-8")
        self.assertIn("\\section{Introduction}", full_text)
        self.assertIn("To avoid overclaiming", full_text)
        self.assertIn("Taken together, the chapter-level evidence indicates", full_text)

    def test_terminology_consistency_is_applied(self) -> None:
        self.run_assemble(strictness="soft", overwrite=True)
        full_text = Path(self.outputs()["output_full_draft_tex"]).read_text(encoding="utf-8")
        self.assertIn("Mechanism-A dominates under mild temperature", full_text)
        self.assertNotIn("mechanism-a remains sensitive at boundaries", full_text)

    def test_figure_table_reference_checks_detect_missing_and_orphan(self) -> None:
        self.section_one_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\label{sec:mechanism-pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "See Figure \\ref{fig:missing}. \\cite{smith2024_pathway}\n"
            "\\label{fig:orphan}\n",
            encoding="utf-8",
        )
        self.run_assemble(strictness="soft", overwrite=True)
        out = self.outputs()
        self.assertGreaterEqual(out["figure_table_ref_issue_count"], 2)
        self.assertGreaterEqual(out["crossref_issue_count"], 1)

    def test_crossref_missing_label_is_detected(self) -> None:
        text = self.section_two_path().read_text(encoding="utf-8")
        text += "See Section \\ref{sec:not-exist} for deferred discussion.\n"
        self.section_two_path().write_text(text, encoding="utf-8")
        self.run_assemble(strictness="soft", overwrite=True)
        self.assertGreaterEqual(self.outputs()["crossref_issue_count"], 1)

    def test_section_audit_aggregation_marks_unresolved_high_risk(self) -> None:
        self.write_section_audit(
            "sec_002_boundary-limitations",
            "Boundary limitations",
            55,
            "high",
            [
                {
                    "finding_id": "SF0001",
                    "type": "support",
                    "severity": "high",
                    "paragraph_id": "SEC002-P01",
                    "claim_text": "Strong claim example",
                    "citation_keys": ["lee2023_boundary"],
                    "message": "Support overlap is weak.",
                    "evidence_refs": [],
                }
            ],
        )
        self.run_assemble(strictness="soft", overwrite=True)
        out = self.outputs()
        self.assertGreaterEqual(out["unresolved_high_risk_count"], 1)
        review = Path(out["full_draft_review_md"]).read_text(encoding="utf-8")
        self.assertIn("sec_002_boundary-limitations", review)

    def test_idempotent_with_overwrite_false_preserves_manual_outputs(self) -> None:
        self.run_assemble(strictness="soft", overwrite=True)
        main_path = Path(self.outputs()["output_main_tex"])
        main_path.write_text("% MANUAL_EDIT\n", encoding="utf-8")
        self.run_assemble(strictness="soft", overwrite=False)
        self.assertEqual(main_path.read_text(encoding="utf-8"), "% MANUAL_EDIT\n")

    def test_soft_and_hard_strictness_behavior(self) -> None:
        self.write_section_audit(
            "sec_002_boundary-limitations",
            "Boundary limitations",
            50,
            "high",
            [
                {
                    "finding_id": "SF0009",
                    "type": "coverage",
                    "severity": "high",
                    "paragraph_id": "SEC002-P01",
                    "claim_text": "Uncovered claim",
                    "citation_keys": [],
                    "message": "No citation.",
                    "evidence_refs": [],
                }
            ],
        )
        self.run_assemble(strictness="soft", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "ok")
        self.run_assemble(strictness="hard", overwrite=True)
        self.assertEqual(self.latest_manifest()["status"], "failed")

    def test_guardrail_does_not_modify_references_index_inputs(self) -> None:
        watched = [
            self.root / "references" / "index" / "argument_graph.json",
            self.root / "references" / "index" / "claims.jsonl",
            self.root / "references" / "index" / "cards.jsonl",
            self.root / "references" / "index" / "included_candidates.csv",
            self.root / "references" / "index" / "records.jsonl",
        ]
        before = {path: path.read_text(encoding="utf-8") for path in watched}
        self.run_assemble(strictness="soft", overwrite=True)
        after = {path: path.read_text(encoding="utf-8") for path in watched}
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
