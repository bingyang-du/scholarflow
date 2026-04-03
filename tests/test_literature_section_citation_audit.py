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


class LiteratureSectionCitationAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_section_citation_audit_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex" / "sections").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "evidence_packets" / "sec_001_mechanism-pathways").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs" / "run_20260403_010101" / "section_drafts").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        self.write_claims()
        self.write_records()
        self.write_bib()
        self.write_section_tex()
        self.write_section_drafts()
        self.write_packets()

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def section_stem(self) -> str:
        return "sec_001_mechanism-pathways"

    def section_tex_path(self) -> Path:
        return self.root / "draft" / "latex" / "sections" / f"{self.section_stem()}.tex"

    def bib_path(self) -> Path:
        return self.root / "draft" / "latex" / "references.bib"

    def section_packets_dir(self) -> Path:
        return self.root / "draft" / "evidence_packets" / self.section_stem()

    def section_draft_record_path(self) -> Path:
        return self.root / "draft" / "runs" / "run_20260403_010101" / "section_drafts" / f"{self.section_stem()}.json"

    def write_claims(self) -> None:
        rows = [
            {
                "claim_id": "CLM000001",
                "candidate_id": "C000001",
                "card_id": "CARD_C000001",
                "claim_type": "mechanism",
                "claim_text": "Pathway A tends to dominate under mild temperature.",
                "evidence_snippet": "Pathway A tends to dominate under mild temperature.",
                "subquestion_id": "SQ001",
                "confidence": 0.9,
            },
            {
                "claim_id": "CLM000002",
                "candidate_id": "C000002",
                "card_id": "CARD_C000002",
                "claim_type": "condition",
                "claim_text": "Pressure windows can alter selectivity boundary.",
                "evidence_snippet": "Pressure windows can alter selectivity boundary.",
                "subquestion_id": "SQ001",
                "confidence": 0.8,
            },
        ]
        path = self.root / "references" / "index" / "claims.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def write_records(self) -> None:
        rows = [
            {
                "paper_id": "smith2024_pathway",
                "citation_key": "smith2024_pathway",
                "doi": "10.1000/a",
                "title": "Pathway A dominates under mild temperature",
                "authors": ["Smith, Alice"],
                "year": 2024,
                "venue": "Catalysis Today",
                "abstract": "Pathway A dominates under mild temperature with stable conversion.",
            },
            {
                "paper_id": "lee2023_boundary",
                "citation_key": "lee2023_boundary",
                "doi": "",
                "arxiv_id": "2401.12345",
                "title": "Pressure windows alter selectivity boundary",
                "authors": ["Lee, Bob"],
                "year": 2023,
                "venue": "arXiv",
                "abstract": "Pressure windows alter selectivity boundary in the observed regime.",
            },
        ]
        path = self.root / "references" / "index" / "records.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def write_bib(self) -> None:
        self.bib_path().write_text(
            "@article{smith2024_pathway,\n"
            "  title = {Pathway A dominates under mild temperature},\n"
            "  author = {Smith, Alice},\n"
            "  year = {2024},\n"
            "  journal = {Catalysis Today}\n"
            "}\n\n"
            "@misc{lee2023_boundary,\n"
            "  title = {Pressure windows alter selectivity boundary},\n"
            "  author = {Lee, Bob},\n"
            "  year = {2023},\n"
            "  eprint = {2401.12345},\n"
            "  archivePrefix = {arXiv}\n"
            "}\n\n"
            "@misc{unused2020,\n"
            "  title = {Unused entry},\n"
            "  author = {Unused, User},\n"
            "  year = {2020},\n"
            "  url = {https://example.org}\n"
            "}\n",
            encoding="utf-8",
        )

    def write_section_tex(self) -> None:
        self.section_tex_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\label{sec:mechanism-pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} Pathway A tends to dominate under mild temperature.\\\\\n"
            "Pathway A dominates under mild temperature with stable conversion. \\cite{smith2024_pathway}\n"
            "\\paragraph{Paragraph 2}\n"
            "\\textbf{Claim.} Pressure windows can alter selectivity boundary.\\\\\n"
            "Pressure windows alter selectivity boundary in observed regime. \\cite{lee2023_boundary}\n",
            encoding="utf-8",
        )

    def write_section_drafts(self) -> None:
        payload = {
            "section_id": "SEC001",
            "section_title": "Mechanism pathways",
            "section_stem": self.section_stem(),
            "section_role": "mechanism_explanation",
            "generated_at": "2026-04-03T00:00:00",
            "paragraphs": [
                {
                    "paragraph_id": "SEC001-P01",
                    "paragraph_type": "mechanism_explanation",
                    "main_conclusion": "Pathway A tends to dominate under mild temperature.",
                    "missing_evidence": False,
                },
                {
                    "paragraph_id": "SEC001-P02",
                    "paragraph_type": "limitation_discussion",
                    "main_conclusion": "Pressure windows can alter selectivity boundary.",
                    "missing_evidence": False,
                },
            ],
        }
        self.section_draft_record_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def write_packets(self) -> None:
        packet_1 = {
            "section_id": "SEC001",
            "section_title": "Mechanism pathways",
            "subquestion_id": "SQ001",
            "paragraph_id": "SEC001-P01",
            "paragraph_type": "mechanism_explanation",
            "claim": {"core_claim_id": "CLM000001", "core_claim_text": "Pathway A tends to dominate under mild temperature."},
            "supporting_references": [
                {
                    "claim_id": "CLM000001",
                    "candidate_id": "C000001",
                    "citation_key": "smith2024_pathway",
                    "support_points": ["Pathway A dominates under mild temperature with stable conversion."],
                }
            ],
            "conflicting_evidence": [],
            "strength": {"score": 80, "label": "strong"},
            "missing_evidence": {"is_missing": False, "missing_claim_ids": [], "reason": ""},
        }
        packet_2 = {
            "section_id": "SEC001",
            "section_title": "Mechanism pathways",
            "subquestion_id": "SQ001",
            "paragraph_id": "SEC001-P02",
            "paragraph_type": "limitation_discussion",
            "claim": {"core_claim_id": "CLM000002", "core_claim_text": "Pressure windows can alter selectivity boundary."},
            "supporting_references": [
                {
                    "claim_id": "CLM000002",
                    "candidate_id": "C000002",
                    "citation_key": "lee2023_boundary",
                    "support_points": ["Pressure windows alter selectivity boundary in observed regime."],
                }
            ],
            "conflicting_evidence": [],
            "strength": {"score": 72, "label": "medium"},
            "missing_evidence": {"is_missing": False, "missing_claim_ids": [], "reason": ""},
        }
        (self.section_packets_dir() / "SEC001-P01.json").write_text(json.dumps(packet_1, ensure_ascii=False, indent=2), encoding="utf-8")
        (self.section_packets_dir() / "SEC001-P02.json").write_text(json.dumps(packet_2, ensure_ascii=False, indent=2), encoding="utf-8")

    def overwrite_packet(self, paragraph_id: str, updates: dict) -> None:
        path = self.section_packets_dir() / f"{paragraph_id}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.update(updates)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def latest_manifest(self) -> dict:
        run_dirs = sorted([path for path in (self.root / "draft" / "runs").glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs)
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists())
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def run_section_audit(self, strictness: str = "soft", section_tex: str = "", bib_path: str = "", evidence_packets_dir: str = "") -> tuple[dict, dict]:
        args = argparse.Namespace(
            base_dir=str(self.root),
            section_tex=section_tex or str(self.section_tex_path()),
            bib_path=bib_path,
            evidence_packets_dir=evidence_packets_dir,
            claims_jsonl="",
            records_jsonl="",
            section_drafts_dir="",
            audit_output_dir="",
            strictness=strictness,
        )
        code = reference_pipeline.section_citation_audit(args)
        self.assertEqual(code, 0)
        manifest = self.latest_manifest()
        audit_json = Path(manifest["outputs"]["section_audit_json"]) if manifest.get("outputs", {}).get("section_audit_json") else None
        payload = json.loads(audit_json.read_text(encoding="utf-8")) if audit_json and audit_json.exists() else {}
        return manifest, payload

    def finding_types(self, payload: dict) -> list[str]:
        return [row.get("type", "") for row in payload.get("findings", []) if isinstance(row, dict)]

    def test_happy_path_generates_section_json_and_md(self) -> None:
        manifest, payload = self.run_section_audit(strictness="soft")
        self.assertEqual(manifest["status"], "ok")
        self.assertEqual(payload["summary"]["section_stem"], self.section_stem())
        self.assertIn("score", payload["summary"])
        self.assertTrue(Path(manifest["outputs"]["section_audit_json"]).exists())
        self.assertTrue(Path(manifest["outputs"]["section_audit_md"]).exists())

    def test_coverage_hits_when_key_claim_has_no_citation(self) -> None:
        self.section_tex_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} Pathway A tends to dominate under mild temperature.\\\\\n"
            "Pathway A dominates under mild temperature with stable conversion.\n",
            encoding="utf-8",
        )
        _, payload = self.run_section_audit(strictness="soft")
        self.assertIn("coverage", self.finding_types(payload))

    def test_support_hits_when_citation_cannot_support_claim(self) -> None:
        self.section_tex_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} Quantum plasmonic resonance dominates photonic lattice coupling.\\\\\n"
            "Quantum plasmonic resonance dominates photonic lattice coupling. \\cite{smith2024_pathway}\n",
            encoding="utf-8",
        )
        _, payload = self.run_section_audit(strictness="soft")
        self.assertIn("support", self.finding_types(payload))

    def test_isolated_citation_hits_when_no_key_claim_anchor(self) -> None:
        self.section_tex_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\cite{smith2024_pathway}\n",
            encoding="utf-8",
        )
        _, payload = self.run_section_audit(strictness="soft")
        self.assertIn("isolated_citation", self.finding_types(payload))

    def test_cited_but_not_used_hits(self) -> None:
        self.section_tex_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} Pressure windows can alter selectivity boundary.\\\\\n"
            "Pressure windows alter selectivity boundary in observed regime. \\cite{lee2023_boundary,unused2020}\n",
            encoding="utf-8",
        )
        _, payload = self.run_section_audit(strictness="soft")
        self.assertIn("cited_not_used", self.finding_types(payload))

    def test_strong_claim_weak_evidence_hits(self) -> None:
        self.section_tex_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} This result proves pathway A dominates under mild temperature.\\\\\n"
            "Pathway A dominates under mild temperature with stable conversion. \\cite{smith2024_pathway}\n",
            encoding="utf-8",
        )
        self.overwrite_packet("SEC001-P01", {"strength": {"score": 25, "label": "weak"}})
        _, payload = self.run_section_audit(strictness="soft")
        self.assertIn("strong_claim_weak_evidence", self.finding_types(payload))

    def test_overgeneralization_hits(self) -> None:
        self.section_tex_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} Pathway A always dominates in all systems.\\\\\n"
            "Pathway A always dominates in all systems. \\cite{smith2024_pathway}\n",
            encoding="utf-8",
        )
        _, payload = self.run_section_audit(strictness="soft")
        self.assertIn("overgeneralization", self.finding_types(payload))

    def test_soft_and_hard_behavior(self) -> None:
        self.section_tex_path().write_text(
            "\\section{Mechanism pathways}\n"
            "\\paragraph{Paragraph 1}\n"
            "\\textbf{Claim.} Pathway A always dominates in all systems.\\\\\n"
            "Pathway A always dominates in all systems. \\cite{smith2024_pathway}\n",
            encoding="utf-8",
        )
        manifest_soft, payload_soft = self.run_section_audit(strictness="soft")
        self.assertEqual(manifest_soft["status"], "ok")
        self.assertIn("overgeneralization", self.finding_types(payload_soft))

        manifest_hard, _ = self.run_section_audit(strictness="hard")
        self.assertEqual(manifest_hard["status"], "failed")

    def test_missing_dependencies_fail_with_auditable_errors(self) -> None:
        with self.subTest("missing section"):
            manifest, _ = self.run_section_audit(section_tex=str(self.root / "draft" / "latex" / "sections" / "missing.tex"))
            self.assertEqual(manifest["status"], "failed")
            self.assertIn("section_tex", [row["source"] for row in manifest["errors"]])

        with self.subTest("missing bib"):
            manifest, _ = self.run_section_audit(bib_path=str(self.root / "draft" / "latex" / "missing.bib"))
            self.assertEqual(manifest["status"], "failed")
            self.assertIn("bib_path", [row["source"] for row in manifest["errors"]])

        with self.subTest("missing packets"):
            manifest, _ = self.run_section_audit(evidence_packets_dir=str(self.root / "draft" / "missing_packets"))
            self.assertEqual(manifest["status"], "failed")
            self.assertIn("evidence_packets_dir", [row["source"] for row in manifest["errors"]])


if __name__ == "__main__":
    unittest.main()
