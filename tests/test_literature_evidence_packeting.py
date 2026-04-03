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


class LiteratureEvidencePacketingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_evidence_packeting_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "paragraph_plans").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "latex").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "evidence_packets").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        self.claims_path = self.root / "references" / "index" / "claims.jsonl"
        self.cards_path = self.root / "references" / "index" / "cards.jsonl"
        self.included_path = self.root / "references" / "index" / "included_candidates.csv"
        self.bib_path = self.root / "draft" / "latex" / "references.bib"
        self.overrides_path = self.root / "draft" / "evidence_packets" / "packet_overrides.json"

        self.write_paragraph_plans()
        self.write_claims()
        self.write_cards()
        self.write_included()
        self.write_bib()

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_paragraph_plans(self) -> None:
        section_one = {
            "section_id": "SEC001",
            "section_title": "Mechanism pathways",
            "subquestion_id": "SQ001",
            "section_file_stem": "sec_001_mechanism-pathways",
            "paragraphs": [
                {
                    "paragraph_id": "SEC001-P01",
                    "paragraph_no": 1,
                    "paragraph_type": "机制解释段",
                    "purpose": "Explain mechanism and supporting evidence.",
                    "core_claim_id": "CLM000001",
                    "core_claim_text": "Selectivity increases under pathway A.",
                    "required_evidence_ids": ["CLM000001", "CLM009999"],
                    "supporting_candidate_ids": ["C000001"],
                    "section_id": "SEC001",
                    "section_title": "Mechanism pathways",
                    "subquestion_id": "SQ001",
                }
            ],
        }
        section_two = {
            "section_id": "SEC002",
            "section_title": "Comparative boundary",
            "subquestion_id": "SQ002",
            "section_file_stem": "sec_002_comparative-boundary",
            "paragraphs": [
                {
                    "paragraph_id": "SEC002-P01",
                    "paragraph_no": 1,
                    "paragraph_type": "比较段",
                    "purpose": "Compare catalyst outcomes.",
                    "core_claim_id": "CLM000004",
                    "core_claim_text": "Catalyst B performs better than baseline.",
                    "required_evidence_ids": ["CLM000004"],
                    "supporting_candidate_ids": ["C000002"],
                    "section_id": "SEC002",
                    "section_title": "Comparative boundary",
                    "subquestion_id": "SQ002",
                }
            ],
        }
        (self.root / "draft" / "paragraph_plans" / "sec_001_mechanism-pathways.json").write_text(
            json.dumps(section_one, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (self.root / "draft" / "paragraph_plans" / "sec_002_comparative-boundary.json").write_text(
            json.dumps(section_two, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def write_claims(self) -> None:
        rows = [
            {
                "claim_id": "CLM000001",
                "candidate_id": "C000001",
                "card_id": "CARD_C000001",
                "claim_type": "mechanism",
                "claim_text": "Selectivity increases under pathway A.",
                "evidence_snippet": "Selectivity increases under pathway A.",
                "subquestion_id": "SQ001",
                "confidence": 0.9,
            },
            {
                "claim_id": "CLM000002",
                "candidate_id": "C000002",
                "card_id": "CARD_C000002",
                "claim_type": "observation",
                "claim_text": "Selectivity decreases when severity rises.",
                "evidence_snippet": "Selectivity decreases at higher severity.",
                "subquestion_id": "SQ001",
                "confidence": 0.78,
            },
            {
                "claim_id": "CLM000003",
                "candidate_id": "C000003",
                "card_id": "CARD_C000003",
                "claim_type": "observation",
                "claim_text": "Excluded candidate appears positive.",
                "evidence_snippet": "Excluded evidence should not enter packets.",
                "subquestion_id": "SQ001",
                "confidence": 0.95,
            },
            {
                "claim_id": "CLM000004",
                "candidate_id": "C000002",
                "card_id": "CARD_C000002",
                "claim_type": "comparison",
                "claim_text": "Catalyst B performs better than baseline.",
                "evidence_snippet": "Catalyst B outperforms baseline in selectivity.",
                "subquestion_id": "SQ002",
                "confidence": 0.86,
            },
        ]
        with self.claims_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def write_cards(self) -> None:
        rows = [
            {
                "card_id": "CARD_C000001",
                "candidate_id": "C000001",
                "title": "Pathway A mechanism paper",
                "year": "2024",
                "doi": "10.1000/a",
                "arxiv_id": "",
                "source_db": "crossref",
                "query_id": "Q1",
                "research_question": "",
                "method": "Kinetic model estimates pathway preference.",
                "data": "",
                "main_findings": "Selectivity increases under pathway A.",
                "limitations": "Boundary at extreme severity is unclear.",
                "citable_points": ["Increase trend is stable in repeated runs."],
                "topic_relevance_score": 3,
                "body_inclusion": "yes",
                "body_inclusion_reason": "relevant",
                "evidence_level": "metadata_abstract",
                "card_status": "completed",
                "created_at": "2026-04-03T00:00:00",
                "updated_at": "2026-04-03T00:00:00",
            },
            {
                "card_id": "CARD_C000002",
                "candidate_id": "C000002",
                "title": "Severity effect paper",
                "year": "2023",
                "doi": "",
                "arxiv_id": "2401.12345",
                "source_db": "arxiv",
                "query_id": "Q2",
                "research_question": "",
                "method": "Experiment under variable severity.",
                "data": "",
                "main_findings": "Selectivity decreases when severity rises.",
                "limitations": "Short observation window.",
                "citable_points": ["Catalyst B performs better than baseline."],
                "topic_relevance_score": 2,
                "body_inclusion": "yes",
                "body_inclusion_reason": "relevant",
                "evidence_level": "metadata_only",
                "card_status": "completed",
                "created_at": "2026-04-03T00:00:00",
                "updated_at": "2026-04-03T00:00:00",
            },
            {
                "card_id": "CARD_C000003",
                "candidate_id": "C000003",
                "title": "Excluded paper",
                "year": "2022",
                "doi": "10.1000/excluded",
                "arxiv_id": "",
                "source_db": "crossref",
                "query_id": "Q3",
                "research_question": "",
                "method": "Unrelated method.",
                "data": "",
                "main_findings": "Excluded claim should never be included.",
                "limitations": "",
                "citable_points": [],
                "topic_relevance_score": 1,
                "body_inclusion": "no",
                "body_inclusion_reason": "excluded",
                "evidence_level": "metadata_abstract",
                "card_status": "completed",
                "created_at": "2026-04-03T00:00:00",
                "updated_at": "2026-04-03T00:00:00",
            },
        ]
        with self.cards_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def write_included(self) -> None:
        with self.included_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=reference_pipeline.INCLUDED_CANDIDATE_COLUMNS)
            writer.writeheader()
            writer.writerow(
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Pathway A mechanism paper",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                }
            )
            writer.writerow(
                {
                    "candidate_id": "C000002",
                    "card_id": "CARD_C000002",
                    "title": "Severity effect paper",
                    "year": "2023",
                    "doi": "",
                    "arxiv_id": "2401.12345",
                    "source_db": "arxiv",
                    "query_id": "Q2",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                }
            )

    def write_bib(self) -> None:
        self.bib_path.write_text(
            (
                "@article{smith2024_pathway,\n"
                "  title={Pathway A mechanism paper},\n"
                "  doi={10.1000/a},\n"
                "  year={2024}\n"
                "}\n\n"
                "@misc{lee2023_shift,\n"
                "  title={Severity effect paper},\n"
                "  eprint={2401.12345},\n"
                "  archivePrefix={arXiv},\n"
                "  year={2023}\n"
                "}\n"
            ),
            encoding="utf-8",
        )

    def run_assemble(self, overwrite: bool = False, packet_overrides_json: str = "", bib_path: str = "") -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            paragraph_plans_dir="",
            claims_jsonl="",
            cards_jsonl="",
            included_candidates_csv="",
            bib_path=bib_path,
            packet_overrides_json=packet_overrides_json,
            evidence_packets_dir="",
            overwrite=overwrite,
        )
        return reference_pipeline.assemble_evidence_packets(args)

    def latest_manifest(self) -> dict:
        runs_dir = self.root / "draft" / "runs"
        run_dirs = sorted([path for path in runs_dir.glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs)
        manifest_path = run_dirs[-1] / "manifest.json"
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def load_packet(self, section_stem: str, paragraph_id: str) -> dict:
        path = self.root / "draft" / "evidence_packets" / section_stem / f"{paragraph_id}.json"
        self.assertTrue(path.exists(), f"packet not found: {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    def packet_paths(self) -> list[Path]:
        return sorted((self.root / "draft" / "evidence_packets").glob("sec_*/*.json"))

    def test_happy_path_generates_packets_and_manifest(self) -> None:
        code = self.run_assemble()
        self.assertEqual(code, 0)
        packets = self.packet_paths()
        self.assertEqual(len(packets), 2)
        payload = json.loads(packets[0].read_text(encoding="utf-8"))
        self.assertIn("supporting_references", payload)
        self.assertIn("conflicting_evidence", payload)
        self.assertIn("strength", payload)
        self.assertIn("missing_evidence", payload)
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertEqual(manifest["outputs"]["packet_count"], 2)

    def test_scope_gate_only_include_candidates_enter_packets(self) -> None:
        self.run_assemble()
        for packet_path in self.packet_paths():
            payload = json.loads(packet_path.read_text(encoding="utf-8"))
            for row in payload.get("supporting_references", []):
                self.assertIn(row["candidate_id"], {"C000001", "C000002"})
            for row in payload.get("conflicting_evidence", []):
                self.assertIn(row["candidate_id"], {"C000001", "C000002"})

    def test_claim_binding_missing_ids_are_marked(self) -> None:
        self.run_assemble()
        packet = self.load_packet("sec_001_mechanism-pathways", "SEC001-P01")
        self.assertTrue(packet["missing_evidence"]["is_missing"])
        self.assertIn("CLM009999", packet["missing_evidence"]["missing_claim_ids"])

    def test_citation_key_prefers_bib_and_falls_back_when_missing(self) -> None:
        self.run_assemble()
        packet = self.load_packet("sec_001_mechanism-pathways", "SEC001-P01")
        support = packet["supporting_references"][0]
        self.assertEqual(support["citation_key"], "smith2024_pathway")

        fallback_bib = self.root / "draft" / "latex" / "fallback_only_a.bib"
        fallback_bib.write_text(
            (
                "@article{smith2024_pathway,\n"
                "  title={Pathway A mechanism paper},\n"
                "  doi={10.1000/a},\n"
                "  year={2024}\n"
                "}\n"
            ),
            encoding="utf-8",
        )
        self.run_assemble(overwrite=True, bib_path=str(fallback_bib))
        packet_b = self.load_packet("sec_002_comparative-boundary", "SEC002-P01")
        self.assertEqual(packet_b["supporting_references"][0]["citation_key"], "C000002")

    def test_conflict_detection_hits_opposite_direction(self) -> None:
        self.run_assemble()
        packet = self.load_packet("sec_001_mechanism-pathways", "SEC001-P01")
        conflict_claim_ids = [row["claim_id"] for row in packet["conflicting_evidence"]]
        self.assertIn("CLM000002", conflict_claim_ids)

    def test_strength_label_is_valid_and_deterministic(self) -> None:
        self.run_assemble()
        first_run = [path.read_text(encoding="utf-8") for path in self.packet_paths()]
        for content in first_run:
            payload = json.loads(content)
            self.assertIn(payload["strength"]["label"], {"strong", "medium", "weak"})
        self.run_assemble()
        second_run = [path.read_text(encoding="utf-8") for path in self.packet_paths()]
        self.assertEqual(first_run, second_run)

    def test_overrides_force_and_ignore_are_applied(self) -> None:
        overrides = {
            "ignore_claim_ids": [
                {"paragraph_id": "SEC001-P01", "claim_id": "CLM000001"},
            ],
            "force_support": [
                {
                    "paragraph_id": "SEC001-P01",
                    "claim_id": "CLM000002",
                    "candidate_id": "C000002",
                    "citation_key": "lee2023_shift",
                    "support_points": ["Manual support point."],
                }
            ],
            "force_conflict": [
                {
                    "paragraph_id": "SEC002-P01",
                    "claim_id": "CLM000001",
                    "candidate_id": "C000001",
                    "conflict_point": "Manual conflict point.",
                }
            ],
        }
        self.overrides_path.write_text(json.dumps(overrides, ensure_ascii=False, indent=2), encoding="utf-8")
        self.run_assemble(packet_overrides_json=str(self.overrides_path))

        packet = self.load_packet("sec_001_mechanism-pathways", "SEC001-P01")
        support_claim_ids = [row["claim_id"] for row in packet["supporting_references"]]
        self.assertNotIn("CLM000001", support_claim_ids)
        self.assertIn("CLM000002", support_claim_ids)

        packet_two = self.load_packet("sec_002_comparative-boundary", "SEC002-P01")
        conflict_claim_ids = [row["claim_id"] for row in packet_two["conflicting_evidence"]]
        self.assertIn("CLM000001", conflict_claim_ids)

        manifest = self.latest_manifest()
        self.assertGreater(manifest["outputs"]["overrides_applied_count"], 0)

    def test_idempotent_without_overwrite_preserves_manual_edits(self) -> None:
        self.run_assemble()
        path = self.root / "draft" / "evidence_packets" / "sec_002_comparative-boundary" / "SEC002-P01.json"
        path.write_text("MANUAL_EDIT\n", encoding="utf-8")
        self.run_assemble(overwrite=False)
        self.assertEqual(path.read_text(encoding="utf-8"), "MANUAL_EDIT\n")

    def test_guardrail_does_not_mutate_references_index_or_library(self) -> None:
        watched = [
            self.claims_path,
            self.cards_path,
            self.included_path,
        ]
        before = {path: path.read_text(encoding="utf-8") for path in watched}
        self.run_assemble()
        after = {path: path.read_text(encoding="utf-8") for path in watched}
        self.assertEqual(before, after)

        library_files = [p for p in (self.root / "references" / "library").rglob("*") if p.is_file()]
        self.assertEqual(library_files, [])


if __name__ == "__main__":
    unittest.main()
