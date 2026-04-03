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


class LiteratureArgumentOutlineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_argument_outline_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "outline").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        self.topic_frame_path = self.root / "topic_frame.json"
        self.write_topic_frame()

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_topic_frame(self) -> None:
        payload = {
            "topic_frame": {
                "version": "TFR-1",
                "topic": "Pentene isomerization over ZSM-5",
                "research_questions": {
                    "primary": "How can pentene isomerization pathways be explained on ZSM-5?",
                    "sub_questions": [
                        "Which mechanism pathways dominate under reaction conditions?",
                        "What boundary conditions limit interpretation of observed selectivity?",
                    ],
                },
                "keywords": {
                    "core_concepts": ["pentene isomerization", "zsm-5"],
                    "domain_terms": ["zeolite catalysis", "olefin conversion"],
                    "methods_or_mechanisms": ["reaction mechanism", "kinetic modeling"],
                    "bilingual_synonyms": [],
                },
                "search_constraints": {
                    "time_range": {"enabled": True, "start_year": 2015, "end_year": 2026},
                    "language_range": {"enabled": True, "languages": ["en"]},
                    "venue_preference": {"mode": "balanced", "prioritize": ["journal", "conference"]},
                },
            }
        }
        self.topic_frame_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def write_included_candidates(self, rows: list[dict]) -> Path:
        path = self.root / "references" / "index" / "included_candidates.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=reference_pipeline.INCLUDED_CANDIDATE_COLUMNS)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return path

    def write_cards(self) -> Path:
        path = self.root / "references" / "index" / "cards.jsonl"
        rows = [
            {
                "card_id": "CARD_C000001",
                "candidate_id": "C000001",
                "title": "Pathway analysis for pentene isomerization",
                "year": "2024",
                "doi": "10.1000/a1",
                "arxiv_id": "",
                "source_db": "crossref",
                "query_id": "Q1",
                "research_question": "RQ1",
                "method": "Kinetic modeling reveals likely reaction pathway over ZSM-5.",
                "data": "Contains conversion and selectivity values.",
                "main_findings": "Under low temperature conditions, pathway A explains the selectivity trend.",
                "limitations": "Evidence remains insufficient for high pressure condition transferability.",
                "citable_points": [
                    "Compared with baseline catalyst, ZSM-5 shows better selectivity stability."
                ],
                "topic_relevance_score": 3,
                "body_inclusion": "yes",
                "body_inclusion_reason": "Strong relevance",
                "evidence_level": "metadata_abstract",
                "card_status": "completed",
                "created_at": "2026-04-03T00:00:00",
                "updated_at": "2026-04-03T00:00:00",
            },
            {
                "card_id": "CARD_C000002",
                "candidate_id": "C000002",
                "title": "Boundary conditions in zeolite catalysis",
                "year": "2023",
                "doi": "10.1000/a2",
                "arxiv_id": "",
                "source_db": "openalex",
                "query_id": "Q2",
                "research_question": "RQ2",
                "method": "Experiment focuses on catalyst deactivation behavior.",
                "data": "Partial data report.",
                "main_findings": "Observation shows selectivity drift when feed composition changes.",
                "limitations": "Limitation: dataset size is small and uncertain.",
                "citable_points": [],
                "topic_relevance_score": 2,
                "body_inclusion": "yes",
                "body_inclusion_reason": "Useful boundary evidence",
                "evidence_level": "metadata_abstract",
                "card_status": "completed",
                "created_at": "2026-04-03T00:00:00",
                "updated_at": "2026-04-03T00:00:00",
            },
            {
                "card_id": "CARD_C000003",
                "candidate_id": "C000003",
                "title": "Unrelated unsure candidate",
                "year": "2022",
                "doi": "10.1000/a3",
                "arxiv_id": "",
                "source_db": "crossref",
                "query_id": "Q3",
                "research_question": "RQ3",
                "method": "Unclear",
                "data": "Unclear",
                "main_findings": "This should not be used by outline stage.",
                "limitations": "Unclear",
                "citable_points": [],
                "topic_relevance_score": 1,
                "body_inclusion": "maybe",
                "body_inclusion_reason": "unsure",
                "evidence_level": "metadata_only",
                "card_status": "needs_review",
                "created_at": "2026-04-03T00:00:00",
                "updated_at": "2026-04-03T00:00:00",
            },
        ]
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        return path

    def latest_manifest(self) -> dict:
        runs_dir = self.root / "draft" / "runs"
        run_dirs = sorted([path for path in runs_dir.glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs, "Expected at least one run directory")
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists(), "Expected manifest.json in latest run directory")
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def load_jsonl(self, path: Path) -> list[dict]:
        rows = []
        if not path.exists():
            return rows
        with path.open("r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def run_outline(
        self,
        included_candidates_csv: str = "",
        cards_jsonl: str = "",
        argument_graph_json: str = "",
        claims_jsonl: str = "",
        outline_markdown: str = "",
    ) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            topic_frame_json=str(self.topic_frame_path),
            cards_jsonl=cards_jsonl,
            included_candidates_csv=included_candidates_csv,
            argument_graph_json=argument_graph_json,
            claims_jsonl=claims_jsonl,
            outline_markdown=outline_markdown,
        )
        return reference_pipeline.outline_from_evidence(args)

    def test_outline_happy_path_outputs_are_generated(self) -> None:
        self.write_cards()
        self.write_included_candidates(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Pathway analysis for pentene isomerization",
                    "year": "2024",
                    "doi": "10.1000/a1",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                },
                {
                    "candidate_id": "C000002",
                    "card_id": "CARD_C000002",
                    "title": "Boundary conditions in zeolite catalysis",
                    "year": "2023",
                    "doi": "10.1000/a2",
                    "arxiv_id": "",
                    "source_db": "openalex",
                    "query_id": "Q2",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                },
            ]
        )

        code = self.run_outline()
        self.assertEqual(code, 0)

        claims_path = self.root / "references" / "index" / "claims.jsonl"
        graph_path = self.root / "references" / "index" / "argument_graph.json"
        outline_path = self.root / "outline" / "generated_outline.md"
        self.assertTrue(claims_path.exists())
        self.assertTrue(graph_path.exists())
        self.assertTrue(outline_path.exists())

        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")

        claims = self.load_jsonl(claims_path)
        self.assertTrue(len(claims) > 0)
        self.assertTrue(all(claim["candidate_id"] in {"C000001", "C000002"} for claim in claims))

        graph = json.loads(graph_path.read_text(encoding="utf-8"))
        self.assertIn("section_plan", graph)
        self.assertEqual(len(graph["subquestions"]), 2)

        outline_text = outline_path.read_text(encoding="utf-8")
        self.assertIn("子问题1", outline_text)
        self.assertIn("段落要点", outline_text)

        library_files = [p for p in (self.root / "references" / "library").rglob("*") if p.is_file()]
        self.assertEqual(library_files, [])

    def test_scope_gate_only_include_candidates_enter_graph(self) -> None:
        self.write_cards()
        self.write_included_candidates(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Pathway analysis for pentene isomerization",
                    "year": "2024",
                    "doi": "10.1000/a1",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                }
            ]
        )

        self.run_outline()
        claims = self.load_jsonl(self.root / "references" / "index" / "claims.jsonl")
        candidate_ids = {row["candidate_id"] for row in claims}
        self.assertEqual(candidate_ids, {"C000001"})

    def test_missing_included_candidates_file_marks_failed(self) -> None:
        self.write_cards()
        missing_path = self.root / "references" / "index" / "missing_included.csv"
        self.run_outline(included_candidates_csv=str(missing_path))

        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertTrue(any(error["source"] == "included_candidates_csv" for error in manifest["errors"]))

    def test_missing_card_association_marks_failed(self) -> None:
        self.write_cards()
        self.write_included_candidates(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Pathway analysis for pentene isomerization",
                    "year": "2024",
                    "doi": "10.1000/a1",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                },
                {
                    "candidate_id": "C009999",
                    "card_id": "CARD_C009999",
                    "title": "Missing card row",
                    "year": "2024",
                    "doi": "",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "QX",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                },
            ]
        )

        self.run_outline()
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertTrue(any(error["source"] == "card_linking" for error in manifest["errors"]))

    def test_claim_taxonomy_is_fixed_set(self) -> None:
        self.write_cards()
        self.write_included_candidates(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Pathway analysis for pentene isomerization",
                    "year": "2024",
                    "doi": "10.1000/a1",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                },
                {
                    "candidate_id": "C000002",
                    "card_id": "CARD_C000002",
                    "title": "Boundary conditions in zeolite catalysis",
                    "year": "2023",
                    "doi": "10.1000/a2",
                    "arxiv_id": "",
                    "source_db": "openalex",
                    "query_id": "Q2",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                },
            ]
        )
        self.run_outline()
        claims = self.load_jsonl(self.root / "references" / "index" / "claims.jsonl")
        allowed = set(reference_pipeline.CLAIM_TYPES)
        for claim in claims:
            self.assertIn(claim["claim_type"], allowed)

    def test_deterministic_outputs_across_runs(self) -> None:
        self.write_cards()
        self.write_included_candidates(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Pathway analysis for pentene isomerization",
                    "year": "2024",
                    "doi": "10.1000/a1",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                },
                {
                    "candidate_id": "C000002",
                    "card_id": "CARD_C000002",
                    "title": "Boundary conditions in zeolite catalysis",
                    "year": "2023",
                    "doi": "10.1000/a2",
                    "arxiv_id": "",
                    "source_db": "openalex",
                    "query_id": "Q2",
                    "include_reason": "good",
                    "screened_at": "2026-04-03T00:00:00",
                },
            ]
        )

        self.run_outline()
        claims_path = self.root / "references" / "index" / "claims.jsonl"
        graph_path = self.root / "references" / "index" / "argument_graph.json"
        outline_path = self.root / "outline" / "generated_outline.md"
        claims_first = claims_path.read_text(encoding="utf-8")
        graph_first = graph_path.read_text(encoding="utf-8")
        outline_first = outline_path.read_text(encoding="utf-8")

        self.run_outline()
        self.assertEqual(claims_first, claims_path.read_text(encoding="utf-8"))
        self.assertEqual(graph_first, graph_path.read_text(encoding="utf-8"))
        self.assertEqual(outline_first, outline_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
