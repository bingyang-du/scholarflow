import argparse
import csv
import importlib.util
import json
import sys
import shutil
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "reference_pipeline.py"
SPEC = importlib.util.spec_from_file_location("reference_pipeline", SCRIPT_PATH)
assert SPEC and SPEC.loader
reference_pipeline = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = reference_pipeline
SPEC.loader.exec_module(reference_pipeline)


class LiteratureSearchCandidateTests(unittest.TestCase):
    def sample_topic_frame(self) -> dict:
        return {
            "topic_frame": {
                "version": "TFR-1",
                "topic": "Pentene isomerization over ZSM-5",
                "research_questions": {
                    "primary": "How does catalyst topology impact pentene isomerization?",
                    "sub_questions": ["Which reaction pathways dominate under low temperature?"],
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

    def test_build_search_queries_from_topic_frame(self) -> None:
        frame = self.sample_topic_frame()["topic_frame"]
        queries = reference_pipeline.build_search_queries(frame, max_queries=8)
        self.assertTrue(len(queries) > 0)
        self.assertTrue(any("pentene" in q.lower() for q in queries))

    def test_deduplicate_candidate_rows_priority(self) -> None:
        rows = [
            {
                "candidate_id": "",
                "title": "A Study on Pentene Isomerization",
                "year": 2020,
                "authors": "A",
                "venue": "J1",
                "doi": "10.1000/abc",
                "arxiv_id": "",
                "url": "u1",
                "source_db": "crossref",
                "query_id": "Q1",
                "retrieved_at": "2026-01-01T00:00:00",
                "dedup_key": "",
                "dedup_status": "unique",
                "screen_state": "unreviewed",
            },
            {
                "candidate_id": "",
                "title": "A Study on Pentene Isomerization",
                "year": 2020,
                "authors": "B",
                "venue": "J2",
                "doi": "10.1000/abc",
                "arxiv_id": "",
                "url": "u2",
                "source_db": "openalex",
                "query_id": "Q2",
                "retrieved_at": "2026-01-01T00:00:00",
                "dedup_key": "",
                "dedup_status": "unique",
                "screen_state": "unreviewed",
            },
        ]
        stats = reference_pipeline.deduplicate_candidate_rows(rows)
        self.assertEqual(stats["unique_count"], 1)
        self.assertEqual(stats["duplicate_count"], 1)
        self.assertEqual(rows[0]["dedup_status"], "unique")
        self.assertTrue(rows[1]["dedup_status"].startswith("duplicate_of:"))

    def test_search_candidates_backend_unavailable_still_writes_audit_files(self) -> None:
        workspace_root = Path(__file__).resolve().parents[1]
        root = workspace_root / "tests" / "_tmp_search_candidate_case"
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)
        try:
            topic_frame_path = root / "topic_frame.json"
            topic_frame_path.write_text(json.dumps(self.sample_topic_frame()), encoding="utf-8")

            args = argparse.Namespace(
                base_dir=str(root),
                topic_frame_json=str(topic_frame_path),
                max_queries=2,
                rows_per_source=3,
                backend_order="mcp",
            )
            exit_code = reference_pipeline.search_candidates(args)
            self.assertEqual(exit_code, 0)

            search_sources = root / "references" / "index" / "search_sources.csv"
            candidates = root / "references" / "index" / "candidates.csv"
            self.assertTrue(search_sources.exists())
            self.assertTrue(candidates.exists())

            with search_sources.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertTrue(len(rows) >= 1)
            self.assertTrue(all(r["status"] == "failed" for r in rows))

            with candidates.open("r", encoding="utf-8", newline="") as handle:
                candidate_rows = list(csv.DictReader(handle))
            self.assertEqual(len(candidate_rows), 0)

            library_dir = root / "references" / "library"
            if library_dir.exists():
                library_files = [p for p in library_dir.rglob("*") if p.is_file()]
                self.assertEqual(library_files, [])
        finally:
            if root.exists():
                shutil.rmtree(root)


if __name__ == "__main__":
    unittest.main()
