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


class LiteratureCardifyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_cardify_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_candidates(self) -> Path:
        candidates_path = self.root / "references" / "index" / "candidates.csv"
        with candidates_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=reference_pipeline.CANDIDATE_COLUMNS)
            writer.writeheader()
            writer.writerow(
                {
                    "candidate_id": "C000001",
                    "title": "Pentene isomerization on ZSM-5 catalyst",
                    "year": "2024",
                    "authors": "Alice; Bob",
                    "venue": "Catalysis Today",
                    "doi": "10.1000/demo1",
                    "arxiv_id": "",
                    "url": "https://example.org/p1",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "retrieved_at": "2026-04-03T00:00:00",
                    "dedup_key": "doi:10.1000/demo1",
                    "dedup_status": "unique",
                    "screen_state": "unreviewed",
                }
            )
            writer.writerow(
                {
                    "candidate_id": "C000002",
                    "title": "General zeolite overview",
                    "year": "2020",
                    "authors": "Carol",
                    "venue": "Journal X",
                    "doi": "10.1000/demo2",
                    "arxiv_id": "",
                    "url": "https://example.org/p2",
                    "source_db": "openalex",
                    "query_id": "Q2",
                    "retrieved_at": "2026-04-03T00:00:00",
                    "dedup_key": "doi:10.1000/demo2",
                    "dedup_status": "unique",
                    "screen_state": "unreviewed",
                }
            )
        return candidates_path

    def write_records(self) -> Path:
        records_path = self.root / "references" / "index" / "records.jsonl"
        rows = [
            {
                "doi": "10.1000/demo1",
                "title": "Pentene isomerization on ZSM-5 catalyst",
                "year": 2024,
                "venue": "Catalysis Today",
                "abstract": "This study investigates reaction pathways and reports improved selectivity for pentene isomerization over ZSM-5.",
            },
            {
                "doi": "10.1000/demo2",
                "title": "General zeolite overview",
                "year": 2020,
                "venue": "Journal X",
                "abstract": "",
            },
        ]
        with records_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        return records_path

    def load_cards(self) -> list[dict]:
        cards_path = self.root / "references" / "index" / "cards.jsonl"
        rows = []
        if not cards_path.exists():
            return rows
        with cards_path.open("r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def test_cardify_generates_cards_and_updates_status(self) -> None:
        self.write_candidates()
        self.write_records()
        args = argparse.Namespace(
            base_dir=str(self.root),
            candidates_csv="",
            records_jsonl="",
            cards_jsonl="",
            overwrite_existing=False,
        )
        code = reference_pipeline.cardify_candidates(args)
        self.assertEqual(code, 0)

        cards = self.load_cards()
        self.assertEqual(len(cards), 2)
        first, second = cards[0], cards[1]
        self.assertEqual(first["evidence_level"], "metadata_abstract")
        self.assertEqual(first["card_status"], "completed")
        self.assertIn(first["body_inclusion"], {"yes", "maybe"})
        self.assertEqual(second["evidence_level"], "metadata_only")
        self.assertEqual(second["card_status"], "needs_review")

        candidates_path = self.root / "references" / "index" / "candidates.csv"
        with candidates_path.open("r", encoding="utf-8", newline="") as handle:
            candidate_rows = list(csv.DictReader(handle))
        self.assertIn("card_status", candidate_rows[0].keys())
        self.assertEqual(candidate_rows[0]["card_status"], "completed")
        self.assertEqual(candidate_rows[1]["card_status"], "needs_review")

    def test_cardify_is_idempotent_without_overwrite(self) -> None:
        self.write_candidates()
        self.write_records()
        args = argparse.Namespace(
            base_dir=str(self.root),
            candidates_csv="",
            records_jsonl="",
            cards_jsonl="",
            overwrite_existing=False,
        )
        reference_pipeline.cardify_candidates(args)
        cards_before = self.load_cards()
        reference_pipeline.cardify_candidates(args)
        cards_after = self.load_cards()
        self.assertEqual(len(cards_before), len(cards_after))
        self.assertEqual(cards_before[0]["created_at"], cards_after[0]["created_at"])

    def test_cardify_overwrite_regenerates_content(self) -> None:
        self.write_candidates()
        self.write_records()
        args = argparse.Namespace(
            base_dir=str(self.root),
            candidates_csv="",
            records_jsonl="",
            cards_jsonl="",
            overwrite_existing=False,
        )
        reference_pipeline.cardify_candidates(args)
        cards = self.load_cards()
        cards[0]["main_findings"] = "MANUAL_OVERRIDE"
        cards_path = self.root / "references" / "index" / "cards.jsonl"
        with cards_path.open("w", encoding="utf-8") as handle:
            for row in cards:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

        overwrite_args = argparse.Namespace(
            base_dir=str(self.root),
            candidates_csv="",
            records_jsonl="",
            cards_jsonl="",
            overwrite_existing=True,
        )
        reference_pipeline.cardify_candidates(overwrite_args)
        refreshed = self.load_cards()
        self.assertNotEqual(refreshed[0]["main_findings"], "MANUAL_OVERRIDE")

        library_dir = self.root / "references" / "library"
        files = [p for p in library_dir.rglob("*") if p.is_file()]
        self.assertEqual(files, [])


if __name__ == "__main__":
    unittest.main()
