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


class LiteratureScreeningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_screen_case"
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
            fieldnames = list(reference_pipeline.CANDIDATE_COLUMNS) + ["card_status"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(
                {
                    "candidate_id": "C000001",
                    "title": "Strong relevant paper",
                    "year": "2024",
                    "authors": "A",
                    "venue": "V1",
                    "doi": "10.1000/strong",
                    "arxiv_id": "",
                    "url": "u1",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "retrieved_at": "2026-04-03T00:00:00",
                    "dedup_key": "doi:10.1000/strong",
                    "dedup_status": "unique",
                    "screen_state": "unreviewed",
                    "card_status": "completed",
                }
            )
            writer.writerow(
                {
                    "candidate_id": "C000002",
                    "title": "Weak evidence paper",
                    "year": "2023",
                    "authors": "B",
                    "venue": "V2",
                    "doi": "10.1000/weak",
                    "arxiv_id": "",
                    "url": "u2",
                    "source_db": "openalex",
                    "query_id": "Q2",
                    "retrieved_at": "2026-04-03T00:00:00",
                    "dedup_key": "doi:10.1000/weak",
                    "dedup_status": "unique",
                    "screen_state": "unreviewed",
                    "card_status": "needs_review",
                }
            )
            writer.writerow(
                {
                    "candidate_id": "C000003",
                    "title": "Duplicate paper",
                    "year": "2023",
                    "authors": "C",
                    "venue": "V3",
                    "doi": "10.1000/dup",
                    "arxiv_id": "",
                    "url": "u3",
                    "source_db": "crossref",
                    "query_id": "Q3",
                    "retrieved_at": "2026-04-03T00:00:00",
                    "dedup_key": "doi:10.1000/dup",
                    "dedup_status": "duplicate_of:C000001",
                    "screen_state": "unreviewed",
                    "card_status": "completed",
                }
            )
        return candidates_path

    def write_cards(self) -> Path:
        cards_path = self.root / "references" / "index" / "cards.jsonl"
        rows = [
            {
                "card_id": "CARD_C000001",
                "candidate_id": "C000001",
                "title": "Strong relevant paper",
                "year": "2024",
                "doi": "10.1000/strong",
                "arxiv_id": "",
                "source_db": "crossref",
                "query_id": "Q1",
                "research_question": "RQ1",
                "method": "实验方法明确",
                "data": "有转化率数据",
                "main_findings": "发现A",
                "limitations": "L1",
                "citable_points": ["P1"],
                "topic_relevance_score": 3,
                "body_inclusion": "yes",
                "body_inclusion_reason": "B1",
                "evidence_level": "metadata_abstract",
                "card_status": "completed",
                "created_at": "2026-04-03T00:00:00",
                "updated_at": "2026-04-03T00:00:00",
            },
            {
                "card_id": "CARD_C000002",
                "candidate_id": "C000002",
                "title": "Weak evidence paper",
                "year": "2023",
                "doi": "10.1000/weak",
                "arxiv_id": "",
                "source_db": "openalex",
                "query_id": "Q2",
                "research_question": "RQ2",
                "method": "方法信息有限，建议后续补充全文确认。",
                "data": "不明确",
                "main_findings": "摘要不足",
                "limitations": "L2",
                "citable_points": [],
                "topic_relevance_score": 1,
                "body_inclusion": "maybe",
                "body_inclusion_reason": "B2",
                "evidence_level": "metadata_only",
                "card_status": "needs_review",
                "created_at": "2026-04-03T00:00:00",
                "updated_at": "2026-04-03T00:00:00",
            },
        ]
        with cards_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        return cards_path

    def load_cards(self) -> list[dict]:
        cards_path = self.root / "references" / "index" / "cards.jsonl"
        rows = []
        with cards_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def latest_manifest(self) -> dict:
        runs_dir = self.root / "draft" / "runs"
        run_dirs = sorted([path for path in runs_dir.glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs, "Expected at least one run directory")
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists(), "Expected manifest.json in latest run directory")
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def run_screening(self, allow_auto_minimal_cards: bool = False) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            candidates_csv="",
            cards_jsonl="",
            screening_decisions_csv="",
            included_candidates_csv="",
            allow_auto_minimal_cards=allow_auto_minimal_cards,
        )
        return reference_pipeline.screen_candidates(args)

    def test_screening_outputs_and_backwrite(self) -> None:
        self.write_candidates()
        self.write_cards()
        code = self.run_screening()
        self.assertEqual(code, 0)

        decisions_path = self.root / "references" / "index" / "screening_decisions.csv"
        included_path = self.root / "references" / "index" / "included_candidates.csv"
        self.assertTrue(decisions_path.exists())
        self.assertTrue(included_path.exists())

        with decisions_path.open("r", encoding="utf-8", newline="") as handle:
            decisions = list(csv.DictReader(handle))
        self.assertEqual(len(decisions), 2)
        decision_by_id = {row["candidate_id"]: row for row in decisions}
        self.assertEqual(decision_by_id["C000001"]["decision"], "include")
        self.assertEqual(decision_by_id["C000002"]["decision"], "unsure")
        self.assertEqual(decision_by_id["C000002"]["reason_code"], "R2")

        with included_path.open("r", encoding="utf-8", newline="") as handle:
            included = list(csv.DictReader(handle))
        self.assertEqual(len(included), 1)
        self.assertEqual(included[0]["candidate_id"], "C000001")

        candidates_path = self.root / "references" / "index" / "candidates.csv"
        with candidates_path.open("r", encoding="utf-8", newline="") as handle:
            candidate_rows = list(csv.DictReader(handle))
        by_id = {row["candidate_id"]: row for row in candidate_rows}
        self.assertEqual(by_id["C000001"]["screen_state"], "screened")
        self.assertEqual(by_id["C000001"]["screen_decision"], "include")
        self.assertEqual(by_id["C000002"]["screen_decision"], "unsure")
        self.assertEqual(by_id["C000003"]["screen_state"], "unreviewed")

        cards = self.load_cards()
        cards_by_id = {row["candidate_id"]: row for row in cards}
        self.assertEqual(cards_by_id["C000001"]["screen_decision"], "include")
        self.assertEqual(cards_by_id["C000002"]["screen_reason_code"], "R2")

        library_files = [p for p in (self.root / "references" / "library").rglob("*") if p.is_file()]
        self.assertEqual(library_files, [])

    def test_screening_is_idempotent(self) -> None:
        self.write_candidates()
        self.write_cards()
        self.run_screening()
        self.run_screening()

        decisions_path = self.root / "references" / "index" / "screening_decisions.csv"
        with decisions_path.open("r", encoding="utf-8", newline="") as handle:
            decisions = list(csv.DictReader(handle))
        self.assertEqual(len(decisions), 2)
        self.assertEqual(sorted([d["candidate_id"] for d in decisions]), ["C000001", "C000002"])

    def test_reason_codes_from_fixed_set_for_non_include(self) -> None:
        self.write_candidates()
        self.write_cards()
        self.run_screening()
        allowed = {"R1", "R2", "R3", "R4", "R5", "R6"}
        decisions_path = self.root / "references" / "index" / "screening_decisions.csv"
        with decisions_path.open("r", encoding="utf-8", newline="") as handle:
            decisions = list(csv.DictReader(handle))
        for row in decisions:
            if row["decision"] != "include":
                self.assertIn(row["reason_code"], allowed)

    def test_screening_fails_when_card_missing_by_default(self) -> None:
        self.write_candidates()
        self.write_cards()

        cards_path = self.root / "references" / "index" / "cards.jsonl"
        cards = self.load_cards()
        cards = [row for row in cards if row.get("candidate_id") != "C000002"]
        with cards_path.open("w", encoding="utf-8") as handle:
            for row in cards:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

        code = self.run_screening()
        self.assertEqual(code, 0)

        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["outputs"]["missing_card_count"], 1)
        self.assertEqual(manifest["outputs"]["allow_auto_minimal_cards"], False)

        decisions_path = self.root / "references" / "index" / "screening_decisions.csv"
        with decisions_path.open("r", encoding="utf-8", newline="") as handle:
            decisions = list(csv.DictReader(handle))
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0]["candidate_id"], "C000001")

        candidates_path = self.root / "references" / "index" / "candidates.csv"
        with candidates_path.open("r", encoding="utf-8", newline="") as handle:
            candidate_rows = list(csv.DictReader(handle))
        by_id = {row["candidate_id"]: row for row in candidate_rows}
        self.assertEqual(by_id["C000001"]["screen_state"], "screened")
        self.assertEqual(by_id["C000002"]["screen_state"], "unreviewed")
        self.assertEqual(by_id["C000002"]["screen_decision"], "")

    def test_screening_can_autofill_minimal_card_when_explicitly_enabled(self) -> None:
        self.write_candidates()
        self.write_cards()

        cards_path = self.root / "references" / "index" / "cards.jsonl"
        cards = self.load_cards()
        cards = [row for row in cards if row.get("candidate_id") != "C000002"]
        with cards_path.open("w", encoding="utf-8") as handle:
            for row in cards:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

        code = self.run_screening(allow_auto_minimal_cards=True)
        self.assertEqual(code, 0)

        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertEqual(manifest["outputs"]["missing_card_count"], 0)
        self.assertEqual(manifest["outputs"]["allow_auto_minimal_cards"], True)

        decisions_path = self.root / "references" / "index" / "screening_decisions.csv"
        with decisions_path.open("r", encoding="utf-8", newline="") as handle:
            decisions = list(csv.DictReader(handle))
        self.assertEqual(len(decisions), 2)
        self.assertEqual(sorted([d["candidate_id"] for d in decisions]), ["C000001", "C000002"])


if __name__ == "__main__":
    unittest.main()
