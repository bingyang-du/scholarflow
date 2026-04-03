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


class LiteratureFulltextFetchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace_root = Path(__file__).resolve().parents[1]
        self.root = self.workspace_root / "tests" / "_tmp_fulltext_fetch_case"
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

        (self.root / "references" / "index").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "library").mkdir(parents=True, exist_ok=True)
        (self.root / "references" / "inbox").mkdir(parents=True, exist_ok=True)
        (self.root / "draft" / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "logs").mkdir(parents=True, exist_ok=True)

        self._original_download = reference_pipeline.download_pdf_to_path

    def tearDown(self) -> None:
        reference_pipeline.download_pdf_to_path = self._original_download
        if self.root.exists():
            shutil.rmtree(self.root)

    def write_included(self, rows: list[dict]) -> Path:
        path = self.root / "references" / "index" / "included_candidates.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=reference_pipeline.INCLUDED_CANDIDATE_COLUMNS)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return path

    def write_cards(self, rows: list[dict]) -> Path:
        path = self.root / "references" / "index" / "cards.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        return path

    def write_records(self, rows: list[dict]) -> Path:
        path = self.root / "references" / "index" / "records.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        return path

    def latest_manifest(self) -> dict:
        runs_dir = self.root / "draft" / "runs"
        run_dirs = sorted([path for path in runs_dir.glob("run_*") if path.is_dir()])
        self.assertTrue(run_dirs, "Expected run directory")
        manifest_path = run_dirs[-1] / "manifest.json"
        self.assertTrue(manifest_path.exists(), "Expected manifest in latest run")
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def read_csv_rows(self, path: Path) -> list[dict]:
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def run_fetch(self) -> int:
        args = argparse.Namespace(
            base_dir=str(self.root),
            included_candidates_csv="",
            cards_jsonl="",
            records_jsonl="",
            download_log_csv="",
            downloaded_index_csv="",
            library_dir="",
            max_retries=1,
            timeout_seconds=10,
        )
        return reference_pipeline.fetch_fulltext(args)

    def test_happy_path_downloads_only_included_candidates(self) -> None:
        calls: list[str] = []

        def fake_download(source_url: str, target_path: Path, max_retries: int, timeout_seconds: int):
            calls.append(source_url)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(b"%PDF-1.4\n")
            return "downloaded", 200, "", 0

        reference_pipeline.download_pdf_to_path = fake_download

        self.write_included(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "ok",
                    "screened_at": "2026-04-03T00:00:00",
                }
            ]
        )
        self.write_cards(
            [
                {
                    "card_id": "CARD_C000001",
                    "candidate_id": "C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "pdf_url": "https://example.org/a.pdf",
                },
                {
                    "card_id": "CARD_C000002",
                    "candidate_id": "C000002",
                    "title": "Paper B",
                    "year": "2023",
                    "doi": "10.1000/b",
                    "pdf_url": "https://example.org/b.pdf",
                },
            ]
        )
        self.write_records([])

        code = self.run_fetch()
        self.assertEqual(code, 0)

        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "ok")
        self.assertEqual(manifest["outputs"]["attempted_count"], 1)
        self.assertEqual(manifest["outputs"]["success_count"], 1)
        self.assertEqual(len(calls), 1)
        self.assertIn("a.pdf", calls[0])

        log_rows = self.read_csv_rows(self.root / "references" / "index" / "fulltext_fetch_log.csv")
        self.assertEqual(len(log_rows), 1)
        self.assertEqual(log_rows[0]["candidate_id"], "C000001")
        self.assertEqual(log_rows[0]["status"], "downloaded")

        downloaded_rows = self.read_csv_rows(self.root / "references" / "index" / "downloaded_fulltexts.csv")
        self.assertEqual(len(downloaded_rows), 1)
        self.assertEqual(downloaded_rows[0]["candidate_id"], "C000001")

        library_files = [p for p in (self.root / "references" / "library").rglob("*.pdf")]
        self.assertEqual(len(library_files), 1)

    def test_scope_gate_ignores_non_included_cards(self) -> None:
        called_urls: list[str] = []

        def fake_download(source_url: str, target_path: Path, max_retries: int, timeout_seconds: int):
            called_urls.append(source_url)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(b"%PDF-1.4\n")
            return "downloaded", 200, "", 0

        reference_pipeline.download_pdf_to_path = fake_download

        self.write_included(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "ok",
                    "screened_at": "2026-04-03T00:00:00",
                }
            ]
        )
        self.write_cards(
            [
                {
                    "card_id": "CARD_C000001",
                    "candidate_id": "C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "pdf_url": "https://example.org/a.pdf",
                },
                {
                    "card_id": "CARD_C000009",
                    "candidate_id": "C000009",
                    "title": "Paper X",
                    "year": "2022",
                    "doi": "10.1000/x",
                    "pdf_url": "https://example.org/x.pdf",
                },
            ]
        )
        self.write_records([])

        self.run_fetch()
        self.assertEqual(called_urls, ["https://example.org/a.pdf"])
        self.assertFalse(any("x.pdf" in url for url in called_urls))

    def test_failure_continue_for_next_candidate(self) -> None:
        def fake_download(source_url: str, target_path: Path, max_retries: int, timeout_seconds: int):
            if source_url.endswith("a.pdf"):
                return "failed", 403, "forbidden", 1
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(b"%PDF-1.4\n")
            return "downloaded", 200, "", 0

        reference_pipeline.download_pdf_to_path = fake_download

        self.write_included(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "ok",
                    "screened_at": "2026-04-03T00:00:00",
                },
                {
                    "candidate_id": "C000002",
                    "card_id": "CARD_C000002",
                    "title": "Paper B",
                    "year": "2024",
                    "doi": "10.1000/b",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q2",
                    "include_reason": "ok",
                    "screened_at": "2026-04-03T00:00:00",
                },
            ]
        )
        self.write_cards(
            [
                {
                    "card_id": "CARD_C000001",
                    "candidate_id": "C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "pdf_url": "https://example.org/a.pdf",
                },
                {
                    "card_id": "CARD_C000002",
                    "candidate_id": "C000002",
                    "title": "Paper B",
                    "year": "2024",
                    "doi": "10.1000/b",
                    "pdf_url": "https://example.org/b.pdf",
                },
            ]
        )
        self.write_records([])

        self.run_fetch()
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["outputs"]["attempted_count"], 2)
        self.assertEqual(manifest["outputs"]["failed_count"], 1)
        self.assertEqual(manifest["outputs"]["success_count"], 1)

        log_rows = self.read_csv_rows(self.root / "references" / "index" / "fulltext_fetch_log.csv")
        status_by_candidate = {row["candidate_id"]: row["status"] for row in log_rows}
        self.assertEqual(status_by_candidate["C000001"], "failed")
        self.assertEqual(status_by_candidate["C000002"], "downloaded")

    def test_no_url_is_recorded_and_batch_continues(self) -> None:
        def fake_download(source_url: str, target_path: Path, max_retries: int, timeout_seconds: int):
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(b"%PDF-1.4\n")
            return "downloaded", 200, "", 0

        reference_pipeline.download_pdf_to_path = fake_download

        self.write_included(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "ok",
                    "screened_at": "2026-04-03T00:00:00",
                },
                {
                    "candidate_id": "C000003",
                    "card_id": "CARD_C000003",
                    "title": "Paper C",
                    "year": "2024",
                    "doi": "",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q3",
                    "include_reason": "ok",
                    "screened_at": "2026-04-03T00:00:00",
                },
            ]
        )
        self.write_cards(
            [
                {
                    "card_id": "CARD_C000001",
                    "candidate_id": "C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "pdf_url": "https://example.org/a.pdf",
                },
                {
                    "card_id": "CARD_C000003",
                    "candidate_id": "C000003",
                    "title": "Paper C",
                    "year": "2024",
                    "doi": "",
                },
            ]
        )
        self.write_records([])

        self.run_fetch()
        manifest = self.latest_manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["outputs"]["no_url_count"], 1)
        self.assertEqual(manifest["outputs"]["success_count"], 1)

        log_rows = self.read_csv_rows(self.root / "references" / "index" / "fulltext_fetch_log.csv")
        by_id = {row["candidate_id"]: row for row in log_rows}
        self.assertEqual(by_id["C000003"]["status"], "no_url")

    def test_idempotency_marks_exists_on_second_run(self) -> None:
        calls = {"count": 0}

        def fake_download(source_url: str, target_path: Path, max_retries: int, timeout_seconds: int):
            calls["count"] += 1
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(b"%PDF-1.4\n")
            return "downloaded", 200, "", 0

        reference_pipeline.download_pdf_to_path = fake_download

        self.write_included(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "ok",
                    "screened_at": "2026-04-03T00:00:00",
                }
            ]
        )
        self.write_cards(
            [
                {
                    "card_id": "CARD_C000001",
                    "candidate_id": "C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "pdf_url": "https://example.org/a.pdf",
                }
            ]
        )
        self.write_records([])

        self.run_fetch()
        self.run_fetch()
        self.assertEqual(calls["count"], 1)

        log_rows = self.read_csv_rows(self.root / "references" / "index" / "fulltext_fetch_log.csv")
        self.assertEqual(len(log_rows), 1)
        self.assertEqual(log_rows[0]["status"], "exists")

    def test_guardrail_only_writes_expected_paths(self) -> None:
        def fake_download(source_url: str, target_path: Path, max_retries: int, timeout_seconds: int):
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(b"%PDF-1.4\n")
            return "downloaded", 200, "", 0

        reference_pipeline.download_pdf_to_path = fake_download

        included_path = self.write_included(
            [
                {
                    "candidate_id": "C000001",
                    "card_id": "CARD_C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "arxiv_id": "",
                    "source_db": "crossref",
                    "query_id": "Q1",
                    "include_reason": "ok",
                    "screened_at": "2026-04-03T00:00:00",
                }
            ]
        )
        included_before = included_path.read_text(encoding="utf-8")

        self.write_cards(
            [
                {
                    "card_id": "CARD_C000001",
                    "candidate_id": "C000001",
                    "title": "Paper A",
                    "year": "2024",
                    "doi": "10.1000/a",
                    "pdf_url": "https://example.org/a.pdf",
                }
            ]
        )
        self.write_records([])
        self.run_fetch()

        self.assertEqual(included_before, included_path.read_text(encoding="utf-8"))
        self.assertTrue((self.root / "references" / "index" / "fulltext_fetch_log.csv").exists())
        self.assertTrue((self.root / "references" / "index" / "downloaded_fulltexts.csv").exists())
        self.assertEqual(
            [p for p in (self.root / "references" / "inbox").rglob("*") if p.is_file()],
            [],
        )


if __name__ == "__main__":
    unittest.main()
