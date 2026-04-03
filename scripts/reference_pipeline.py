#!/usr/bin/env python
"""Reference enrichment pipeline for DOI/arXiv + JSONL/BibTeX persistence."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import difflib
import json
import re
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as et
from dataclasses import dataclass
from pathlib import Path
from typing import Any

TITLE_SIMILARITY_THRESHOLD = 0.93
REQUEST_TIMEOUT_SECONDS = 20
DEFAULT_BACKEND_ORDER = ("openalex", "crossref", "arxiv", "mcp")

SEARCH_SOURCE_COLUMNS = [
    "query_id",
    "source_db",
    "query_string",
    "filters",
    "time_range",
    "language_range",
    "venue_preference",
    "retrieved_at",
    "result_count",
    "status",
    "error",
]

CANDIDATE_COLUMNS = [
    "candidate_id",
    "title",
    "year",
    "authors",
    "venue",
    "doi",
    "arxiv_id",
    "url",
    "source_db",
    "query_id",
    "retrieved_at",
    "dedup_key",
    "dedup_status",
    "screen_state",
]

SCREENING_DECISION_COLUMNS = [
    "candidate_id",
    "card_id",
    "decision",
    "reason_code",
    "reason_note",
    "reviewer_mode",
    "screened_at",
    "topic_relevance_score",
    "body_inclusion",
    "evidence_level",
]

INCLUDED_CANDIDATE_COLUMNS = [
    "candidate_id",
    "card_id",
    "title",
    "year",
    "doi",
    "arxiv_id",
    "source_db",
    "query_id",
    "include_reason",
    "screened_at",
]

CARD_FIELDS = [
    "card_id",
    "candidate_id",
    "title",
    "year",
    "doi",
    "arxiv_id",
    "source_db",
    "query_id",
    "research_question",
    "method",
    "data",
    "main_findings",
    "limitations",
    "citable_points",
    "topic_relevance_score",
    "body_inclusion",
    "body_inclusion_reason",
    "evidence_level",
    "card_status",
    "created_at",
    "updated_at",
]

CLAIM_TYPES = ("mechanism", "observation", "condition", "comparison", "limitation")
CLAIM_TYPE_RANK = {name: index for index, name in enumerate(CLAIM_TYPES)}
CLAIM_SOURCE_PRIORITY = {
    "main_findings": 0,
    "citable_points": 1,
    "method": 2,
    "limitations": 3,
}
TERM_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "were",
    "was",
    "are",
    "into",
    "over",
    "under",
    "using",
    "use",
    "via",
    "between",
    "among",
    "their",
    "our",
    "than",
    "then",
}


@dataclass(frozen=True)
class RepositoryPaths:
    root: Path
    references_library: Path
    records_jsonl: Path
    search_results_jsonl: Path
    search_sources_csv: Path
    candidates_csv: Path
    cards_jsonl: Path
    screening_decisions_csv: Path
    included_candidates_csv: Path
    refs_bib: Path
    runs_dir: Path
    logs_dir: Path


def resolve_paths(base_dir: Path | None = None) -> RepositoryPaths:
    root = base_dir if base_dir else Path(__file__).resolve().parents[1]
    return RepositoryPaths(
        root=root,
        references_library=root / "references" / "library",
        records_jsonl=root / "references" / "index" / "records.jsonl",
        search_results_jsonl=root / "references" / "index" / "search_results.jsonl",
        search_sources_csv=root / "references" / "index" / "search_sources.csv",
        candidates_csv=root / "references" / "index" / "candidates.csv",
        cards_jsonl=root / "references" / "index" / "cards.jsonl",
        screening_decisions_csv=root / "references" / "index" / "screening_decisions.csv",
        included_candidates_csv=root / "references" / "index" / "included_candidates.csv",
        refs_bib=root / "draft" / "latex" / "refs.bib",
        runs_dir=root / "draft" / "runs",
        logs_dir=root / "logs",
    )


def ensure_workspace(paths: RepositoryPaths) -> None:
    for directory in (
        paths.references_library,
        paths.records_jsonl.parent,
        paths.refs_bib.parent,
        paths.runs_dir,
        paths.logs_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    if not paths.records_jsonl.exists():
        paths.records_jsonl.touch()

    if not paths.search_results_jsonl.exists():
        paths.search_results_jsonl.touch()

    ensure_csv_with_headers(paths.search_sources_csv, SEARCH_SOURCE_COLUMNS)
    ensure_csv_with_headers(paths.candidates_csv, CANDIDATE_COLUMNS)
    ensure_csv_with_headers(paths.screening_decisions_csv, SCREENING_DECISION_COLUMNS)
    ensure_csv_with_headers(paths.included_candidates_csv, INCLUDED_CANDIDATE_COLUMNS)
    if not paths.cards_jsonl.exists():
        paths.cards_jsonl.touch()

    if not paths.refs_bib.exists():
        paths.refs_bib.write_text("", encoding="utf-8")


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def local_now() -> dt.datetime:
    return dt.datetime.now()


def append_log(paths: RepositoryPaths, level: str, message: str, payload: dict[str, Any]) -> None:
    stamp = local_now()
    line = (
        f"{stamp.isoformat(timespec='seconds')} [{level}] {message} "
        f"{json.dumps(payload, ensure_ascii=False)}\n"
    )
    log_file = paths.logs_dir / f"pipeline_{stamp.strftime('%Y%m%d')}.log"
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(line)


def ensure_csv_with_headers(path: Path, fieldnames: list[str]) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()


def append_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    ensure_csv_with_headers(path, fieldnames)
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        for row in rows:
            writer.writerow(row)


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists() or path.stat().st_size == 0:
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at {path}:{line_no}") from exc
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def fetch_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url=url,
        headers={
            "Accept": "application/json",
            "User-Agent": "codex-reference-pipeline/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw)


def fetch_text(url: str) -> str:
    request = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "codex-reference-pipeline/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        return response.read().decode("utf-8")


def compact_whitespace(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def strip_html(value: str) -> str:
    return compact_whitespace(re.sub(r"<[^>]+>", " ", value))


def normalize_identifier(value: str | None) -> str:
    return compact_whitespace(value).lower().strip()


def normalize_title(value: str | None) -> str:
    normalized = compact_whitespace(value).lower()
    return re.sub(r"[^a-z0-9]+", "", normalized)


def parse_year(text_value: str | None) -> int | None:
    if not text_value:
        return None
    match = re.search(r"(19|20)\d{2}", text_value)
    if not match:
        return None
    return int(match.group(0))


def parse_crossref_authors(author_entries: list[dict[str, Any]]) -> list[str]:
    authors: list[str] = []
    for author in author_entries:
        family = compact_whitespace(author.get("family"))
        given = compact_whitespace(author.get("given"))
        if family and given:
            authors.append(f"{family}, {given}")
        elif family:
            authors.append(family)
        elif given:
            authors.append(given)
    return authors


def fetch_from_doi(doi: str) -> dict[str, Any]:
    url = f"https://api.crossref.org/works/{urllib.parse.quote(doi)}"
    payload = fetch_json(url)
    message = payload.get("message", {})

    titles = message.get("title", [])
    title = compact_whitespace(titles[0] if titles else "")

    year = None
    date_parts = message.get("issued", {}).get("date-parts", [])
    if date_parts and isinstance(date_parts[0], list) and date_parts[0]:
        if isinstance(date_parts[0][0], int):
            year = int(date_parts[0][0])

    venue_candidates = message.get("container-title", [])
    venue = compact_whitespace(venue_candidates[0] if venue_candidates else "")

    abstract = strip_html(message.get("abstract", ""))
    url_value = compact_whitespace(message.get("URL", ""))
    pdf_url = ""

    for entry in message.get("link", []) or []:
        content_type = normalize_identifier(entry.get("content-type"))
        if "pdf" in content_type:
            pdf_url = compact_whitespace(entry.get("URL"))
            break

    if not pdf_url:
        pdf_url = url_value

    return {
        "doi": compact_whitespace(message.get("DOI") or doi),
        "title": title,
        "authors": parse_crossref_authors(message.get("author", [])),
        "year": year,
        "venue": venue,
        "abstract": abstract,
        "pdf_url": pdf_url,
        "source": "doi",
    }


def fetch_from_arxiv(arxiv_id: str) -> dict[str, Any]:
    url = f"http://export.arxiv.org/api/query?id_list={urllib.parse.quote(arxiv_id)}"
    xml_text = fetch_text(url)
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "arxiv": "http://arxiv.org/schemas/atom",
    }
    root = et.fromstring(xml_text)
    entry = root.find("atom:entry", ns)
    if entry is None:
        raise ValueError(f"arXiv entry not found: {arxiv_id}")

    title = compact_whitespace(entry.findtext("atom:title", default="", namespaces=ns))
    abstract = compact_whitespace(entry.findtext("atom:summary", default="", namespaces=ns))
    published = compact_whitespace(entry.findtext("atom:published", default="", namespaces=ns))
    year = parse_year(published)

    authors = []
    for author_node in entry.findall("atom:author", ns):
        name = compact_whitespace(author_node.findtext("atom:name", default="", namespaces=ns))
        if name:
            authors.append(name)

    doi = compact_whitespace(entry.findtext("arxiv:doi", default="", namespaces=ns))
    entry_id = compact_whitespace(entry.findtext("atom:id", default="", namespaces=ns))
    pdf_url = ""
    for link in entry.findall("atom:link", ns):
        href = compact_whitespace(link.attrib.get("href"))
        title_attr = compact_whitespace(link.attrib.get("title"))
        if title_attr.lower() == "pdf" or href.endswith(".pdf"):
            pdf_url = href
            break
    if not pdf_url and entry_id:
        pdf_url = entry_id.replace("/abs/", "/pdf/") + ".pdf"

    resolved_arxiv_id = arxiv_id.strip()
    id_match = re.search(r"arxiv\.org/abs/([^v]+(?:v\d+)?)", entry_id)
    if id_match:
        resolved_arxiv_id = id_match.group(1)

    record = {
        "arxiv_id": resolved_arxiv_id,
        "title": title,
        "authors": authors,
        "year": year,
        "venue": "arXiv",
        "abstract": abstract,
        "pdf_url": pdf_url,
        "source": "arxiv",
    }
    if doi:
        record["doi"] = doi
    return record


def fetch_keyword_results(query: str, rows: int) -> list[dict[str, Any]]:
    url = (
        "https://api.crossref.org/works?"
        f"query.bibliographic={urllib.parse.quote(query)}&rows={int(rows)}"
    )
    payload = fetch_json(url)
    items = payload.get("message", {}).get("items", []) or []
    results: list[dict[str, Any]] = []
    for item in items:
        title_entries = item.get("title", []) or []
        venue_entries = item.get("container-title", []) or []
        year = None
        date_parts = item.get("issued", {}).get("date-parts", [])
        if date_parts and isinstance(date_parts[0], list) and date_parts[0]:
            if isinstance(date_parts[0][0], int):
                year = int(date_parts[0][0])

        results.append(
            {
                "query": query,
                "doi": compact_whitespace(item.get("DOI")),
                "title": compact_whitespace(title_entries[0] if title_entries else ""),
                "year": year,
                "venue": compact_whitespace(venue_entries[0] if venue_entries else ""),
                "url": compact_whitespace(item.get("URL")),
                "source": "keyword_search",
            }
        )
    return results


def parse_backend_order(raw_value: str | None) -> list[str]:
    if not raw_value:
        return list(DEFAULT_BACKEND_ORDER)
    backend_names = []
    for token in raw_value.split(","):
        name = compact_whitespace(token).lower()
        if name:
            backend_names.append(name)
    return backend_names or list(DEFAULT_BACKEND_ORDER)


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def to_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [compact_whitespace(str(item)) for item in value if compact_whitespace(str(item))]
    if isinstance(value, str):
        compact = compact_whitespace(value)
        return [compact] if compact else []
    return []


def ensure_topic_frame(payload: dict[str, Any]) -> dict[str, Any]:
    if "topic_frame" in payload and isinstance(payload["topic_frame"], dict):
        return payload["topic_frame"]
    return payload


def load_topic_frame_file(path: Path) -> dict[str, Any]:
    raw_text = path.read_text(encoding="utf-8")
    data: dict[str, Any] | None = None
    try:
        decoded = json.loads(raw_text)
        if isinstance(decoded, dict):
            data = decoded
    except json.JSONDecodeError:
        data = None

    if data is None:
        try:
            import yaml  # type: ignore
        except Exception as exc:  # pragma: no cover - fallback branch
            raise ValueError(
                "Topic frame must be valid JSON, or install PyYAML for YAML input."
            ) from exc
        decoded = yaml.safe_load(raw_text)  # type: ignore[attr-defined]
        if not isinstance(decoded, dict):
            raise ValueError("Topic frame input must decode into an object.")
        data = decoded

    frame = ensure_topic_frame(data)
    validate_topic_frame(frame)
    return frame


def validate_topic_frame(frame: dict[str, Any]) -> None:
    required_fields = ["topic", "research_questions", "keywords", "search_constraints"]
    missing = [name for name in required_fields if name not in frame]
    if missing:
        raise ValueError(f"Missing required topic_frame fields: {', '.join(missing)}")

    if not isinstance(frame.get("research_questions"), dict):
        raise ValueError("topic_frame.research_questions must be an object.")
    if not isinstance(frame.get("keywords"), dict):
        raise ValueError("topic_frame.keywords must be an object.")
    if not isinstance(frame.get("search_constraints"), dict):
        raise ValueError("topic_frame.search_constraints must be an object.")


def merge_unique_queries(base: list[str]) -> list[str]:
    seen: set[str] = set()
    queries: list[str] = []
    for query in base:
        compact = compact_whitespace(query)
        if not compact:
            continue
        lowered = compact.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        queries.append(compact)
    return queries


def build_search_queries(frame: dict[str, Any], max_queries: int) -> list[str]:
    keywords = frame.get("keywords", {})
    core = to_text_list(keywords.get("core_concepts"))
    domain = to_text_list(keywords.get("domain_terms"))
    methods = to_text_list(keywords.get("methods_or_mechanisms"))
    bilingual = to_text_list(keywords.get("bilingual_synonyms"))
    topic_text = compact_whitespace(str(frame.get("topic", "")))

    drafted: list[str] = []

    for left in core[:6]:
        for right in domain[:6]:
            if left.lower() == right.lower():
                continue
            drafted.append(f"\"{left}\" AND \"{right}\"")

    for left in core[:5]:
        for right in methods[:5]:
            if left.lower() == right.lower():
                continue
            drafted.append(f"\"{left}\" AND \"{right}\"")

    for synonym in bilingual[:8]:
        if topic_text:
            drafted.append(f"\"{topic_text}\" AND \"{synonym}\"")
        else:
            drafted.append(f"\"{synonym}\"")

    if topic_text:
        drafted.append(f"\"{topic_text}\"")

    unique_queries = merge_unique_queries(drafted)
    if max_queries <= 0:
        return unique_queries
    return unique_queries[:max_queries]


def extract_year_from_openalex(work: dict[str, Any]) -> int | None:
    publication_year = work.get("publication_year")
    if isinstance(publication_year, int):
        return publication_year
    publication_date = compact_whitespace(work.get("publication_date"))
    return parse_year(publication_date)


def fetch_openalex_results(query: str, rows: int, constraints: dict[str, Any]) -> list[dict[str, Any]]:
    params: list[str] = [
        f"search={urllib.parse.quote(query)}",
        f"per-page={max(1, int(rows))}",
    ]
    time_range = constraints.get("time_range", {})
    if isinstance(time_range, dict) and time_range.get("enabled"):
        start_year = time_range.get("start_year")
        end_year = time_range.get("end_year")
        if isinstance(start_year, int) and isinstance(end_year, int):
            params.append(f"filter=from_publication_date:{start_year}-01-01,to_publication_date:{end_year}-12-31")
    url = "https://api.openalex.org/works?" + "&".join(params)

    payload = fetch_json(url)
    works = payload.get("results", []) or []
    normalized: list[dict[str, Any]] = []
    for work in works:
        title = compact_whitespace(work.get("title"))
        if not title:
            continue

        authors = []
        for authorship in work.get("authorships", []) or []:
            author_name = compact_whitespace((authorship or {}).get("author", {}).get("display_name"))
            if author_name:
                authors.append(author_name)

        venue = compact_whitespace((work.get("primary_location") or {}).get("source", {}).get("display_name"))
        doi = compact_whitespace(work.get("doi"))
        if doi.startswith("https://doi.org/"):
            doi = doi.replace("https://doi.org/", "", 1)
        arxiv_id = ""
        primary_loc = work.get("primary_location") or {}
        landing_page = compact_whitespace(primary_loc.get("landing_page_url"))
        if "arxiv.org/abs/" in landing_page:
            arxiv_id = landing_page.rsplit("/", 1)[-1]

        normalized.append(
            {
                "title": title,
                "year": extract_year_from_openalex(work),
                "authors": authors,
                "venue": venue,
                "doi": doi,
                "arxiv_id": arxiv_id,
                "url": compact_whitespace(work.get("id")) or landing_page,
            }
        )
    return normalized


def fetch_crossref_candidates(query: str, rows: int) -> list[dict[str, Any]]:
    raw_rows = fetch_keyword_results(query, rows)
    normalized: list[dict[str, Any]] = []
    for row in raw_rows:
        normalized.append(
            {
                "title": compact_whitespace(row.get("title")),
                "year": row.get("year"),
                "authors": [],
                "venue": compact_whitespace(row.get("venue")),
                "doi": compact_whitespace(row.get("doi")),
                "arxiv_id": "",
                "url": compact_whitespace(row.get("url")),
            }
        )
    return normalized


def fetch_arxiv_candidates(query: str, rows: int) -> list[dict[str, Any]]:
    url = (
        "http://export.arxiv.org/api/query?"
        f"search_query=all:{urllib.parse.quote(query)}&start=0&max_results={max(1, int(rows))}"
    )
    xml_text = fetch_text(url)
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
    }
    root = et.fromstring(xml_text)
    normalized: list[dict[str, Any]] = []
    for entry in root.findall("atom:entry", ns):
        title = compact_whitespace(entry.findtext("atom:title", default="", namespaces=ns))
        if not title:
            continue
        entry_id = compact_whitespace(entry.findtext("atom:id", default="", namespaces=ns))
        published = compact_whitespace(entry.findtext("atom:published", default="", namespaces=ns))
        year = parse_year(published)
        authors = []
        for author in entry.findall("atom:author", ns):
            name = compact_whitespace(author.findtext("atom:name", default="", namespaces=ns))
            if name:
                authors.append(name)
        arxiv_id = ""
        if entry_id and "arxiv.org/abs/" in entry_id:
            arxiv_id = entry_id.rsplit("/", 1)[-1]

        normalized.append(
            {
                "title": title,
                "year": year,
                "authors": authors,
                "venue": "arXiv",
                "doi": "",
                "arxiv_id": arxiv_id,
                "url": entry_id,
            }
        )
    return normalized


def search_backend(
    backend: str,
    query: str,
    rows: int,
    constraints: dict[str, Any],
) -> list[dict[str, Any]]:
    if backend == "openalex":
        return fetch_openalex_results(query, rows, constraints)
    if backend == "crossref":
        return fetch_crossref_candidates(query, rows)
    if backend == "arxiv":
        return fetch_arxiv_candidates(query, rows)
    if backend == "mcp":
        raise RuntimeError("MCP backend is not configured for academic search in this workspace.")
    raise ValueError(f"Unsupported backend: {backend}")


def stringify_authors(authors: Any) -> str:
    if isinstance(authors, list):
        return "; ".join([compact_whitespace(str(item)) for item in authors if compact_whitespace(str(item))])
    return compact_whitespace(str(authors))


def normalize_candidate_row(
    raw_candidate: dict[str, Any],
    source_db: str,
    query_id: str,
    retrieved_at: str,
) -> dict[str, Any]:
    title = compact_whitespace(raw_candidate.get("title"))
    year_raw = raw_candidate.get("year")
    year: int | str = ""
    if isinstance(year_raw, int):
        year = year_raw
    elif isinstance(year_raw, str):
        parsed_year = parse_year(year_raw)
        year = parsed_year if parsed_year is not None else ""

    doi_value = compact_whitespace(raw_candidate.get("doi"))
    if doi_value.startswith("https://doi.org/"):
        doi_value = doi_value.replace("https://doi.org/", "", 1)

    arxiv_id = compact_whitespace(raw_candidate.get("arxiv_id"))

    return {
        "candidate_id": "",
        "title": title,
        "year": year,
        "authors": stringify_authors(raw_candidate.get("authors")),
        "venue": compact_whitespace(raw_candidate.get("venue")),
        "doi": doi_value,
        "arxiv_id": arxiv_id,
        "url": compact_whitespace(raw_candidate.get("url")),
        "source_db": source_db,
        "query_id": query_id,
        "retrieved_at": retrieved_at,
        "dedup_key": "",
        "dedup_status": "unique",
        "screen_state": "unreviewed",
    }


def candidate_dedup_key(candidate: dict[str, Any]) -> str:
    doi = normalize_identifier(candidate.get("doi"))
    if doi:
        return f"doi:{doi}"

    arxiv_id = normalize_identifier(candidate.get("arxiv_id"))
    if arxiv_id:
        return f"arxiv:{arxiv_id}"

    normalized_title = normalize_title(candidate.get("title"))
    year = str(candidate.get("year") or "")
    if normalized_title and year:
        return f"titleyear:{normalized_title}:{year}"

    fallback = normalize_title(candidate.get("title")) + ":" + normalize_identifier(candidate.get("url"))
    return f"fallback:{fallback or 'unknown'}"


def deduplicate_candidate_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    canonical_by_key: dict[str, str] = {}
    unique_count = 0
    duplicate_count = 0
    for index, row in enumerate(rows, start=1):
        candidate_id = f"C{index:06d}"
        row["candidate_id"] = candidate_id
        dedup_key = candidate_dedup_key(row)
        row["dedup_key"] = dedup_key
        if dedup_key in canonical_by_key:
            row["dedup_status"] = f"duplicate_of:{canonical_by_key[dedup_key]}"
            duplicate_count += 1
        else:
            row["dedup_status"] = "unique"
            canonical_by_key[dedup_key] = candidate_id
            unique_count += 1
    return {
        "total_count": len(rows),
        "unique_count": unique_count,
        "duplicate_count": duplicate_count,
    }


def load_records(records_jsonl: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not records_jsonl.exists():
        return records

    with records_jsonl.open("r", encoding="utf-8") as handle:
        for index, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                if isinstance(payload, dict):
                    records.append(payload)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at line {index}: {exc}") from exc
    return records


def index_records(records: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    by_doi: dict[str, dict[str, Any]] = {}
    by_arxiv: dict[str, dict[str, Any]] = {}
    by_title_year: dict[str, dict[str, Any]] = {}
    for record in records:
        doi = normalize_identifier(record.get("doi"))
        if doi and doi not in by_doi:
            by_doi[doi] = record
        arxiv_id = normalize_identifier(record.get("arxiv_id"))
        if arxiv_id and arxiv_id not in by_arxiv:
            by_arxiv[arxiv_id] = record
        title = normalize_title(record.get("title"))
        year = str(record.get("year") or "")
        if title and year:
            key = f"{title}:{year}"
            if key not in by_title_year:
                by_title_year[key] = record
    return {
        "doi": by_doi,
        "arxiv": by_arxiv,
        "title_year": by_title_year,
    }


def matched_record_for_candidate(
    candidate: dict[str, Any],
    indices: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, Any] | None:
    doi = normalize_identifier(candidate.get("doi"))
    if doi and doi in indices["doi"]:
        return indices["doi"][doi]
    arxiv_id = normalize_identifier(candidate.get("arxiv_id"))
    if arxiv_id and arxiv_id in indices["arxiv"]:
        return indices["arxiv"][arxiv_id]
    title = normalize_title(candidate.get("title"))
    year = str(candidate.get("year") or "")
    if title and year:
        key = f"{title}:{year}"
        if key in indices["title_year"]:
            return indices["title_year"][key]
    return None


def first_sentence(text: str) -> str:
    compact = compact_whitespace(text)
    if not compact:
        return ""
    match = re.split(r"(?<=[.!?。；;])\s+", compact, maxsplit=1)
    return compact_whitespace(match[0])


def tokenize_terms(text: str) -> list[str]:
    tokens = [token.lower() for token in re.findall(r"[a-zA-Z0-9]+", text)]
    normalized: list[str] = []
    for token in tokens:
        if len(token) < 3 or token in TERM_STOPWORDS:
            continue
        normalized.append(token)
    return normalized


def deduplicate_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def build_subquestion_rows(topic_frame: dict[str, Any]) -> tuple[str, list[dict[str, Any]], list[set[str]]]:
    research_questions = topic_frame.get("research_questions", {})
    primary = compact_whitespace(research_questions.get("primary"))
    sub_questions = to_text_list(research_questions.get("sub_questions"))
    if not sub_questions:
        if primary:
            sub_questions = [primary]
        else:
            sub_questions = ["Unspecified sub-question"]

    rows: list[dict[str, Any]] = []
    token_sets: list[set[str]] = []
    for index, sub_question in enumerate(sub_questions, start=1):
        subquestion_id = f"SQ{index:03d}"
        rows.append({"subquestion_id": subquestion_id, "text": sub_question})
        token_sets.append(set(tokenize_terms(f"{sub_question} {primary}")))
    return primary, rows, token_sets


def flatten_topic_terms(topic_frame: dict[str, Any]) -> set[str]:
    keywords = topic_frame.get("keywords", {})
    terms: list[str] = [compact_whitespace(topic_frame.get("topic"))]
    for key in ("core_concepts", "domain_terms", "methods_or_mechanisms", "bilingual_synonyms"):
        terms.extend(to_text_list(keywords.get(key)))
    token_pool: list[str] = []
    for term in terms:
        token_pool.extend(tokenize_terms(term))
    return set(token_pool)


def classify_claim_type(source_field: str, claim_text: str) -> str:
    normalized_field = compact_whitespace(source_field).lower()
    lowered = claim_text.lower()
    if normalized_field == "limitations":
        return "limitation"

    comparison_tokens = ("compared", "versus", "vs", "higher", "lower", "better", "worse")
    if any(token in lowered for token in comparison_tokens):
        return "comparison"

    condition_tokens = ("under", "at ", "when", "if ", "temperature", "pressure", "condition")
    if any(token in lowered for token in condition_tokens):
        return "condition"

    mechanism_tokens = (
        "mechanism",
        "pathway",
        "kinetic",
        "catalyst",
        "active site",
        "isomerization",
        "reaction",
    )
    if normalized_field == "method" or any(token in lowered for token in mechanism_tokens):
        return "mechanism"

    limitation_tokens = ("limitation", "uncertain", "insufficient", "not enough")
    if any(token in lowered for token in limitation_tokens):
        return "limitation"
    return "observation"


def claim_confidence(source_field: str, evidence_level: str) -> float:
    base_map = {
        "main_findings": 0.82,
        "citable_points": 0.78,
        "method": 0.74,
        "limitations": 0.70,
    }
    base = base_map.get(source_field, 0.70)
    normalized_evidence = compact_whitespace(evidence_level)
    if normalized_evidence == "metadata_abstract":
        base += 0.08
    elif normalized_evidence == "metadata_only":
        base -= 0.08
    return max(0.0, min(1.0, round(base, 2)))


def map_claim_to_subquestion(claim_text: str, subquestion_rows: list[dict[str, Any]], token_sets: list[set[str]]) -> str:
    if not subquestion_rows:
        return "SQ001"
    claim_tokens = set(tokenize_terms(claim_text))
    best_index = 0
    best_score = -1
    for index, token_set in enumerate(token_sets):
        overlap = len(claim_tokens.intersection(token_set))
        if overlap > best_score:
            best_index = index
            best_score = overlap
    return compact_whitespace(subquestion_rows[best_index].get("subquestion_id")) or "SQ001"


def build_claim_sources(card: dict[str, Any]) -> list[tuple[str, str]]:
    sources: list[tuple[str, str]] = []
    for field_name in ("main_findings", "method", "limitations"):
        value = compact_whitespace(card.get(field_name))
        if value:
            sources.append((field_name, value))

    citable_points = card.get("citable_points")
    if isinstance(citable_points, list):
        for point in citable_points:
            compact = compact_whitespace(str(point))
            if compact:
                sources.append(("citable_points", compact))
    elif isinstance(citable_points, str):
        compact = compact_whitespace(citable_points)
        if compact:
            sources.append(("citable_points", compact))
    return sources


def infer_method_text(title: str, abstract: str) -> str:
    blob = f"{title} {abstract}".lower()
    if any(token in blob for token in ["kinetic", "kinetics", "microkinetic", "model"]):
        return "以动力学或模型分析为主。"
    if any(token in blob for token in ["dft", "simulation", "theory", "computational"]):
        return "以理论计算或模拟方法为主。"
    if any(token in blob for token in ["experiment", "catalyst", "reaction", "test"]):
        return "以实验催化反应评估为主。"
    return "方法信息有限，建议后续补充全文确认。"


def infer_data_text(abstract: str) -> str:
    blob = abstract.lower()
    if any(token in blob for token in ["dataset", "data", "measurement", "yield", "conversion", "selectivity"]):
        return "包含实验或统计指标数据（需结合原文核实细节）。"
    return "摘要未明确给出数据细节。"


def infer_limitations_text(abstract: str) -> str:
    blob = abstract.lower()
    if any(token in blob for token in ["limited", "limitation", "only", "preliminary"]):
        return "作者在摘要中提示了范围限制或初步性结论。"
    return "摘要未明确讨论局限性，建议阅读全文补充。"


def build_citable_points(title: str, abstract: str, venue: str, year: str) -> list[str]:
    points: list[str] = []
    title_compact = compact_whitespace(title)
    if title_compact:
        points.append(f"{year} 年 {venue} 文献《{title_compact}》与主题直接相关。".strip())
    first = first_sentence(abstract)
    if first:
        points.append(first)
    return points[:3]


def relevance_score(title: str, abstract: str) -> int:
    title_has_content = bool(compact_whitespace(title))
    abstract_has_content = bool(compact_whitespace(abstract))
    if not title_has_content:
        return 0
    if not abstract_has_content:
        return 1
    if len(compact_whitespace(abstract)) >= 160:
        return 3
    return 2


def body_inclusion_from_score(score: int, citable_points: list[str], evidence_level: str) -> tuple[str, str]:
    if score == 0:
        return "no", "缺少有效主题信息，暂不建议进入正文。"
    if score >= 2 and citable_points:
        return "yes", "相关度较高且存在可引用要点，可进入正文候选。"
    if score == 1 or evidence_level == "metadata_only":
        return "maybe", "信息证据不足，建议在筛选阶段复核后决定。"
    return "maybe", "建议保留为候选并在后续筛选中复核。"


def normalize_card_id(candidate_id: str, index: int) -> str:
    compact_candidate_id = compact_whitespace(candidate_id)
    if compact_candidate_id:
        return f"CARD_{compact_candidate_id}"
    return f"CARD_{index:06d}"


def parse_int_safe(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def build_minimal_card_from_candidate(candidate: dict[str, Any], index: int, now_iso: str) -> dict[str, Any]:
    candidate_id = compact_whitespace(candidate.get("candidate_id"))
    return {
        "card_id": normalize_card_id(candidate_id, index),
        "candidate_id": candidate_id,
        "title": compact_whitespace(candidate.get("title")),
        "year": str(candidate.get("year") or ""),
        "doi": compact_whitespace(candidate.get("doi")),
        "arxiv_id": compact_whitespace(candidate.get("arxiv_id")),
        "source_db": compact_whitespace(candidate.get("source_db")),
        "query_id": compact_whitespace(candidate.get("query_id")),
        "research_question": "信息缺失，待补充。",
        "method": "信息缺失，待补充。",
        "data": "信息缺失，待补充。",
        "main_findings": "信息缺失，待补充。",
        "limitations": "信息缺失，待补充。",
        "citable_points": [],
        "topic_relevance_score": 0,
        "body_inclusion": "maybe",
        "body_inclusion_reason": "缺少卡片内容，需人工复核。",
        "evidence_level": "metadata_only",
        "card_status": "needs_review",
        "created_at": now_iso,
        "updated_at": now_iso,
    }


def screening_decision_for_card(card: dict[str, Any], dedup_status: str) -> tuple[str, str, str]:
    if dedup_status != "unique":
        return "exclude", "R5", "重复或次优版本，初筛阶段不纳入。"

    score = parse_int_safe(card.get("topic_relevance_score"), default=0)
    evidence_level = compact_whitespace(card.get("evidence_level"))
    body_inclusion = compact_whitespace(card.get("body_inclusion")).lower()
    title = compact_whitespace(card.get("title"))
    method = compact_whitespace(card.get("method"))

    if not title:
        return "unsure", "R6", "缺少基础题录信息，需人工补充后复筛。"
    if score <= 0:
        return "exclude", "R1", "主题相关性弱，不建议进入正文。"
    if evidence_level == "metadata_only":
        return "unsure", "R2", "证据不足（仅元数据），建议保留复审。"
    if "方法信息有限" in method or "待补充" in method:
        return "unsure", "R3", "方法信息不充分，暂无法确认是否匹配。"
    if body_inclusion == "no":
        return "exclude", "R4", "范围边界不匹配，初筛排除。"
    if body_inclusion == "maybe":
        return "unsure", "R2", "当前证据尚不足，建议进入复审池。"
    return "include", "", "相关度和证据较好，建议进入正文候选。"


def build_card_row(
    candidate: dict[str, Any],
    record: dict[str, Any] | None,
    index: int,
    existing_card: dict[str, Any] | None,
    now_iso: str,
) -> dict[str, Any]:
    title = compact_whitespace(candidate.get("title")) or compact_whitespace((record or {}).get("title"))
    year = str(candidate.get("year") or (record or {}).get("year") or "")
    venue = compact_whitespace(candidate.get("venue") or (record or {}).get("venue"))
    abstract = compact_whitespace((record or {}).get("abstract"))

    evidence_level = "metadata_abstract" if abstract else "metadata_only"
    score = relevance_score(title, abstract)
    method_text = infer_method_text(title, abstract)
    data_text = infer_data_text(abstract)
    findings = first_sentence(abstract) or "摘要信息不足，结论待补充。"
    limitations = infer_limitations_text(abstract)
    citable_points = build_citable_points(title, abstract, venue, year)
    inclusion, inclusion_reason = body_inclusion_from_score(score, citable_points, evidence_level)
    card_status = "completed" if evidence_level == "metadata_abstract" else "needs_review"

    created_at = now_iso
    if existing_card and compact_whitespace(existing_card.get("created_at")):
        created_at = compact_whitespace(existing_card.get("created_at"))

    return {
        "card_id": normalize_card_id(candidate.get("candidate_id", ""), index),
        "candidate_id": compact_whitespace(candidate.get("candidate_id")),
        "title": title,
        "year": year,
        "doi": compact_whitespace(candidate.get("doi") or (record or {}).get("doi")),
        "arxiv_id": compact_whitespace(candidate.get("arxiv_id") or (record or {}).get("arxiv_id")),
        "source_db": compact_whitespace(candidate.get("source_db")),
        "query_id": compact_whitespace(candidate.get("query_id")),
        "research_question": f"该文献围绕“{title or '该主题'}”对应的问题展开研究。",
        "method": method_text,
        "data": data_text,
        "main_findings": findings,
        "limitations": limitations,
        "citable_points": citable_points,
        "topic_relevance_score": score,
        "body_inclusion": inclusion,
        "body_inclusion_reason": inclusion_reason,
        "evidence_level": evidence_level,
        "card_status": card_status,
        "created_at": created_at,
        "updated_at": now_iso,
    }

def find_duplicate(existing: list[dict[str, Any]], incoming: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    incoming_doi = normalize_identifier(incoming.get("doi"))
    if incoming_doi:
        for record in existing:
            if normalize_identifier(record.get("doi")) == incoming_doi:
                return record, "doi"

    incoming_arxiv_id = normalize_identifier(incoming.get("arxiv_id"))
    if incoming_arxiv_id:
        for record in existing:
            if normalize_identifier(record.get("arxiv_id")) == incoming_arxiv_id:
                return record, "arxiv_id"

    incoming_title = normalize_title(incoming.get("title"))
    incoming_year = str(incoming.get("year") or "")
    if incoming_title and incoming_year:
        for record in existing:
            existing_title = normalize_title(record.get("title"))
            existing_year = str(record.get("year") or "")
            if not existing_title or existing_year != incoming_year:
                continue
            similarity = difflib.SequenceMatcher(a=incoming_title, b=existing_title).ratio()
            if similarity >= TITLE_SIMILARITY_THRESHOLD:
                return record, "title_year"

    return None, None


def sanitize_token(token: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", token.lower())


def build_paper_id(record: dict[str, Any]) -> str:
    authors = record.get("authors") or []
    first_author = "unknown"
    if authors:
        first_author_raw = str(authors[0])
        if "," in first_author_raw:
            first_author = sanitize_token(first_author_raw.split(",", 1)[0]) or "unknown"
        else:
            first_author = sanitize_token(first_author_raw.split(" ", 1)[-1]) or "unknown"

    year = str(record.get("year") or "nd")
    title_tokens = [sanitize_token(tok) for tok in re.findall(r"[A-Za-z0-9]+", str(record.get("title") or ""))]
    title_tokens = [tok for tok in title_tokens if tok]
    short_title = "_".join(title_tokens[:4]) if title_tokens else "untitled"
    return f"{first_author}{year}_{short_title}"[:96]


def escape_bibtex(value: str) -> str:
    escaped = value.replace("\\", "\\\\")
    escaped = escaped.replace("{", "\\{").replace("}", "\\}")
    return escaped


def to_bibtex(record: dict[str, Any]) -> str:
    citation_key = record["citation_key"]
    title = escape_bibtex(str(record.get("title") or "Untitled"))
    authors = record.get("authors") or []
    author_value = " and ".join(escape_bibtex(str(author)) for author in authors) or "Unknown"
    year = str(record.get("year") or "n.d.")
    doi = compact_whitespace(record.get("doi"))
    url = compact_whitespace(record.get("pdf_url"))
    venue = escape_bibtex(str(record.get("venue") or ""))
    arxiv_id = compact_whitespace(record.get("arxiv_id"))

    if arxiv_id and not doi:
        fields = [
            f"  title = {{{title}}}",
            f"  author = {{{author_value}}}",
            f"  year = {{{year}}}",
            f"  eprint = {{{escape_bibtex(arxiv_id)}}}",
            "  archivePrefix = {arXiv}",
        ]
        if url:
            fields.append(f"  url = {{{escape_bibtex(url)}}}")
        return "@misc{" + citation_key + ",\n" + ",\n".join(fields) + "\n}\n"

    fields = [
        f"  title = {{{title}}}",
        f"  author = {{{author_value}}}",
        f"  year = {{{year}}}",
    ]
    if venue:
        fields.append(f"  journal = {{{venue}}}")
    if doi:
        fields.append(f"  doi = {{{escape_bibtex(doi)}}}")
    if url:
        fields.append(f"  url = {{{escape_bibtex(url)}}}")
    return "@article{" + citation_key + ",\n" + ",\n".join(fields) + "\n}\n"


def append_bib_if_missing(refs_bib: Path, record: dict[str, Any]) -> str:
    citation_key = record["citation_key"]
    content = refs_bib.read_text(encoding="utf-8")
    key_pattern = re.compile(rf"@\w+\{{\s*{re.escape(citation_key)}\s*,", flags=re.IGNORECASE)
    if key_pattern.search(content):
        return "exists"

    entry = to_bibtex(record)
    prefix = "" if content.endswith("\n") or not content else "\n"
    with refs_bib.open("a", encoding="utf-8") as handle:
        handle.write(prefix + entry)
    return "appended"


def append_record(records_jsonl: Path, record: dict[str, Any]) -> None:
    with records_jsonl.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def append_many_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def create_run_dir(runs_dir: Path) -> tuple[str, Path]:
    base_name = f"run_{local_now().strftime('%Y%m%d_%H%M%S')}"
    candidate = runs_dir / base_name
    suffix = 1
    while candidate.exists():
        candidate = runs_dir / f"{base_name}_{suffix}"
        suffix += 1
    candidate.mkdir(parents=True, exist_ok=False)
    return candidate.name, candidate


def write_manifest(run_dir: Path, manifest: dict[str, Any]) -> Path:
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def minimal_manual_record(args: argparse.Namespace) -> dict[str, Any]:
    if args.year is not None:
        year_value: int | None = int(args.year)
    else:
        year_value = None

    return {
        "doi": compact_whitespace(args.doi),
        "arxiv_id": compact_whitespace(args.arxiv_id),
        "title": compact_whitespace(args.title) or f"Unresolved reference ({args.doi or args.arxiv_id or 'manual'})",
        "authors": [],
        "year": year_value,
        "venue": compact_whitespace(args.venue),
        "abstract": compact_whitespace(args.abstract),
        "pdf_url": compact_whitespace(args.pdf_url),
        "source": "manual_fallback",
    }


def canonicalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "doi": compact_whitespace(record.get("doi")),
        "arxiv_id": compact_whitespace(record.get("arxiv_id")),
        "title": compact_whitespace(record.get("title")),
        "authors": [compact_whitespace(str(author)) for author in (record.get("authors") or []) if compact_whitespace(str(author))],
        "year": record.get("year"),
        "venue": compact_whitespace(record.get("venue")),
        "abstract": compact_whitespace(record.get("abstract")),
        "pdf_url": compact_whitespace(record.get("pdf_url")),
        "source": compact_whitespace(record.get("source")) or "unknown",
    }
    if isinstance(normalized["year"], str):
        normalized["year"] = parse_year(normalized["year"])
    return normalized


def file_stem_to_id(stem: str) -> str:
    tokens = [sanitize_token(t) for t in re.findall(r"[A-Za-z0-9]+", stem)]
    tokens = [t for t in tokens if t]
    if not tokens:
        return "manual_pdf"
    return "_".join(tokens[:6])


def ingest_pdf(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    source_path = Path(args.pdf).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"PDF not found: {source_path}")

    paper_id = compact_whitespace(args.paper_id) or file_stem_to_id(source_path.stem)
    suffix = source_path.suffix.lower() if source_path.suffix else ".pdf"
    target = paths.references_library / f"{paper_id}{suffix}"
    if target.exists():
        increment = 1
        while True:
            candidate = paths.references_library / f"{paper_id}_{increment}{suffix}"
            if not candidate.exists():
                target = candidate
                break
            increment += 1

    target.parent.mkdir(parents=True, exist_ok=True)
    if args.move:
        shutil.move(str(source_path), str(target))
        action = "moved"
    else:
        shutil.copy2(source_path, target)
        action = "copied"

    result = {
        "status": "ok",
        "action": action,
        "source": str(source_path),
        "target": str(target),
        "paper_id": paper_id,
    }
    append_log(paths, "INFO", "manual_pdf_ingested", result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def search_keyword(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    errors: list[dict[str, str]] = []
    results: list[dict[str, Any]] = []
    status = "ok"

    try:
        results = fetch_keyword_results(args.query, args.rows)
        append_many_jsonl(paths.search_results_jsonl, results)
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as exc:
        errors.append({"source": "keyword_search", "message": str(exc)})
        status = "failed"

    manifest = {
        "run_id": run_id,
        "timestamp": local_now().isoformat(timespec="seconds"),
        "inputs": {"query": args.query, "rows": args.rows},
        "enrich_source": "keyword_search",
        "status": status,
        "dedup_by": None,
        "outputs": {
            "search_results_jsonl": str(paths.search_results_jsonl.relative_to(paths.root)),
            "result_count": len(results),
        },
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "keyword_search_failed", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "keyword_search_completed",
        {"run_id": run_id, "query": args.query, "rows": args.rows, "result_count": len(results), "status": status},
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)),
                "result_count": len(results),
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def search_candidates(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    topic_frame_path = Path(args.topic_frame_json).expanduser().resolve()
    errors: list[dict[str, str]] = []
    source_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    status = "ok"
    adapter_stats: dict[str, dict[str, int]] = {}

    try:
        topic_frame = load_topic_frame_file(topic_frame_path)
    except Exception as exc:
        status = "failed"
        errors.append({"source": "topic_frame", "message": str(exc)})
        topic_frame = {}

    queries: list[str] = []
    constraints: dict[str, Any] = {}
    if topic_frame:
        constraints = topic_frame.get("search_constraints", {})
        queries = build_search_queries(topic_frame, args.max_queries)

    backend_order = parse_backend_order(args.backend_order)
    timestamp = local_now().isoformat(timespec="seconds")

    if not queries and not errors:
        status = "failed"
        errors.append({"source": "query_generation", "message": "No queries generated from topic_frame."})

    for query_index, query_text in enumerate(queries, start=1):
        query_id = f"{run_id}_Q{query_index:03d}"
        for backend in backend_order:
            adapter_stats.setdefault(backend, {"success": 0, "failed": 0, "results": 0})
            source_row = {
                "query_id": query_id,
                "source_db": backend,
                "query_string": query_text,
                "filters": compact_json({"output_type": ((topic_frame.get("output") or {}).get("type") if topic_frame else "")}),
                "time_range": compact_json(constraints.get("time_range", {})),
                "language_range": compact_json(constraints.get("language_range", {})),
                "venue_preference": compact_json(constraints.get("venue_preference", {})),
                "retrieved_at": timestamp,
                "result_count": 0,
                "status": "failed",
                "error": "",
            }

            try:
                raw_candidates = search_backend(
                    backend=backend,
                    query=query_text,
                    rows=args.rows_per_source,
                    constraints=constraints,
                )
                source_row["status"] = "ok"
                source_row["result_count"] = len(raw_candidates)
                adapter_stats[backend]["success"] += 1
                adapter_stats[backend]["results"] += len(raw_candidates)

                for raw in raw_candidates:
                    normalized = normalize_candidate_row(
                        raw_candidate=raw,
                        source_db=backend,
                        query_id=query_id,
                        retrieved_at=timestamp,
                    )
                    if normalized["title"]:
                        candidate_rows.append(normalized)
            except Exception as exc:
                source_row["status"] = "failed"
                source_row["error"] = str(exc)
                adapter_stats[backend]["failed"] += 1
                errors.append({"source": backend, "message": str(exc)})
            source_rows.append(source_row)

    dedup_stats = deduplicate_candidate_rows(candidate_rows) if candidate_rows else {
        "total_count": 0,
        "unique_count": 0,
        "duplicate_count": 0,
    }

    append_csv_rows(paths.search_sources_csv, SEARCH_SOURCE_COLUMNS, source_rows)
    append_csv_rows(paths.candidates_csv, CANDIDATE_COLUMNS, candidate_rows)

    if not candidate_rows:
        status = "failed"

    manifest = {
        "run_id": run_id,
        "timestamp": timestamp,
        "inputs": {
            "topic_frame_json": str(topic_frame_path),
            "max_queries": args.max_queries,
            "rows_per_source": args.rows_per_source,
            "backend_order": backend_order,
        },
        "status": status,
        "adapter_stats": adapter_stats,
        "query_count": len(queries),
        "outputs": {
            "search_sources_csv": str(paths.search_sources_csv.relative_to(paths.root)),
            "candidates_csv": str(paths.candidates_csv.relative_to(paths.root)),
            "source_rows_written": len(source_rows),
            "candidate_rows_written": len(candidate_rows),
            "dedup_stats": dedup_stats,
        },
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "search_candidates_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "search_candidates_completed",
        {
            "run_id": run_id,
            "status": status,
            "query_count": len(queries),
            "candidate_rows": len(candidate_rows),
            "unique_candidates": dedup_stats["unique_count"],
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)),
                "query_count": len(queries),
                "source_rows_written": len(source_rows),
                "candidate_rows_written": len(candidate_rows),
                "dedup_stats": dedup_stats,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cardify_candidates(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    timestamp = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    status = "ok"

    candidates_path = Path(args.candidates_csv).resolve() if args.candidates_csv else paths.candidates_csv
    records_path = Path(args.records_jsonl).resolve() if args.records_jsonl else paths.records_jsonl
    cards_path = Path(args.cards_jsonl).resolve() if args.cards_jsonl else paths.cards_jsonl

    try:
        candidates = read_csv_rows(candidates_path)
    except Exception as exc:
        candidates = []
        status = "failed"
        errors.append({"source": "candidates_csv", "message": str(exc)})

    try:
        records = load_records(records_path)
    except Exception as exc:
        records = []
        errors.append({"source": "records_jsonl", "message": str(exc)})

    try:
        existing_cards = load_jsonl(cards_path)
    except Exception as exc:
        existing_cards = []
        errors.append({"source": "cards_jsonl", "message": str(exc)})

    records_index = index_records(records)
    existing_by_candidate: dict[str, dict[str, Any]] = {}
    for card in existing_cards:
        cid = compact_whitespace(card.get("candidate_id"))
        if cid:
            existing_by_candidate[cid] = card

    result_cards: list[dict[str, Any]] = []
    cardified_count = 0
    skipped_existing = 0
    needs_review_count = 0

    for row_index, candidate in enumerate(candidates, start=1):
        candidate_id = compact_whitespace(candidate.get("candidate_id"))
        if "card_status" not in candidate:
            candidate["card_status"] = "not_started"

        existing_card = existing_by_candidate.get(candidate_id) if candidate_id else None
        if existing_card and not args.overwrite_existing:
            result_cards.append(existing_card)
            candidate["card_status"] = compact_whitespace(existing_card.get("card_status")) or "completed"
            skipped_existing += 1
            continue

        matched_record = matched_record_for_candidate(candidate, records_index)
        card = build_card_row(
            candidate=candidate,
            record=matched_record,
            index=row_index,
            existing_card=existing_card,
            now_iso=timestamp,
        )
        result_cards.append(card)
        candidate["card_status"] = card["card_status"]
        cardified_count += 1
        if card["card_status"] == "needs_review":
            needs_review_count += 1

    if not candidates and status == "ok":
        status = "failed"
        errors.append({"source": "candidates_csv", "message": "No candidate rows found to cardify."})

    fields = list(candidates[0].keys()) if candidates else list(CANDIDATE_COLUMNS)
    if "card_status" not in fields:
        fields.append("card_status")
    for row in candidates:
        if "card_status" not in row:
            row["card_status"] = "not_started"

    try:
        write_jsonl(cards_path, result_cards)
        write_csv_rows(candidates_path, fields, candidates)
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": timestamp,
        "inputs": {
            "candidates_csv": str(candidates_path),
            "records_jsonl": str(records_path),
            "cards_jsonl": str(cards_path),
            "overwrite_existing": bool(args.overwrite_existing),
        },
        "status": status,
        "outputs": {
            "cards_count": len(result_cards),
            "cardified_count": cardified_count,
            "skipped_existing": skipped_existing,
            "needs_review_count": needs_review_count,
            "cards_jsonl": str(cards_path),
            "updated_candidates_csv": str(candidates_path),
        },
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "cardify_candidates_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "cardify_candidates_completed",
        {
            "run_id": run_id,
            "status": status,
            "cards_count": len(result_cards),
            "needs_review_count": needs_review_count,
            "skipped_existing": skipped_existing,
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "cards_count": len(result_cards),
                "cardified_count": cardified_count,
                "skipped_existing": skipped_existing,
                "needs_review_count": needs_review_count,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def screen_candidates(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    status = "ok"

    candidates_path = Path(args.candidates_csv).resolve() if args.candidates_csv else paths.candidates_csv
    cards_path = Path(args.cards_jsonl).resolve() if args.cards_jsonl else paths.cards_jsonl
    decisions_path = (
        Path(args.screening_decisions_csv).resolve()
        if args.screening_decisions_csv
        else paths.screening_decisions_csv
    )
    included_path = (
        Path(args.included_candidates_csv).resolve()
        if args.included_candidates_csv
        else paths.included_candidates_csv
    )

    try:
        candidates = read_csv_rows(candidates_path)
    except Exception as exc:
        candidates = []
        status = "failed"
        errors.append({"source": "candidates_csv", "message": str(exc)})

    try:
        cards = load_jsonl(cards_path)
    except Exception as exc:
        cards = []
        status = "failed"
        errors.append({"source": "cards_jsonl", "message": str(exc)})

    cards_by_candidate: dict[str, dict[str, Any]] = {}
    for card in cards:
        cid = compact_whitespace(card.get("candidate_id"))
        if cid:
            cards_by_candidate[cid] = card

    decisions: list[dict[str, Any]] = []
    included_rows: list[dict[str, Any]] = []
    processed_count = 0
    include_count = 0
    unsure_count = 0
    exclude_count = 0
    allow_auto_minimal_cards = bool(getattr(args, "allow_auto_minimal_cards", False))
    missing_card_candidate_ids: list[str] = []

    for idx, candidate in enumerate(candidates, start=1):
        candidate_id = compact_whitespace(candidate.get("candidate_id"))
        if "screen_state" not in candidate:
            candidate["screen_state"] = "unreviewed"
        if "screen_decision" not in candidate:
            candidate["screen_decision"] = ""

        dedup_status = compact_whitespace(candidate.get("dedup_status"))
        if dedup_status != "unique":
            continue

        card = cards_by_candidate.get(candidate_id)
        if card is None:
            if allow_auto_minimal_cards:
                card = build_minimal_card_from_candidate(candidate, idx, now_iso)
                cards.append(card)
                cards_by_candidate[candidate_id] = card
            else:
                missing_card_candidate_ids.append(candidate_id or f"row_{idx}")
                continue

        decision, reason_code, reason_note = screening_decision_for_card(card, dedup_status)
        processed_count += 1
        if decision == "include":
            include_count += 1
        elif decision == "unsure":
            unsure_count += 1
        else:
            exclude_count += 1

        card["screen_decision"] = decision
        card["screen_reason_code"] = reason_code
        card["screen_reason_note"] = reason_note
        card["screened_at"] = now_iso
        card["updated_at"] = now_iso

        candidate["screen_state"] = "screened"
        candidate["screen_decision"] = decision

        decision_row = {
            "candidate_id": candidate_id,
            "card_id": compact_whitespace(card.get("card_id")),
            "decision": decision,
            "reason_code": reason_code,
            "reason_note": reason_note,
            "reviewer_mode": "rule_assisted_v1",
            "screened_at": now_iso,
            "topic_relevance_score": parse_int_safe(card.get("topic_relevance_score"), 0),
            "body_inclusion": compact_whitespace(card.get("body_inclusion")),
            "evidence_level": compact_whitespace(card.get("evidence_level")),
        }
        decisions.append(decision_row)

        if decision == "include":
            included_rows.append(
                {
                    "candidate_id": candidate_id,
                    "card_id": compact_whitespace(card.get("card_id")),
                    "title": compact_whitespace(candidate.get("title")),
                    "year": str(candidate.get("year") or ""),
                    "doi": compact_whitespace(candidate.get("doi")),
                    "arxiv_id": compact_whitespace(candidate.get("arxiv_id")),
                    "source_db": compact_whitespace(candidate.get("source_db")),
                    "query_id": compact_whitespace(candidate.get("query_id")),
                    "include_reason": reason_note,
                    "screened_at": now_iso,
                }
            )

    if missing_card_candidate_ids:
        status = "failed"
        preview = ", ".join(missing_card_candidate_ids[:10])
        if len(missing_card_candidate_ids) > 10:
            preview = f"{preview}, ..."
        errors.append(
            {
                "source": "cards_jsonl",
                "message": (
                    f"Missing cards for {len(missing_card_candidate_ids)} unique candidates. "
                    f"Run cardify-candidates first: {preview}"
                ),
            }
        )

    if processed_count == 0 and status == "ok":
        status = "failed"
        errors.append({"source": "screening", "message": "No unique candidates available for screening."})

    candidate_fields = list(candidates[0].keys()) if candidates else list(CANDIDATE_COLUMNS)
    if "screen_state" not in candidate_fields:
        candidate_fields.append("screen_state")
    if "screen_decision" not in candidate_fields:
        candidate_fields.append("screen_decision")

    try:
        write_csv_rows(candidates_path, candidate_fields, candidates)
        write_jsonl(cards_path, cards)
        write_csv_rows(decisions_path, SCREENING_DECISION_COLUMNS, decisions)
        write_csv_rows(included_path, INCLUDED_CANDIDATE_COLUMNS, included_rows)
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "candidates_csv": str(candidates_path),
            "cards_jsonl": str(cards_path),
            "scope": "dedup_status=unique",
        },
        "status": status,
        "outputs": {
            "screening_decisions_csv": str(decisions_path),
            "included_candidates_csv": str(included_path),
            "processed_count": processed_count,
            "include_count": include_count,
            "unsure_count": unsure_count,
            "exclude_count": exclude_count,
            "missing_card_count": len(missing_card_candidate_ids),
            "allow_auto_minimal_cards": allow_auto_minimal_cards,
        },
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "screen_candidates_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "screen_candidates_completed",
        {
            "run_id": run_id,
            "status": status,
            "processed_count": processed_count,
            "include_count": include_count,
            "unsure_count": unsure_count,
            "exclude_count": exclude_count,
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "processed_count": processed_count,
                "include_count": include_count,
                "unsure_count": unsure_count,
                "exclude_count": exclude_count,
                "missing_card_count": len(missing_card_candidate_ids),
                "allow_auto_minimal_cards": allow_auto_minimal_cards,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def build_cluster_term_tokens(claim_text: str, topic_terms: set[str]) -> list[str]:
    claim_tokens = tokenize_terms(claim_text)
    from_topic = [token for token in claim_tokens if token in topic_terms]
    selected = from_topic[:3] if from_topic else claim_tokens[:2]
    return deduplicate_preserve_order(selected)[:3]


def render_outline_from_section_plan(section_plan: list[dict[str, Any]]) -> str:
    lines = [
        "# 研究问题驱动正文大纲",
        "",
    ]
    for section in section_plan:
        title = compact_whitespace(section.get("section_title")) or "未命名章节"
        lines.append(f"## {title}")
        points = section.get("paragraph_points")
        if not isinstance(points, list) or not points:
            lines.append("- 待补充证据簇。")
            lines.append("")
            continue

        for index, point in enumerate(points, start=1):
            lines.append(f"### 段落要点 {index}")
            lines.append(f"- 主张：{compact_whitespace(point.get('claim'))}")
            lines.append(f"- 证据：{compact_whitespace(point.get('evidence'))}")
            lines.append(f"- 局限/边界：{compact_whitespace(point.get('limitation_or_boundary'))}")
            lines.append("")
    return "\n".join(lines).strip() + "\n"


def outline_from_evidence(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    status = "ok"

    topic_frame_path = Path(args.topic_frame_json).expanduser().resolve()
    cards_path = Path(args.cards_jsonl).resolve() if args.cards_jsonl else paths.cards_jsonl
    included_path = (
        Path(args.included_candidates_csv).resolve()
        if args.included_candidates_csv
        else paths.included_candidates_csv
    )
    claims_path = (
        Path(args.claims_jsonl).resolve()
        if args.claims_jsonl
        else (paths.root / "references" / "index" / "claims.jsonl")
    )
    argument_graph_path = (
        Path(args.argument_graph_json).resolve()
        if args.argument_graph_json
        else (paths.root / "references" / "index" / "argument_graph.json")
    )
    outline_markdown_path = (
        Path(args.outline_markdown).resolve()
        if args.outline_markdown
        else (paths.root / "outline" / "generated_outline.md")
    )

    try:
        topic_frame = load_topic_frame_file(topic_frame_path)
    except Exception as exc:
        topic_frame = {}
        status = "failed"
        errors.append({"source": "topic_frame", "message": str(exc)})

    if not included_path.exists():
        status = "failed"
        errors.append(
            {
                "source": "included_candidates_csv",
                "message": f"Included candidates file not found: {included_path}",
            }
        )
        included_rows: list[dict[str, Any]] = []
    else:
        try:
            included_rows = read_csv_rows(included_path)
        except Exception as exc:
            included_rows = []
            status = "failed"
            errors.append({"source": "included_candidates_csv", "message": str(exc)})

    try:
        cards = load_jsonl(cards_path)
    except Exception as exc:
        cards = []
        status = "failed"
        errors.append({"source": "cards_jsonl", "message": str(exc)})

    cards_by_candidate: dict[str, dict[str, Any]] = {}
    for card in cards:
        candidate_id = compact_whitespace(card.get("candidate_id"))
        if candidate_id and candidate_id not in cards_by_candidate:
            cards_by_candidate[candidate_id] = card

    include_candidate_ids: list[str] = []
    include_seen: set[str] = set()
    for row in included_rows:
        candidate_id = compact_whitespace(row.get("candidate_id"))
        if not candidate_id or candidate_id in include_seen:
            continue
        include_seen.add(candidate_id)
        include_candidate_ids.append(candidate_id)

    if not include_candidate_ids and status == "ok":
        status = "failed"
        errors.append({"source": "included_candidates_csv", "message": "No included candidates found."})

    missing_card_ids: list[str] = []
    candidate_card_pairs: list[tuple[str, dict[str, Any]]] = []
    for candidate_id in include_candidate_ids:
        card = cards_by_candidate.get(candidate_id)
        if card is None:
            missing_card_ids.append(candidate_id)
            continue
        candidate_card_pairs.append((candidate_id, card))

    if missing_card_ids:
        status = "failed"
        preview = ", ".join(missing_card_ids[:10])
        if len(missing_card_ids) > 10:
            preview = f"{preview}, ..."
        errors.append(
            {
                "source": "card_linking",
                "message": (
                    f"Missing cards for {len(missing_card_ids)} included candidates: {preview}"
                ),
            }
        )

    primary_rq, subquestion_rows, subquestion_token_sets = build_subquestion_rows(topic_frame)
    topic = compact_whitespace(topic_frame.get("topic"))
    topic_terms = flatten_topic_terms(topic_frame)
    if not topic_terms and topic:
        topic_terms = set(tokenize_terms(topic))

    claim_rows: list[dict[str, Any]] = []
    evidence_edges: list[dict[str, Any]] = []
    claim_counter = 1

    for candidate_id, card in sorted(candidate_card_pairs, key=lambda item: item[0]):
        source_items = build_claim_sources(card)
        source_items = sorted(
            source_items,
            key=lambda item: (CLAIM_SOURCE_PRIORITY.get(item[0], 99), item[1].lower()),
        )
        for source_field, claim_text in source_items:
            cleaned_text = compact_whitespace(claim_text)
            if not cleaned_text:
                continue

            claim_type = classify_claim_type(source_field, cleaned_text)
            if claim_type not in CLAIM_TYPES:
                claim_type = "observation"
            subquestion_id = map_claim_to_subquestion(cleaned_text, subquestion_rows, subquestion_token_sets)
            claim_id = f"CLM{claim_counter:06d}"
            claim_counter += 1

            evidence_level = compact_whitespace(card.get("evidence_level"))
            claim_row = {
                "claim_id": claim_id,
                "candidate_id": candidate_id,
                "card_id": compact_whitespace(card.get("card_id")),
                "claim_type": claim_type,
                "claim_text": cleaned_text,
                "evidence_snippet": first_sentence(cleaned_text) or cleaned_text,
                "subquestion_id": subquestion_id,
                "confidence": claim_confidence(source_field, evidence_level),
                "source_field": source_field,
                "topic_relevance_score": parse_int_safe(card.get("topic_relevance_score"), 0),
                "evidence_level": evidence_level,
            }
            claim_rows.append(claim_row)
            evidence_edges.append(
                {
                    "claim_id": claim_id,
                    "candidate_id": candidate_id,
                    "card_id": compact_whitespace(card.get("card_id")),
                }
            )

    if not claim_rows and status == "ok":
        status = "failed"
        errors.append({"source": "claim_extraction", "message": "No claims extracted from included cards."})

    cluster_buckets: dict[str, dict[str, Any]] = {}
    for claim in claim_rows:
        term_tokens = build_cluster_term_tokens(claim["claim_text"], topic_terms)
        term_key = "-".join(term_tokens) if term_tokens else "general"
        cluster_key = f"{claim['subquestion_id']}|{claim['claim_type']}|{term_key}"
        claim["cluster_key"] = cluster_key
        bucket = cluster_buckets.setdefault(
            cluster_key,
            {
                "subquestion_id": claim["subquestion_id"],
                "claim_type": claim["claim_type"],
                "normalized_topic_terms": term_tokens,
                "claim_rows": [],
            },
        )
        bucket["claim_rows"].append(claim)

    cluster_id_by_key: dict[str, str] = {}
    clusters: list[dict[str, Any]] = []
    for index, cluster_key in enumerate(sorted(cluster_buckets.keys()), start=1):
        bucket = cluster_buckets[cluster_key]
        claim_list = sorted(
            bucket["claim_rows"],
            key=lambda row: (
                -float(row.get("confidence", 0.0)),
                row.get("candidate_id", ""),
                row.get("claim_id", ""),
            ),
        )
        cluster_id = f"CLS{index:03d}"
        cluster_id_by_key[cluster_key] = cluster_id
        relevance_scores = [parse_int_safe(row.get("topic_relevance_score"), 0) for row in claim_list]
        avg_relevance = round(sum(relevance_scores) / len(relevance_scores), 2) if relevance_scores else 0.0
        candidate_ids = deduplicate_preserve_order(
            [compact_whitespace(row.get("candidate_id")) for row in claim_list if compact_whitespace(row.get("candidate_id"))]
        )
        evidence_levels = sorted(
            {
                compact_whitespace(row.get("evidence_level"))
                for row in claim_list
                if compact_whitespace(row.get("evidence_level"))
            }
        )
        clusters.append(
            {
                "cluster_id": cluster_id,
                "cluster_key": cluster_key,
                "subquestion_id": bucket["subquestion_id"],
                "claim_type": bucket["claim_type"],
                "normalized_topic_terms": bucket["normalized_topic_terms"],
                "claim_ids": [row["claim_id"] for row in claim_list],
                "candidate_ids": candidate_ids,
                "evidence_count": len(claim_list),
                "avg_relevance_score": avg_relevance,
                "evidence_levels": evidence_levels,
            }
        )

    claims_by_subquestion: dict[str, list[dict[str, Any]]] = {}
    for claim in claim_rows:
        claims_by_subquestion.setdefault(claim["subquestion_id"], []).append(claim)

    clusters_by_subquestion: dict[str, list[dict[str, Any]]] = {}
    for cluster in clusters:
        clusters_by_subquestion.setdefault(cluster["subquestion_id"], []).append(cluster)

    section_plan: list[dict[str, Any]] = []
    for section_index, subquestion in enumerate(subquestion_rows, start=1):
        subquestion_id = subquestion["subquestion_id"]
        section_clusters = clusters_by_subquestion.get(subquestion_id, [])
        ranked_clusters = sorted(
            section_clusters,
            key=lambda item: (
                -int(item.get("evidence_count", 0)),
                -float(item.get("avg_relevance_score", 0.0)),
                CLAIM_TYPE_RANK.get(item.get("claim_type", "observation"), 99),
                item.get("cluster_id", ""),
            ),
        )

        sub_claims = claims_by_subquestion.get(subquestion_id, [])
        limitation_claims = [claim for claim in sub_claims if claim.get("claim_type") == "limitation"]
        paragraph_points: list[dict[str, Any]] = []
        for cluster in ranked_clusters:
            cluster_claims = [claim for claim in sub_claims if claim.get("cluster_key") == cluster.get("cluster_key")]
            if not cluster_claims:
                continue
            lead_claim = sorted(
                cluster_claims,
                key=lambda row: (-float(row.get("confidence", 0.0)), row.get("claim_id", "")),
            )[0]
            limitation_text = "待补充局限证据。"
            if lead_claim.get("claim_type") == "limitation":
                limitation_text = lead_claim.get("claim_text", limitation_text)
            elif limitation_claims:
                limitation_text = limitation_claims[0].get("claim_text", limitation_text)

            paragraph_points.append(
                {
                    "cluster_id": cluster["cluster_id"],
                    "claim_type": lead_claim.get("claim_type", "observation"),
                    "claim": lead_claim.get("claim_text", ""),
                    "evidence": lead_claim.get("evidence_snippet", ""),
                    "limitation_or_boundary": limitation_text,
                }
            )

        section_plan.append(
            {
                "section_id": f"SEC{section_index:03d}",
                "subquestion_id": subquestion_id,
                "section_title": f"子问题{section_index}：{subquestion['text']}",
                "cluster_ids": [cluster["cluster_id"] for cluster in ranked_clusters],
                "paragraph_points": paragraph_points,
            }
        )

    for claim in claim_rows:
        cluster_key = claim.get("cluster_key", "")
        claim["cluster_id"] = cluster_id_by_key.get(cluster_key, "")

    for edge in evidence_edges:
        claim_id = edge.get("claim_id")
        claim = next((item for item in claim_rows if item["claim_id"] == claim_id), None)
        if claim is not None:
            edge["cluster_id"] = claim.get("cluster_id", "")

    claim_rows = sorted(claim_rows, key=lambda row: row["claim_id"])
    evidence_edges = sorted(evidence_edges, key=lambda row: row["claim_id"])
    clusters = sorted(clusters, key=lambda row: row["cluster_id"])

    claims_output_rows: list[dict[str, Any]] = []
    for row in claim_rows:
        claims_output_rows.append(
            {
                "claim_id": row["claim_id"],
                "candidate_id": row["candidate_id"],
                "card_id": row["card_id"],
                "claim_type": row["claim_type"],
                "claim_text": row["claim_text"],
                "evidence_snippet": row["evidence_snippet"],
                "subquestion_id": row["subquestion_id"],
                "confidence": row["confidence"],
            }
        )

    argument_graph = {
        "topic": topic,
        "primary_rq": primary_rq,
        "subquestions": subquestion_rows,
        "claim_nodes": claims_output_rows,
        "evidence_edges": evidence_edges,
        "clusters": clusters,
        "section_plan": section_plan,
    }

    try:
        write_jsonl(claims_path, claims_output_rows)
        argument_graph_path.parent.mkdir(parents=True, exist_ok=True)
        argument_graph_path.write_text(
            json.dumps(argument_graph, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        outline_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        outline_markdown_path.write_text(
            render_outline_from_section_plan(section_plan),
            encoding="utf-8",
        )
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "topic_frame_json": str(topic_frame_path),
            "cards_jsonl": str(cards_path),
            "included_candidates_csv": str(included_path),
        },
        "status": status,
        "outputs": {
            "claims_jsonl": str(claims_path),
            "argument_graph_json": str(argument_graph_path),
            "outline_markdown": str(outline_markdown_path),
            "included_candidate_count": len(include_candidate_ids),
            "missing_card_count": len(missing_card_ids),
            "claim_count": len(claim_rows),
            "cluster_count": len(clusters),
            "section_count": len(section_plan),
        },
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "outline_from_evidence_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "outline_from_evidence_completed",
        {
            "run_id": run_id,
            "status": status,
            "included_candidate_count": len(include_candidate_ids),
            "claim_count": len(claim_rows),
            "section_count": len(section_plan),
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "included_candidate_count": len(include_candidate_ids),
                "missing_card_count": len(missing_card_ids),
                "claim_count": len(claim_rows),
                "cluster_count": len(clusters),
                "section_count": len(section_plan),
                "outputs": {
                    "claims_jsonl": str(claims_path),
                    "argument_graph_json": str(argument_graph_path),
                    "outline_markdown": str(outline_markdown_path),
                },
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def enrich_reference(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    errors: list[dict[str, str]] = []
    enrich_source = "manual_fallback"
    resolved: dict[str, Any] | None = None

    if args.doi:
        try:
            resolved = fetch_from_doi(args.doi)
            enrich_source = "doi"
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError) as exc:
            errors.append({"source": "doi", "message": str(exc)})

    if resolved is None and args.arxiv_id:
        try:
            resolved = fetch_from_arxiv(args.arxiv_id)
            enrich_source = "arxiv"
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, et.ParseError) as exc:
            errors.append({"source": "arxiv", "message": str(exc)})

    if resolved is None:
        resolved = minimal_manual_record(args)
    else:
        if args.doi and not resolved.get("doi"):
            resolved["doi"] = args.doi
        if args.arxiv_id and not resolved.get("arxiv_id"):
            resolved["arxiv_id"] = args.arxiv_id
        if args.title and not resolved.get("title"):
            resolved["title"] = args.title

    canonical_record = canonicalize_record(resolved)
    existing = load_records(paths.records_jsonl)
    duplicate_record, duplicate_by = find_duplicate(existing, canonical_record)
    status = "duplicate" if duplicate_record else "created"
    bib_update = "skipped"

    if duplicate_record:
        canonical_record = duplicate_record
    else:
        canonical_record["paper_id"] = build_paper_id(canonical_record)
        canonical_record["citation_key"] = canonical_record["paper_id"]
        canonical_record["created_at"] = utc_now_iso()
        append_record(paths.records_jsonl, canonical_record)
        bib_update = append_bib_if_missing(paths.refs_bib, canonical_record)

    manifest = {
        "run_id": run_id,
        "timestamp": local_now().isoformat(timespec="seconds"),
        "inputs": {
            "doi": args.doi,
            "arxiv_id": args.arxiv_id,
            "title": args.title,
            "year": args.year,
            "venue": args.venue,
        },
        "enrich_source": enrich_source,
        "status": status,
        "dedup_by": duplicate_by,
        "outputs": {
            "records_jsonl": str(paths.records_jsonl.relative_to(paths.root)),
            "refs_bib": str(paths.refs_bib.relative_to(paths.root)),
            "record": canonical_record,
            "bib_update": bib_update,
        },
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "enrichment_with_errors", {"run_id": run_id, "errors": errors})

    append_log(
        paths,
        "INFO",
        "enrichment_completed",
        {"run_id": run_id, "status": status, "dedup_by": duplicate_by, "enrich_source": enrich_source},
    )

    print(
        json.dumps(
            {
                "status": status,
                "dedup_by": duplicate_by,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)),
                "record": canonical_record,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reference pipeline with DOI/arXiv enrichment + JSONL/BibTeX persistence."
    )
    parser.add_argument("--base-dir", help="Optional workspace root. Defaults to repository root.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest-pdf", help="Copy/move a manual PDF into references/library.")
    ingest.add_argument("--pdf", required=True, help="Path to local PDF file.")
    ingest.add_argument("--paper-id", default="", help="Optional paper_id. Defaults to sanitized file stem.")
    ingest.add_argument("--move", action="store_true", help="Move file instead of copy.")
    ingest.set_defaults(func=ingest_pdf)

    search = subparsers.add_parser("search-keyword", help="Search Crossref by keyword and append results JSONL.")
    search.add_argument("--query", required=True, help="Keyword query.")
    search.add_argument("--rows", type=int, default=20, help="Max rows to fetch (default: 20).")
    search.set_defaults(func=search_keyword)

    search_candidates_parser = subparsers.add_parser(
        "search-candidates",
        help="Generate candidate pool CSV from TFR-1 topic frame without downloading PDFs.",
    )
    search_candidates_parser.add_argument(
        "--topic-frame-json",
        required=True,
        help="Path to topic frame JSON/YAML file (expects `topic_frame` payload or root-equivalent fields).",
    )
    search_candidates_parser.add_argument(
        "--max-queries",
        type=int,
        default=12,
        help="Maximum number of generated queries from topic frame.",
    )
    search_candidates_parser.add_argument(
        "--rows-per-source",
        type=int,
        default=20,
        help="Maximum result rows fetched per query per backend.",
    )
    search_candidates_parser.add_argument(
        "--backend-order",
        default="openalex,crossref,arxiv,mcp",
        help="Comma-separated backend order, e.g. openalex,crossref,arxiv,mcp",
    )
    search_candidates_parser.set_defaults(func=search_candidates)

    cardify = subparsers.add_parser(
        "cardify-candidates",
        help="Generate structured literature cards from candidates.csv and records.jsonl.",
    )
    cardify.add_argument(
        "--candidates-csv",
        default="",
        help="Path to candidates CSV. Defaults to references/index/candidates.csv.",
    )
    cardify.add_argument(
        "--records-jsonl",
        default="",
        help="Path to records JSONL. Defaults to references/index/records.jsonl.",
    )
    cardify.add_argument(
        "--cards-jsonl",
        default="",
        help="Path to output cards JSONL. Defaults to references/index/cards.jsonl.",
    )
    cardify.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Overwrite existing card rows with regenerated content.",
    )
    cardify.set_defaults(func=cardify_candidates)

    screening = subparsers.add_parser(
        "screen-candidates",
        help="Run title+abstract screening on unique candidates using cards.jsonl.",
    )
    screening.add_argument(
        "--candidates-csv",
        default="",
        help="Path to candidates CSV. Defaults to references/index/candidates.csv.",
    )
    screening.add_argument(
        "--cards-jsonl",
        default="",
        help="Path to cards JSONL. Defaults to references/index/cards.jsonl.",
    )
    screening.add_argument(
        "--screening-decisions-csv",
        default="",
        help="Path to screening decisions CSV. Defaults to references/index/screening_decisions.csv.",
    )
    screening.add_argument(
        "--included-candidates-csv",
        default="",
        help="Path to included candidates CSV. Defaults to references/index/included_candidates.csv.",
    )
    screening.add_argument(
        "--allow-auto-minimal-cards",
        action="store_true",
        help=(
            "Allow screening to auto-create minimal cards for unique candidates missing in cards.jsonl. "
            "Default behavior is strict: fail and require running cardify-candidates first."
        ),
    )
    screening.set_defaults(func=screen_candidates)

    outline_from_evidence_parser = subparsers.add_parser(
        "outline-from-evidence",
        help="Build argument graph and chapter outline from included evidence cards.",
    )
    outline_from_evidence_parser.add_argument(
        "--topic-frame-json",
        required=True,
        help="Path to topic frame JSON/YAML file (expects `topic_frame` payload or equivalent root fields).",
    )
    outline_from_evidence_parser.add_argument(
        "--cards-jsonl",
        default="",
        help="Path to cards JSONL. Defaults to references/index/cards.jsonl.",
    )
    outline_from_evidence_parser.add_argument(
        "--included-candidates-csv",
        default="",
        help="Path to included candidates CSV. Defaults to references/index/included_candidates.csv.",
    )
    outline_from_evidence_parser.add_argument(
        "--argument-graph-json",
        default="",
        help="Path to output argument graph JSON. Defaults to references/index/argument_graph.json.",
    )
    outline_from_evidence_parser.add_argument(
        "--claims-jsonl",
        default="",
        help="Path to output claims JSONL. Defaults to references/index/claims.jsonl.",
    )
    outline_from_evidence_parser.add_argument(
        "--outline-markdown",
        default="",
        help="Path to output outline Markdown. Defaults to outline/generated_outline.md.",
    )
    outline_from_evidence_parser.set_defaults(func=outline_from_evidence)

    enrich = subparsers.add_parser("enrich", help="Enrich one reference and persist it.")
    enrich.add_argument("--doi", help="DOI identifier, e.g. 10.1021/acscatal.0c01234")
    enrich.add_argument("--arxiv-id", help="arXiv identifier, e.g. 2401.12345")
    enrich.add_argument("--title", help="Fallback title if DOI/arXiv lookup fails.")
    enrich.add_argument("--year", type=int, help="Fallback publication year.")
    enrich.add_argument("--venue", default="", help="Fallback venue/journal.")
    enrich.add_argument("--abstract", default="", help="Fallback abstract.")
    enrich.add_argument("--pdf-url", default="", help="Fallback PDF URL.")
    enrich.set_defaults(func=enrich_reference)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "enrich" and not any([args.doi, args.arxiv_id, args.title]):
        parser.error("enrich requires at least one of --doi, --arxiv-id, or --title")
    if args.command == "search-candidates":
        if args.max_queries <= 0:
            parser.error("--max-queries must be > 0")
        if args.rows_per_source <= 0:
            parser.error("--rows-per-source must be > 0")

    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
