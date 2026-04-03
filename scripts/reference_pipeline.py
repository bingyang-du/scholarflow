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
import subprocess
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

FULLTEXT_FETCH_LOG_COLUMNS = [
    "candidate_id",
    "card_id",
    "doi",
    "arxiv_id",
    "source_url",
    "target_path",
    "status",
    "http_code",
    "error",
    "retried",
    "retrieved_at",
]

DOWNLOADED_FULLTEXT_COLUMNS = [
    "candidate_id",
    "card_id",
    "target_path",
    "source_url",
    "retrieved_at",
]

CITATION_AUDIT_FINDINGS_COLUMNS = [
    "finding_id",
    "category",
    "severity",
    "section_id",
    "section_title",
    "file_path",
    "claim_text",
    "citation_keys",
    "message",
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

ASSERTION_KEYWORDS = (
    "demonstrate",
    "demonstrates",
    "prove",
    "proves",
    "show",
    "shows",
    "reveal",
    "reveals",
    "confirm",
    "confirms",
    "indicate",
    "indicates",
    "dominates",
    "dominant",
    "increase",
    "decrease",
    "superior",
    "outperform",
)

PARAGRAPH_TYPES = (
    "背景段",
    "定义段",
    "比较段",
    "方法段",
    "机制解释段",
    "争议/局限段",
    "小结段",
)
CORE_PARAGRAPH_TYPES = ("机制解释段", "比较段", "方法段")
DEFINITION_KEYWORDS = (
    "define",
    "defined",
    "definition",
    "refers to",
    "denote",
    "concept",
    "定义",
    "指的是",
)
METHOD_KEYWORDS = (
    "method",
    "methods",
    "experiment",
    "experimental",
    "kinetic",
    "microkinetic",
    "simulation",
    "model",
    "dft",
    "procedure",
    "protocol",
    "方法",
    "实验",
    "模型",
    "模拟",
)
EVIDENCE_STRENGTH_LABELS = ("strong", "medium", "weak")
EVIDENCE_LEVEL_SCORE = {
    "metadata_abstract": 1.0,
    "metadata_only": 0.5,
}
CONFLICT_DIRECTION_KEYWORDS = {
    "up": ("increase", "increases", "higher", "rise", "rises", "improve", "improves", "promotion"),
    "down": ("decrease", "decreases", "lower", "drop", "drops", "reduce", "reduces", "suppression"),
    "better": ("better", "outperform", "outperforms", "superior", "advantage"),
    "worse": ("worse", "inferior", "underperform", "underperforms", "disadvantage"),
    "dominate": ("dominate", "dominates", "dominant"),
    "suppress": ("suppress", "suppresses", "inhibit", "inhibits"),
}
CONFLICT_OPPOSITE_PAIRS = (
    ("up", "down"),
    ("better", "worse"),
    ("dominate", "suppress"),
)
HIGH_RISK_STRENGTH_LABELS = {"weak"}
CONSISTENCY_TRANSITION_MARKERS = (
    "however,",
    "by contrast,",
    "in contrast,",
    "meanwhile,",
    "additionally,",
    "further,",
    "taken together,",
    "building on the previous paragraph,",
)
CONSISTENCY_BOUNDARY_SENTENCE = "Current evidence is limited to reported conditions and should not be generalized."
CONSISTENCY_UNCERTAINTY_SENTENCE = "Uncertainty remains due to incomplete direct support for this claim."
CONSISTENCY_ISSUE_PENALTY = {
    "adjacent_duplication": 5,
    "term_consistency": 3,
    "logical_jump": 5,
    "claim_evidence_order": 8,
    "overclaim_without_support": 12,
}
SECTION_AUDIT_FINDING_TYPES = (
    "coverage",
    "support",
    "isolated_citation",
    "cited_not_used",
    "strong_claim_weak_evidence",
    "overgeneralization",
)
SECTION_AUDIT_STRONG_CLAIM_KEYWORDS = (
    "prove",
    "proves",
    "demonstrate",
    "demonstrates",
    "confirm",
    "confirms",
    "definitive",
    "clearly",
    "dominates",
    "outperform",
    "outperforms",
    "significantly",
)
SECTION_AUDIT_ABSOLUTE_TERMS = (
    "always",
    "never",
    "all",
    "none",
    "without exception",
    "unequivocally",
    "definitively",
    "universally",
    "must",
    "必然",
    "总是",
    "完全",
)
SECTION_AUDIT_BOUNDARY_TERMS = (
    "suggest",
    "suggests",
    "may",
    "might",
    "likely",
    "within",
    "under",
    "condition",
    "conditions",
    "boundary",
    "boundaries",
    "uncertain",
    "uncertainty",
    "limited",
    "scope",
    "范围",
    "条件",
    "可能",
    "边界",
    "局限",
)
SECTION_AUDIT_DEDUCTIONS = {
    "coverage": 20,
    "support": 15,
    "isolated_citation": 8,
    "cited_not_used": 8,
    "strong_claim_weak_evidence": 15,
    "overgeneralization": 12,
}
FULL_DRAFT_SCORE_DEDUCTIONS = {
    "unresolved_high_risk": 20,
    "crossref_issue": 8,
    "figure_table_ref_issue": 6,
    "citation_key_missing": 12,
}
SECTION_GATE_DEDUCTIONS = {"high": 20, "medium": 8, "low": 3}
SECTION_GATE_DECISIONS = ("go", "revise", "block")
FIGURE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".pdf", ".eps", ".svg"}
PIPELINE_STAGE_ORDER = [
    "search-candidates",
    "cardify-candidates",
    "screen-candidates",
    "fetch-fulltext",
    "outline-from-evidence",
    "generate-paragraph-plans",
    "assemble-evidence-packets",
    "generate-section-drafts",
    "revise-section-consistency",
    "section-citation-audit",
    "section-release-gate",
    "generate-cross-section-bridges",
    "export-claim-trace-matrix",
    "ground-figure-table-links",
    "generate-latex-draft",
    "assemble-full-draft",
    "citation-audit",
    "latex-build-qa",
]
PIPELINE_STAGE_ALIASES = {"full-draft": "assemble-full-draft"}
PIPELINE_STAGE_CHOICES = PIPELINE_STAGE_ORDER + ["full-draft"]


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


def safe_filename_component(value: str) -> str:
    compact = compact_whitespace(value)
    if not compact:
        return ""
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", compact)
    return normalized.strip("_")


def default_arxiv_pdf_url(arxiv_id: str) -> str:
    cleaned = compact_whitespace(arxiv_id)
    if not cleaned:
        return ""
    cleaned = cleaned.replace("arXiv:", "").replace("arxiv:", "")
    if "/" in cleaned:
        cleaned = cleaned.rsplit("/", 1)[-1]
    return f"https://arxiv.org/pdf/{cleaned}.pdf"


def resolve_paper_id(candidate_id: str, included_row: dict[str, Any], card: dict[str, Any] | None, record: dict[str, Any] | None) -> str:
    for payload in (record, card, included_row):
        if not isinstance(payload, dict):
            continue
        for key in ("paper_id", "citation_key"):
            token = safe_filename_component(str(payload.get(key) or ""))
            if token:
                return token

    if isinstance(record, dict):
        try:
            computed = safe_filename_component(build_paper_id(canonicalize_record(record)))
            if computed:
                return computed
        except Exception:
            pass

    for payload in (included_row, card, record):
        if not isinstance(payload, dict):
            continue
        doi = safe_filename_component(str(payload.get("doi") or ""))
        if doi:
            return doi
        arxiv_id = safe_filename_component(str(payload.get("arxiv_id") or ""))
        if arxiv_id:
            return arxiv_id

    fallback = safe_filename_component(candidate_id)
    return fallback or "paper"


def resolve_fulltext_source_url(
    included_row: dict[str, Any],
    card: dict[str, Any] | None,
    record: dict[str, Any] | None,
) -> str:
    for payload in (record, card, included_row):
        if isinstance(payload, dict):
            candidate_url = compact_whitespace(payload.get("pdf_url"))
            if candidate_url:
                return candidate_url

    for payload in (record, card, included_row):
        if isinstance(payload, dict):
            arxiv_id = compact_whitespace(payload.get("arxiv_id"))
            if arxiv_id:
                return default_arxiv_pdf_url(arxiv_id)
    return ""


def download_pdf_to_path(
    source_url: str,
    target_path: Path,
    max_retries: int,
    timeout_seconds: int,
) -> tuple[str, int | None, str, int]:
    retries = 0
    max_attempts = max(1, int(max_retries) + 1)
    last_error = ""
    last_http_code: int | None = None

    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_suffix(target_path.suffix + ".part")

    for attempt in range(1, max_attempts + 1):
        try:
            request = urllib.request.Request(
                url=source_url,
                headers={
                    "User-Agent": "codex-reference-pipeline/1.0",
                    "Accept": "application/pdf,*/*",
                },
            )
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read()
                status_code = response.getcode()
            if not payload:
                raise ValueError("Empty response body")
            temp_path.write_bytes(payload)
            temp_path.replace(target_path)
            return "downloaded", status_code, "", retries
        except urllib.error.HTTPError as exc:
            last_http_code = int(exc.code)
            last_error = str(exc)
        except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
            last_error = str(exc)

        retries = attempt
        if attempt >= max_attempts:
            break

    if temp_path.exists():
        temp_path.unlink(missing_ok=True)
    return "failed", last_http_code, last_error, retries


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


def fetch_fulltext(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    status = "ok"

    included_path = (
        Path(args.included_candidates_csv).resolve()
        if args.included_candidates_csv
        else paths.included_candidates_csv
    )
    cards_path = Path(args.cards_jsonl).resolve() if args.cards_jsonl else paths.cards_jsonl
    records_path = Path(args.records_jsonl).resolve() if args.records_jsonl else paths.records_jsonl
    download_log_path = (
        Path(args.download_log_csv).resolve()
        if args.download_log_csv
        else (paths.root / "references" / "index" / "fulltext_fetch_log.csv")
    )
    downloaded_index_path = (
        Path(args.downloaded_index_csv).resolve()
        if args.downloaded_index_csv
        else (paths.root / "references" / "index" / "downloaded_fulltexts.csv")
    )
    library_dir = Path(args.library_dir).resolve() if args.library_dir else paths.references_library
    max_retries = max(0, int(args.max_retries))
    timeout_seconds = max(1, int(args.timeout_seconds))

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

    try:
        records = load_records(records_path)
    except Exception as exc:
        records = []
        status = "failed"
        errors.append({"source": "records_jsonl", "message": str(exc)})

    records_index = index_records(records)
    cards_by_candidate: dict[str, dict[str, Any]] = {}
    cards_by_card_id: dict[str, dict[str, Any]] = {}
    for card in cards:
        candidate_id = compact_whitespace(card.get("candidate_id"))
        card_id = compact_whitespace(card.get("card_id"))
        if candidate_id and candidate_id not in cards_by_candidate:
            cards_by_candidate[candidate_id] = card
        if card_id and card_id not in cards_by_card_id:
            cards_by_card_id[card_id] = card

    unique_include_rows: list[dict[str, Any]] = []
    seen_candidate_ids: set[str] = set()
    for row in included_rows:
        candidate_id = compact_whitespace(row.get("candidate_id"))
        if not candidate_id or candidate_id in seen_candidate_ids:
            continue
        seen_candidate_ids.add(candidate_id)
        unique_include_rows.append(row)

    attempted_count = 0
    success_count = 0
    exists_count = 0
    failed_count = 0
    no_url_count = 0
    download_log_rows: list[dict[str, Any]] = []
    downloaded_rows: list[dict[str, Any]] = []

    for row in unique_include_rows:
        candidate_id = compact_whitespace(row.get("candidate_id"))
        card_id = compact_whitespace(row.get("card_id"))
        attempted_count += 1

        card = cards_by_candidate.get(candidate_id)
        if card is None and card_id:
            card = cards_by_card_id.get(card_id)

        candidate_stub = {
            "doi": compact_whitespace(row.get("doi")) or compact_whitespace((card or {}).get("doi")),
            "arxiv_id": compact_whitespace(row.get("arxiv_id")) or compact_whitespace((card or {}).get("arxiv_id")),
            "title": compact_whitespace(row.get("title")) or compact_whitespace((card or {}).get("title")),
            "year": str(row.get("year") or (card or {}).get("year") or ""),
        }
        record = matched_record_for_candidate(candidate_stub, records_index)
        source_url = resolve_fulltext_source_url(row, card, record)
        doi_value = candidate_stub["doi"]
        arxiv_id_value = candidate_stub["arxiv_id"]

        paper_id = resolve_paper_id(candidate_id, row, card, record)
        if paper_id:
            file_name = f"{candidate_id}__{paper_id}.pdf"
        else:
            file_name = f"{candidate_id}.pdf"
        target_path = library_dir / file_name

        if not source_url:
            no_url_count += 1
            errors.append(
                {
                    "source": "no_url",
                    "message": f"No fulltext URL resolved for {candidate_id}",
                }
            )
            download_log_rows.append(
                {
                    "candidate_id": candidate_id,
                    "card_id": card_id,
                    "doi": doi_value,
                    "arxiv_id": arxiv_id_value,
                    "source_url": "",
                    "target_path": str(target_path),
                    "status": "no_url",
                    "http_code": "",
                    "error": "No resolved fulltext URL",
                    "retried": 0,
                    "retrieved_at": now_iso,
                }
            )
            continue

        if target_path.exists():
            exists_count += 1
            download_log_rows.append(
                {
                    "candidate_id": candidate_id,
                    "card_id": card_id,
                    "doi": doi_value,
                    "arxiv_id": arxiv_id_value,
                    "source_url": source_url,
                    "target_path": str(target_path),
                    "status": "exists",
                    "http_code": "",
                    "error": "",
                    "retried": 0,
                    "retrieved_at": now_iso,
                }
            )
            downloaded_rows.append(
                {
                    "candidate_id": candidate_id,
                    "card_id": card_id,
                    "target_path": str(target_path),
                    "source_url": source_url,
                    "retrieved_at": now_iso,
                }
            )
            continue

        download_status, http_code, error_message, retried = download_pdf_to_path(
            source_url=source_url,
            target_path=target_path,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
        )
        if download_status == "downloaded":
            success_count += 1
            downloaded_rows.append(
                {
                    "candidate_id": candidate_id,
                    "card_id": card_id,
                    "target_path": str(target_path),
                    "source_url": source_url,
                    "retrieved_at": now_iso,
                }
            )
        else:
            failed_count += 1
            errors.append(
                {
                    "source": "download_failed",
                    "message": f"{candidate_id}: {error_message or 'download failed'}",
                }
            )

        download_log_rows.append(
            {
                "candidate_id": candidate_id,
                "card_id": card_id,
                "doi": doi_value,
                "arxiv_id": arxiv_id_value,
                "source_url": source_url,
                "target_path": str(target_path),
                "status": download_status,
                "http_code": "" if http_code is None else int(http_code),
                "error": error_message,
                "retried": retried,
                "retrieved_at": now_iso,
            }
        )

    if unique_include_rows and (failed_count > 0 or no_url_count > 0):
        status = "failed"
    if not unique_include_rows and status == "ok":
        status = "failed"
        errors.append({"source": "included_candidates_csv", "message": "No included candidates found."})

    try:
        write_csv_rows(download_log_path, FULLTEXT_FETCH_LOG_COLUMNS, download_log_rows)
        write_csv_rows(downloaded_index_path, DOWNLOADED_FULLTEXT_COLUMNS, downloaded_rows)
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "included_candidates_csv": str(included_path),
            "cards_jsonl": str(cards_path),
            "records_jsonl": str(records_path),
            "library_dir": str(library_dir),
            "max_retries": max_retries,
            "timeout_seconds": timeout_seconds,
        },
        "status": status,
        "outputs": {
            "download_log_csv": str(download_log_path),
            "downloaded_index_csv": str(downloaded_index_path),
            "attempted_count": attempted_count,
            "success_count": success_count,
            "exists_count": exists_count,
            "failed_count": failed_count,
            "no_url_count": no_url_count,
        },
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "fetch_fulltext_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "fetch_fulltext_completed",
        {
            "run_id": run_id,
            "status": status,
            "attempted_count": attempted_count,
            "success_count": success_count,
            "exists_count": exists_count,
            "failed_count": failed_count,
            "no_url_count": no_url_count,
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "attempted_count": attempted_count,
                "success_count": success_count,
                "exists_count": exists_count,
                "failed_count": failed_count,
                "no_url_count": no_url_count,
                "outputs": {
                    "download_log_csv": str(download_log_path),
                    "downloaded_index_csv": str(downloaded_index_path),
                },
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def latex_escape(value: str) -> str:
    escaped = value.replace("\\", "\\textbackslash{}")
    escaped = escaped.replace("&", "\\&")
    escaped = escaped.replace("%", "\\%")
    escaped = escaped.replace("$", "\\$")
    escaped = escaped.replace("#", "\\#")
    escaped = escaped.replace("_", "\\_")
    escaped = escaped.replace("{", "\\{")
    escaped = escaped.replace("}", "\\}")
    escaped = escaped.replace("~", "\\textasciitilde{}")
    escaped = escaped.replace("^", "\\textasciicircum{}")
    return escaped


def slugify_text(value: str, default: str = "section", max_length: int = 48) -> str:
    compact = compact_whitespace(value).lower()
    tokens = re.findall(r"[a-z0-9]+", compact)
    if not tokens:
        return default
    slug = "-".join(tokens)
    if len(slug) > max_length:
        slug = slug[:max_length].rstrip("-")
    return slug or default


def stable_citation_key(candidate_id: str, record: dict[str, Any] | None) -> str:
    if isinstance(record, dict):
        for key in ("paper_id", "citation_key"):
            candidate = safe_filename_component(str(record.get(key) or ""))
            if candidate:
                return candidate
    fallback = safe_filename_component(candidate_id)
    return fallback or "ref_unknown"


def parse_bib_entries_by_key(raw_bib: str) -> dict[str, str]:
    entry_pattern = re.compile(r"@\w+\s*\{\s*([^,]+)\s*,.*?\n\}", flags=re.DOTALL)
    entries: dict[str, str] = {}
    for match in entry_pattern.finditer(raw_bib):
        key = compact_whitespace(match.group(1))
        if not key:
            continue
        entries[key] = match.group(0).strip() + "\n"
    return entries


def parse_bib_entry_metadata(raw_bib: str) -> dict[str, dict[str, Any]]:
    entry_pattern = re.compile(r"@(\w+)\s*\{\s*([^,]+)\s*,(.*?)\n\}", flags=re.DOTALL)
    field_pattern = re.compile(r"([A-Za-z][A-Za-z0-9_-]*)\s*=")
    metadata: dict[str, dict[str, Any]] = {}
    for match in entry_pattern.finditer(raw_bib):
        entry_type = compact_whitespace(match.group(1)).lower()
        key = compact_whitespace(match.group(2))
        body = match.group(3)
        if not key:
            continue
        fields = sorted({compact_whitespace(name).lower() for name in field_pattern.findall(body)})
        metadata[key] = {
            "entry_type": entry_type,
            "fields": fields,
            "raw": match.group(0).strip() + "\n",
        }
    return metadata


def extract_citation_keys(text: str) -> list[str]:
    keys: list[str] = []
    for match in re.finditer(r"\\cite\w*\{([^}]+)\}", text):
        chunk = match.group(1)
        for token in chunk.split(","):
            key = compact_whitespace(token)
            if key:
                keys.append(key)
    return deduplicate_preserve_order(keys)


def extract_text_lines_for_assertion(raw_lines: list[str]) -> list[str]:
    extracted: list[str] = []
    for raw in raw_lines:
        line = compact_whitespace(raw)
        if not line:
            continue
        if line.startswith("\\paragraph{") or line.startswith("\\section{") or line.startswith("\\label{"):
            continue
        line = re.sub(r"\\[A-Za-z]+\*?\{([^}]*)\}", r"\1", line)
        line = re.sub(r"\\[A-Za-z]+\*?", " ", line)
        line = line.replace("{", " ").replace("}", " ")
        line = compact_whitespace(line)
        if line:
            extracted.append(line)
    return extracted


def has_assertion_keyword(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in ASSERTION_KEYWORDS)


def extract_section_blocks(section_path: Path) -> tuple[str, list[dict[str, Any]]]:
    lines = section_path.read_text(encoding="utf-8").splitlines()
    section_title = ""
    for line in lines:
        match = re.search(r"\\section\{([^}]*)\}", line)
        if match:
            section_title = compact_whitespace(match.group(1))
            break

    blocks: list[dict[str, Any]] = []
    current_lines: list[str] = []
    start_line = 1
    block_index = 0

    def flush_block(end_line: int) -> None:
        nonlocal block_index
        if not current_lines:
            return
        block_index += 1
        joined = "\n".join(current_lines)
        claim_text = ""
        for row in current_lines:
            claim_match = re.search(r"\\textbf\{Claim\.\}\s*(.+?)(?:\\\\|$)", row)
            if claim_match:
                claim_text = compact_whitespace(claim_match.group(1))
                break
        plain_lines = extract_text_lines_for_assertion(current_lines)
        if not claim_text:
            for plain in plain_lines:
                if has_assertion_keyword(plain):
                    claim_text = plain
                    break
        if not claim_text and plain_lines:
            claim_text = plain_lines[0]

        is_important = bool(claim_text and (has_assertion_keyword(claim_text) or "claim." in joined.lower()))
        if not is_important and claim_text:
            is_important = len(claim_text) >= 20

        blocks.append(
            {
                "block_id": f"B{block_index:03d}",
                "line_start": start_line,
                "line_end": end_line,
                "claim_text": claim_text,
                "citation_keys": extract_citation_keys(joined),
                "raw_text": joined,
                "is_important": is_important,
            }
        )

    for idx, line in enumerate(lines, start=1):
        if re.match(r"\\paragraph\{", compact_whitespace(line)) and current_lines:
            flush_block(idx - 1)
            current_lines = []
            start_line = idx
        current_lines.append(line)
    if current_lines:
        flush_block(len(lines))

    return section_title, blocks


def apply_audit_overrides(blocks: list[dict[str, Any]], overrides: dict[str, Any]) -> list[dict[str, Any]]:
    force_patterns = [compact_whitespace(str(item)).lower() for item in (overrides.get("force_important") or [])]
    ignore_patterns = [compact_whitespace(str(item)).lower() for item in (overrides.get("ignore") or [])]
    result: list[dict[str, Any]] = []
    for block in blocks:
        claim_text = compact_whitespace(block.get("claim_text")).lower()
        raw_text = compact_whitespace(block.get("raw_text")).lower()
        combined = f"{claim_text} {raw_text}".strip()
        if any(pattern and pattern in combined for pattern in ignore_patterns):
            continue
        if any(pattern and pattern in combined for pattern in force_patterns):
            block["is_important"] = True
        result.append(block)
    return result


def score_from_findings(findings: list[dict[str, Any]]) -> tuple[int, str]:
    deductions = {
        "coverage": 20,
        "support": 15,
        "bib_fields": 10,
        "text_only": 10,
        "bib_unused": 5,
    }
    score = 100
    for finding in findings:
        score -= deductions.get(compact_whitespace(finding.get("category")), 5)
    score = max(0, min(100, score))
    if score >= 85:
        risk = "low"
    elif score >= 60:
        risk = "medium"
    else:
        risk = "high"
    return score, risk


def render_citation_audit_report(
    overall_score: int,
    overall_risk: str,
    section_scores: list[dict[str, Any]],
    findings: list[dict[str, Any]],
) -> str:
    lines = [
        "# Citation Audit Report",
        "",
        f"- Overall Score: {overall_score}",
        f"- Overall Risk: {overall_risk}",
        f"- Findings Count: {len(findings)}",
        "",
        "## Section Scores",
    ]
    if not section_scores:
        lines.append("- No section score available.")
    else:
        for row in section_scores:
            lines.append(
                f"- {row['section_id']} {row['section_title']}: score={row['score']}, risk={row['risk_level']}, findings={row['finding_count']}"
            )
    lines.append("")
    lines.append("## Findings")
    if not findings:
        lines.append("- No findings.")
    else:
        for finding in findings:
            raw_citation_keys = finding.get("citation_keys")
            if isinstance(raw_citation_keys, list):
                citation_keys = ", ".join([compact_whitespace(str(item)) for item in raw_citation_keys if compact_whitespace(str(item))])
            else:
                citation_keys = compact_whitespace(str(raw_citation_keys or ""))
            cite_text = f" | cites: {citation_keys}" if citation_keys else ""
            lines.append(
                f"- [{finding['severity']}] {finding['category']} @ {finding['section_id']} ({finding['file_path']}): {finding['message']}{cite_text}"
            )
    lines.append("")
    return "\n".join(lines)


def score_section_citation_findings(findings: list[dict[str, Any]]) -> tuple[int, str]:
    score = 100
    for finding in findings:
        finding_type = compact_whitespace(finding.get("type"))
        score -= SECTION_AUDIT_DEDUCTIONS.get(finding_type, 5)
    score = max(0, min(100, score))
    if score >= 85:
        risk_level = "low"
    elif score >= 60:
        risk_level = "medium"
    else:
        risk_level = "high"
    return score, risk_level


def render_section_citation_audit_report(
    section_stem: str,
    section_title: str,
    summary: dict[str, Any],
    findings: list[dict[str, Any]],
    citation_sets: dict[str, Any],
) -> str:
    lines = [
        f"# Section Citation Audit: {section_stem}",
        "",
        f"- Section Title: {section_title}",
        f"- Key Claims: {summary.get('key_claim_count', 0)}",
        f"- Score: {summary.get('score', 0)}",
        f"- Risk Level: {summary.get('risk_level', 'unknown')}",
        "",
        "## Finding Counts",
    ]
    finding_counts = summary.get("finding_counts_by_type", {})
    if isinstance(finding_counts, dict) and finding_counts:
        for finding_type in SECTION_AUDIT_FINDING_TYPES:
            count = int(finding_counts.get(finding_type, 0))
            lines.append(f"- {finding_type}: {count}")
    else:
        lines.append("- No findings.")

    lines.extend(["", "## Citation Sets"])
    for key in ("cited_keys", "supported_keys", "cited_not_used_keys"):
        raw_values = citation_sets.get(key, []) if isinstance(citation_sets, dict) else []
        values = [compact_whitespace(str(item)) for item in raw_values if compact_whitespace(str(item))]
        lines.append(f"- {key}: {', '.join(values) if values else '(none)'}")

    lines.extend(["", "## Findings"])
    if not findings:
        lines.append("- No findings.")
    else:
        for finding in findings:
            cites = ", ".join([compact_whitespace(str(item)) for item in (finding.get("citation_keys") or []) if compact_whitespace(str(item))])
            cite_text = f" | cites: {cites}" if cites else ""
            lines.append(
                f"- [{finding.get('severity')}] {finding.get('type')} ({finding.get('paragraph_id') or 'N/A'}): {finding.get('message')}{cite_text}"
            )
    lines.append("")
    return "\n".join(lines)


def section_sort_key(path: Path) -> tuple[int, str]:
    match = re.match(r"sec_(\d+)_", path.stem)
    if match:
        try:
            return int(match.group(1)), path.stem
        except Exception:
            pass
    return 999999, path.stem


def normalize_pipeline_stage_name(stage_name: str) -> str:
    cleaned = compact_whitespace(stage_name).lower()
    return PIPELINE_STAGE_ALIASES.get(cleaned, cleaned)


def list_pipeline_stages(with_fulltext: bool) -> list[str]:
    if with_fulltext:
        return list(PIPELINE_STAGE_ORDER)
    return [stage for stage in PIPELINE_STAGE_ORDER if stage != "fetch-fulltext"]


def snapshot_run_manifests(runs_dir: Path) -> dict[str, Path]:
    if not runs_dir.exists():
        return {}
    return {str(path.resolve()): path.resolve() for path in runs_dir.glob("run_*/manifest.json") if path.is_file()}


def resolve_new_manifest_path(before: dict[str, Path], after: dict[str, Path]) -> Path | None:
    new_paths = [after[key] for key in sorted(after.keys()) if key not in before]
    if not new_paths:
        return None
    return sorted(new_paths, key=lambda path: (path.parent.name, str(path)))[-1]


def load_manifest_payload(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def pipeline_stage_handler(stage_name: str):
    handlers = {
        "search-candidates": search_candidates,
        "cardify-candidates": cardify_candidates,
        "screen-candidates": screen_candidates,
        "fetch-fulltext": fetch_fulltext,
        "outline-from-evidence": outline_from_evidence,
        "generate-paragraph-plans": generate_paragraph_plans,
        "assemble-evidence-packets": assemble_evidence_packets,
        "generate-section-drafts": generate_section_drafts,
        "revise-section-consistency": revise_section_consistency,
        "section-citation-audit": section_citation_audit,
        "section-release-gate": section_release_gate,
        "generate-cross-section-bridges": generate_cross_section_bridges,
        "export-claim-trace-matrix": export_claim_trace_matrix,
        "ground-figure-table-links": ground_figure_table_links,
        "generate-latex-draft": generate_latex_draft,
        "assemble-full-draft": assemble_full_draft,
        "citation-audit": citation_audit,
        "latex-build-qa": latex_build_qa,
    }
    return handlers[stage_name]


def build_pipeline_stage_args(
    stage_name: str,
    run_args: argparse.Namespace,
    paths: RepositoryPaths,
    *,
    section_tex: str = "",
    section_stem: str = "",
    section_consistency_report_json: str = "",
    section_audit_json: str = "",
) -> argparse.Namespace:
    base_payload: dict[str, Any] = {
        "base_dir": str(paths.root),
    }

    if stage_name == "search-candidates":
        base_payload.update(
            {
                "topic_frame_json": run_args.topic_frame_json,
                "max_queries": 12,
                "rows_per_source": 20,
                "backend_order": ",".join(DEFAULT_BACKEND_ORDER),
            }
        )
    elif stage_name == "cardify-candidates":
        base_payload.update(
            {
                "candidates_csv": "",
                "records_jsonl": "",
                "cards_jsonl": "",
                "overwrite_existing": False,
            }
        )
    elif stage_name == "screen-candidates":
        base_payload.update(
            {
                "candidates_csv": "",
                "cards_jsonl": "",
                "screening_decisions_csv": "",
                "included_candidates_csv": "",
                "allow_auto_minimal_cards": False,
            }
        )
    elif stage_name == "fetch-fulltext":
        base_payload.update(
            {
                "included_candidates_csv": "",
                "cards_jsonl": "",
                "records_jsonl": "",
                "download_log_csv": "",
                "downloaded_index_csv": "",
                "library_dir": "",
                "max_retries": 1,
                "timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            }
        )
    elif stage_name == "outline-from-evidence":
        base_payload.update(
            {
                "topic_frame_json": run_args.topic_frame_json,
                "cards_jsonl": "",
                "included_candidates_csv": "",
                "argument_graph_json": "",
                "claims_jsonl": "",
                "outline_markdown": "",
            }
        )
    elif stage_name == "generate-paragraph-plans":
        base_payload.update(
            {
                "outline_markdown": "",
                "argument_graph_json": "",
                "claims_jsonl": "",
                "paragraph_plans_dir": "",
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "assemble-evidence-packets":
        base_payload.update(
            {
                "paragraph_plans_dir": "",
                "claims_jsonl": "",
                "cards_jsonl": "",
                "included_candidates_csv": "",
                "bib_path": "",
                "packet_overrides_json": "",
                "evidence_packets_dir": "",
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "generate-section-drafts":
        base_payload.update(
            {
                "paragraph_plans_dir": "",
                "evidence_packets_dir": "",
                "section_roles_json": "",
                "latex_sections_dir": "",
                "latex_template_path": "",
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "revise-section-consistency":
        base_payload.update(
            {
                "latex_sections_dir": "",
                "argument_graph_json": "",
                "section_drafts_dir": "",
                "consistency_report_json": "",
                "strictness": run_args.strictness,
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "section-citation-audit":
        base_payload.update(
            {
                "section_tex": section_tex,
                "bib_path": "",
                "evidence_packets_dir": "",
                "claims_jsonl": "",
                "records_jsonl": "",
                "section_drafts_dir": "",
                "audit_output_dir": "",
                "strictness": run_args.strictness,
            }
        )
    elif stage_name == "section-release-gate":
        base_payload.update(
            {
                "section_stem": section_stem,
                "section_consistency_report_json": section_consistency_report_json,
                "section_audit_json": section_audit_json,
                "gate_output_json": "",
                "gate_fixlist_md": "",
                "strictness": run_args.strictness,
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "generate-cross-section-bridges":
        base_payload.update(
            {
                "latex_sections_dir": "",
                "argument_graph_json": "",
                "bridge_plan_json": "",
                "bridges_tex": "",
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "export-claim-trace-matrix":
        base_payload.update(
            {
                "claims_jsonl": "",
                "paragraph_plans_dir": "",
                "evidence_packets_dir": "",
                "bib_path": "",
                "claim_trace_matrix_csv": "",
                "claim_trace_matrix_json": "",
                "strictness": run_args.strictness,
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "ground-figure-table-links":
        base_payload.update(
            {
                "latex_sections_dir": "",
                "figures_dir": "",
                "tables_dir": "",
                "evidence_packets_dir": "",
                "figure_table_grounding_md": "",
                "figure_table_manifest_json": "",
                "strictness": run_args.strictness,
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "generate-latex-draft":
        base_payload.update(
            {
                "argument_graph_json": "",
                "claims_jsonl": "",
                "included_candidates_csv": "",
                "records_jsonl": "",
                "refs_bib": "",
                "latex_dir": "",
                "figures_dir": "",
                "tables_dir": "",
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "assemble-full-draft":
        base_payload.update(
            {
                "latex_sections_dir": "",
                "section_audit_dir": "",
                "bib_path": "",
                "abstract_template": "",
                "conclusion_template": "",
                "output_main_tex": "",
                "output_full_draft_tex": "",
                "full_draft_review_md": "",
                "strictness": run_args.strictness,
                "overwrite": bool(run_args.overwrite),
            }
        )
    elif stage_name == "citation-audit":
        base_payload.update(
            {
                "latex_dir": "",
                "main_tex": "",
                "outline_tex": "",
                "bib_path": "",
                "argument_graph_json": "",
                "claims_jsonl": "",
                "records_jsonl": "",
                "included_candidates_csv": "",
                "audit_overrides_json": "",
                "audit_output_dir": "",
                "strictness": run_args.strictness,
            }
        )
    elif stage_name == "latex-build-qa":
        base_payload.update(
            {
                "target": "full",
                "main_tex": "",
                "full_draft_tex": "",
                "bib_path": "",
                "latex_build_report_md": "",
                "latex_build_log_txt": "",
                "run_compiler": bool(run_args.run_compiler),
                "strictness": run_args.strictness,
                "overwrite": bool(run_args.overwrite),
            }
        )

    return argparse.Namespace(**base_payload)


def run_pipeline_stage(stage_name: str, stage_args: argparse.Namespace, paths: RepositoryPaths) -> dict[str, Any]:
    before = snapshot_run_manifests(paths.runs_dir)
    started_at = local_now()
    exit_code = 0
    exception_message = ""
    try:
        exit_code = int(pipeline_stage_handler(stage_name)(stage_args))
    except Exception as exc:
        exit_code = 1
        exception_message = str(exc)
    finished_at = local_now()
    after = snapshot_run_manifests(paths.runs_dir)
    manifest_path = resolve_new_manifest_path(before, after)
    manifest_payload = load_manifest_payload(manifest_path)
    manifest_status = compact_whitespace(manifest_payload.get("status")).lower()

    stage_status = "ok"
    if exception_message:
        stage_status = "failed"
    elif exit_code != 0:
        stage_status = "failed"
    elif manifest_status == "failed":
        stage_status = "failed"

    return {
        "stage": stage_name,
        "status": stage_status,
        "exit_code": exit_code,
        "manifest": str(manifest_path) if manifest_path else "",
        "manifest_status": manifest_status,
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "error": exception_message,
        "outputs": manifest_payload.get("outputs", {}) if isinstance(manifest_payload, dict) else {},
    }


def render_pipeline_run_summary_markdown(
    selected_stages: list[str],
    stage_results: list[dict[str, Any]],
    overall_status: str,
    failed_stage: dict[str, Any] | None,
) -> str:
    lines = [
        "# Pipeline Run Summary",
        "",
        f"- Status: {overall_status}",
        f"- Stage Count: {len(selected_stages)}",
    ]
    if failed_stage:
        lines.append(f"- Failure Point: {failed_stage.get('stage')} ({failed_stage.get('target') or 'single'})")
    lines.extend(["", "## Stage Results"])
    if not stage_results:
        lines.append("- No stage executed.")
    else:
        for row in stage_results:
            target = compact_whitespace(row.get("target"))
            target_text = f" [{target}]" if target else ""
            lines.append(
                f"- {row.get('stage')}{target_text}: status={row.get('status')} exit={row.get('exit_code')} "
                f"manifest={row.get('manifest') or 'N/A'}"
            )
            if row.get("error"):
                lines.append(f"  error: {row.get('error')}")
    lines.append("")
    return "\n".join(lines)


def extract_label_keys(text: str) -> list[str]:
    pattern = re.compile(r"\\label\{([^}]*)\}")
    keys: list[str] = []
    for match in pattern.finditer(text):
        value = compact_whitespace(match.group(1))
        if value:
            keys.append(value)
    return deduplicate_preserve_order(keys)


def extract_reference_keys(text: str) -> list[str]:
    pattern = re.compile(r"\\(?:ref|autoref|cref|Cref|eqref)\{([^}]*)\}")
    keys: list[str] = []
    for match in pattern.finditer(text):
        for item in match.group(1).split(","):
            value = compact_whitespace(item)
            if value:
                keys.append(value)
    return deduplicate_preserve_order(keys)


def score_full_draft_review(
    unresolved_high_risk_count: int,
    crossref_issue_count: int,
    figure_table_ref_issue_count: int,
    citation_key_missing_count: int,
) -> tuple[int, str]:
    score = 100
    score -= max(0, unresolved_high_risk_count) * FULL_DRAFT_SCORE_DEDUCTIONS["unresolved_high_risk"]
    score -= max(0, crossref_issue_count) * FULL_DRAFT_SCORE_DEDUCTIONS["crossref_issue"]
    score -= max(0, figure_table_ref_issue_count) * FULL_DRAFT_SCORE_DEDUCTIONS["figure_table_ref_issue"]
    score -= max(0, citation_key_missing_count) * FULL_DRAFT_SCORE_DEDUCTIONS["citation_key_missing"]
    score = max(0, min(100, score))
    if score >= 85:
        risk_level = "low"
    elif score >= 60:
        risk_level = "medium"
    else:
        risk_level = "high"
    return score, risk_level


def render_full_draft_review_report(
    score: int,
    risk_level: str,
    section_audit_rows: list[dict[str, Any]],
    unresolved_high_risk_count: int,
    terminology_events: list[dict[str, str]],
    crossref_missing_keys: list[str],
    missing_figure_table_refs: list[str],
    unused_figure_table_labels: list[str],
    citation_key_missing_keys: list[str],
    warnings: list[dict[str, str]],
) -> str:
    lines = [
        "# Full Draft Review",
        "",
        f"- Score: {score}",
        f"- Risk Level: {risk_level}",
        f"- Section Audit Count: {len(section_audit_rows)}",
        f"- Unresolved High-Risk Findings: {unresolved_high_risk_count}",
        f"- Terminology Fixes: {len(terminology_events)}",
        f"- Cross-Reference Issues: {len(crossref_missing_keys)}",
        f"- Figure/Table Reference Issues: {len(missing_figure_table_refs) + len(unused_figure_table_labels)}",
        f"- Citation Keys Missing in Bib: {len(citation_key_missing_keys)}",
        "",
        "## Section Audit Summary",
    ]
    if not section_audit_rows:
        lines.append("- No section audit payload found.")
    else:
        for row in section_audit_rows:
            lines.append(
                f"- {row.get('section_stem')} ({row.get('section_title')}): "
                f"score={row.get('score')}, risk={row.get('risk_level')}, "
                f"high_severity_findings={row.get('high_severity_findings')}"
            )

    lines.extend(["", "## Terminology Normalization"])
    if not terminology_events:
        lines.append("- No terminology normalization applied.")
    else:
        for event in terminology_events[:30]:
            lines.append(f"- `{event.get('replaced', '')}` -> `{event.get('primary', '')}`")
        if len(terminology_events) > 30:
            lines.append(f"- ... ({len(terminology_events) - 30} more)")

    lines.extend(["", "## Cross-Reference Findings"])
    if crossref_missing_keys:
        lines.append(f"- Missing labels for refs: {', '.join(crossref_missing_keys)}")
    else:
        lines.append("- No missing cross-reference labels.")

    lines.extend(["", "## Figure/Table Reference Findings"])
    if missing_figure_table_refs:
        lines.append(f"- Figure/Table refs missing labels: {', '.join(missing_figure_table_refs)}")
    else:
        lines.append("- No missing figure/table labels for used refs.")
    if unused_figure_table_labels:
        lines.append(f"- Unused figure/table labels: {', '.join(unused_figure_table_labels)}")
    else:
        lines.append("- No unused figure/table labels.")

    lines.extend(["", "## Citation Key Findings"])
    if citation_key_missing_keys:
        lines.append(f"- Citation keys used in text but absent in bib: {', '.join(citation_key_missing_keys)}")
    else:
        lines.append("- No citation key mismatch against bibliography.")

    if warnings:
        lines.extend(["", "## Warnings"])
        for warning in warnings:
            lines.append(f"- [{warning.get('source', 'unknown')}] {warning.get('message', '')}")

    lines.append("")
    return "\n".join(lines)


def resolve_latest_run_file(runs_dir: Path, current_run_dir: Path, file_name: str) -> Path | None:
    run_dirs = sorted([path for path in runs_dir.glob("run_*") if path.is_dir()], reverse=True)
    for run_dir in run_dirs:
        if run_dir == current_run_dir:
            continue
        candidate = run_dir / file_name
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def collect_packet_files(evidence_packets_dir: Path) -> list[Path]:
    if not evidence_packets_dir.exists():
        return []
    return sorted([path for path in evidence_packets_dir.rglob("*.json") if path.is_file()])


def normalize_issue_severity(value: Any) -> str:
    lowered = compact_whitespace(str(value or "")).lower()
    if lowered in {"high", "medium", "low"}:
        return lowered
    return "low"


def collect_paragraph_blocks_with_ids(section_text: str) -> list[dict[str, str]]:
    preamble, paragraphs = parse_section_paragraph_units(section_text)
    _ = preamble
    rows: list[dict[str, str]] = []
    for index, paragraph_lines in enumerate(paragraphs, start=1):
        paragraph_id = f"P{index:03d}"
        paragraph_title = f"Paragraph {index}"
        first = paragraph_lines[0] if paragraph_lines else ""
        match = re.match(r"\s*\\paragraph\{([^}]*)\}", first)
        if match:
            paragraph_title = compact_whitespace(match.group(1)) or paragraph_title
            normalized = slugify_text(paragraph_title, default=paragraph_id)
            paragraph_id = normalized.upper()
        rows.append(
            {
                "paragraph_id": paragraph_id,
                "paragraph_title": paragraph_title,
                "text": "\n".join(paragraph_lines),
            }
        )
    return rows


def load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_section_draft_paragraphs(section_drafts_dir: Path, section_stem: str) -> list[dict[str, Any]]:
    path = section_drafts_dir / f"{section_stem}.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    paragraphs = payload.get("paragraphs") if isinstance(payload, dict) else []
    if not isinstance(paragraphs, list):
        return []
    return [row for row in paragraphs if isinstance(row, dict)]


def parse_packet_strength(packet: dict[str, Any]) -> tuple[str, float]:
    strength_payload = packet.get("strength") if isinstance(packet.get("strength"), dict) else {}
    label = compact_whitespace(strength_payload.get("label")).lower()
    score_raw = strength_payload.get("score", 0)
    try:
        score = float(score_raw)
    except Exception:
        score = 0.0
    return label, score


def is_strong_claim_text(text: str) -> bool:
    lowered = compact_whitespace(text).lower()
    return any(keyword in lowered for keyword in SECTION_AUDIT_STRONG_CLAIM_KEYWORDS)


def is_overgeneralization(claim_text: str, raw_text: str) -> bool:
    claim_lower = compact_whitespace(claim_text).lower()
    raw_lower = compact_whitespace(raw_text).lower()
    has_absolute = any(term in claim_lower for term in SECTION_AUDIT_ABSOLUTE_TERMS)
    if not has_absolute:
        return False
    has_boundary = any(term in raw_lower for term in SECTION_AUDIT_BOUNDARY_TERMS)
    return not has_boundary


def write_text_if_allowed(path: Path, content: str, overwrite: bool) -> str:
    if path.exists() and not overwrite:
        return "skipped"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return "written"


def text_contains_keywords(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = compact_whitespace(text).lower()
    return any(keyword in lowered for keyword in keywords)


def parse_sections_from_outline_markdown(outline_path: Path) -> list[dict[str, Any]]:
    if not outline_path.exists():
        return []
    sections: list[dict[str, Any]] = []
    lines = outline_path.read_text(encoding="utf-8").splitlines()
    for line in lines:
        normalized = compact_whitespace(line)
        if not normalized.startswith("## "):
            continue
        title = compact_whitespace(normalized[3:])
        if not title:
            continue
        sections.append(
            {
                "section_id": f"SEC{len(sections) + 1:03d}",
                "subquestion_id": f"SQ{len(sections) + 1:03d}",
                "section_title": title,
                "paragraph_points": [],
            }
        )
    return sections


def claim_sort_key(row: dict[str, Any]) -> tuple[float, str, str]:
    confidence_raw = row.get("confidence", 0)
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = 0.0
    claim_id = compact_whitespace(row.get("claim_id"))
    return (-confidence, claim_id, compact_whitespace(row.get("candidate_id")))


def pick_first_claim(
    claims: list[dict[str, Any]],
    predicate: Any,
) -> dict[str, Any] | None:
    for claim in claims:
        try:
            if predicate(claim):
                return claim
        except Exception:
            continue
    return None


def build_paragraph_plan_row(
    paragraph_id: str,
    paragraph_no: int,
    paragraph_type: str,
    purpose: str,
    core_claim: dict[str, Any] | None,
    required_evidence_ids: list[str],
    claims_by_id: dict[str, dict[str, Any]],
    section_id: str,
    section_title: str,
    subquestion_id: str,
    claim_text_override: str = "",
) -> dict[str, Any]:
    claim_id = compact_whitespace(core_claim.get("claim_id")) if core_claim else ""
    claim_text = claim_text_override or (compact_whitespace(core_claim.get("claim_text")) if core_claim else "")

    filtered_ids: list[str] = []
    for claim_ref in required_evidence_ids:
        normalized = compact_whitespace(claim_ref)
        if not normalized or normalized not in claims_by_id:
            continue
        filtered_ids.append(normalized)
    if claim_id and claim_id in claims_by_id:
        filtered_ids.insert(0, claim_id)
    filtered_ids = deduplicate_preserve_order(filtered_ids)

    supporting_candidate_ids: list[str] = []
    for evidence_id in filtered_ids:
        claim_row = claims_by_id.get(evidence_id)
        if not claim_row:
            continue
        candidate_id = compact_whitespace(claim_row.get("candidate_id"))
        if candidate_id:
            supporting_candidate_ids.append(candidate_id)
    supporting_candidate_ids = deduplicate_preserve_order(supporting_candidate_ids)

    return {
        "paragraph_id": paragraph_id,
        "paragraph_no": paragraph_no,
        "paragraph_type": paragraph_type,
        "purpose": purpose,
        "core_claim_id": claim_id,
        "core_claim_text": claim_text,
        "required_evidence_ids": filtered_ids,
        "supporting_candidate_ids": supporting_candidate_ids,
        "section_id": section_id,
        "section_title": section_title,
        "subquestion_id": subquestion_id,
    }


def build_section_paragraph_plans(
    section: dict[str, Any],
    ordered_claims: list[dict[str, Any]],
    claims_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    section_id = compact_whitespace(section.get("section_id")) or "SEC000"
    section_title = compact_whitespace(section.get("section_title")) or "Untitled Section"
    subquestion_id = compact_whitespace(section.get("subquestion_id")) or "SQ000"
    paragraph_points = section.get("paragraph_points")
    if not isinstance(paragraph_points, list):
        paragraph_points = []

    claim_index: dict[str, int] = {}
    for index, claim in enumerate(ordered_claims):
        claim_id = compact_whitespace(claim.get("claim_id"))
        if claim_id:
            claim_index[claim_id] = index

    def sort_evidence_ids(values: list[str]) -> list[str]:
        deduped = deduplicate_preserve_order([compact_whitespace(item) for item in values if compact_whitespace(item)])
        return sorted(deduped, key=lambda claim_id: (claim_index.get(claim_id, 9999), claim_id))

    non_limitation_claims = [claim for claim in ordered_claims if compact_whitespace(claim.get("claim_type")) != "limitation"]
    limitation_claims = [claim for claim in ordered_claims if compact_whitespace(claim.get("claim_type")) == "limitation"]
    all_claim_ids = [compact_whitespace(claim.get("claim_id")) for claim in ordered_claims if compact_whitespace(claim.get("claim_id"))]

    definition_claim = pick_first_claim(
        ordered_claims,
        lambda row: text_contains_keywords(
            f"{compact_whitespace(row.get('claim_text'))} {compact_whitespace(row.get('evidence_snippet'))}",
            DEFINITION_KEYWORDS,
        ),
    )
    method_claim = pick_first_claim(
        ordered_claims,
        lambda row: text_contains_keywords(
            f"{compact_whitespace(row.get('claim_text'))} {compact_whitespace(row.get('evidence_snippet'))}",
            METHOD_KEYWORDS,
        ),
    )
    mechanism_claim = pick_first_claim(
        ordered_claims, lambda row: compact_whitespace(row.get("claim_type")) == "mechanism"
    )
    comparison_claim = pick_first_claim(
        ordered_claims, lambda row: compact_whitespace(row.get("claim_type")) == "comparison"
    )

    limitation_text = ""
    for point in paragraph_points:
        raw = compact_whitespace(point.get("limitation_or_boundary"))
        if raw and raw.lower() not in {"todo", "待补充局限证据。"}:
            limitation_text = raw
            break

    chosen_types: set[str] = set()
    rows: list[dict[str, Any]] = []

    def append_row(
        paragraph_type: str,
        purpose: str,
        core_claim: dict[str, Any] | None,
        evidence_ids: list[str],
        claim_text_override: str = "",
    ) -> None:
        if paragraph_type in chosen_types:
            return
        row = build_paragraph_plan_row(
            paragraph_id=f"{section_id}-P{len(rows) + 1:02d}",
            paragraph_no=len(rows) + 1,
            paragraph_type=paragraph_type,
            purpose=purpose,
            core_claim=core_claim,
            required_evidence_ids=sort_evidence_ids(evidence_ids),
            claims_by_id=claims_by_id,
            section_id=section_id,
            section_title=section_title,
            subquestion_id=subquestion_id,
            claim_text_override=claim_text_override,
        )
        rows.append(row)
        chosen_types.add(paragraph_type)

    background_core = non_limitation_claims[0] if non_limitation_claims else (ordered_claims[0] if ordered_claims else None)
    append_row(
        paragraph_type="背景段",
        purpose="交代本节子问题与证据边界，建立阅读上下文。",
        core_claim=background_core,
        evidence_ids=all_claim_ids[:2],
    )

    if definition_claim:
        append_row(
            paragraph_type="定义段",
            purpose="界定关键概念、术语或判定标准。",
            core_claim=definition_claim,
            evidence_ids=[compact_whitespace(definition_claim.get("claim_id"))],
        )

    if method_claim:
        append_row(
            paragraph_type="方法段",
            purpose="说明支撑结论的方法来源与适用条件。",
            core_claim=method_claim,
            evidence_ids=[compact_whitespace(method_claim.get("claim_id"))],
        )

    if mechanism_claim:
        append_row(
            paragraph_type="机制解释段",
            purpose="解释反应机理与因果链条。",
            core_claim=mechanism_claim,
            evidence_ids=[compact_whitespace(mechanism_claim.get("claim_id"))],
        )

    if comparison_claim:
        append_row(
            paragraph_type="比较段",
            purpose="比较不同证据或方案的差异与优劣。",
            core_claim=comparison_claim,
            evidence_ids=[compact_whitespace(comparison_claim.get("claim_id"))],
        )

    if not any(row["paragraph_type"] in CORE_PARAGRAPH_TYPES for row in rows):
        fallback_core = non_limitation_claims[0] if non_limitation_claims else (ordered_claims[0] if ordered_claims else None)
        fallback_id = compact_whitespace(fallback_core.get("claim_id")) if fallback_core else ""
        append_row(
            paragraph_type="机制解释段",
            purpose="给出本节核心论证骨架，避免整节失去主论点。",
            core_claim=fallback_core,
            evidence_ids=[fallback_id] if fallback_id else [],
        )

    if limitation_claims:
        limitation_core = limitation_claims[0]
        limitation_ids = [compact_whitespace(row.get("claim_id")) for row in limitation_claims if compact_whitespace(row.get("claim_id"))]
        append_row(
            paragraph_type="争议/局限段",
            purpose="标注证据边界、争议点与不确定性。",
            core_claim=limitation_core,
            evidence_ids=limitation_ids[:3],
        )
    elif limitation_text:
        fallback_core = non_limitation_claims[0] if non_limitation_claims else (ordered_claims[0] if ordered_claims else None)
        fallback_id = compact_whitespace(fallback_core.get("claim_id")) if fallback_core else ""
        append_row(
            paragraph_type="争议/局限段",
            purpose="在缺少局限类claim时，保留边界条件与风险提示。",
            core_claim=fallback_core,
            evidence_ids=[fallback_id] if fallback_id else [],
            claim_text_override=limitation_text,
        )

    summary_core: dict[str, Any] | None = None
    for row in rows:
        if row["paragraph_type"] in CORE_PARAGRAPH_TYPES and row["core_claim_id"]:
            summary_core = claims_by_id.get(row["core_claim_id"])
            break
    if summary_core is None:
        summary_core = background_core
    summary_evidence_pool: list[str] = []
    for row in rows:
        summary_evidence_pool.extend(row["required_evidence_ids"])
    if not summary_evidence_pool:
        summary_evidence_pool = all_claim_ids[:3]
    append_row(
        paragraph_type="小结段",
        purpose="回收本节主张并衔接到下一节。",
        core_claim=summary_core,
        evidence_ids=summary_evidence_pool[:3],
    )
    return rows


def render_section_paragraph_markdown(section_payload: dict[str, Any]) -> str:
    lines = [
        f"# 段落计划：{compact_whitespace(section_payload.get('section_title'))}",
        "",
        f"- section_id: {compact_whitespace(section_payload.get('section_id'))}",
        f"- subquestion_id: {compact_whitespace(section_payload.get('subquestion_id'))}",
        f"- section_file_stem: {compact_whitespace(section_payload.get('section_file_stem'))}",
        "",
    ]
    paragraphs = section_payload.get("paragraphs")
    if not isinstance(paragraphs, list) or not paragraphs:
        lines.append("- 无可用段落计划。")
        lines.append("")
        return "\n".join(lines)

    for row in paragraphs:
        lines.extend(
            [
                f"## 段落 {row['paragraph_no']}: {row['paragraph_type']}",
                f"- paragraph_id: {row['paragraph_id']}",
                f"- 段落目的: {row['purpose']}",
                f"- 核心 claim: {row['core_claim_id']} | {row['core_claim_text']}",
                f"- 需要的证据 ID: {', '.join(row['required_evidence_ids']) if row['required_evidence_ids'] else '(none)'}",
                f"- 支撑候选: {', '.join(row['supporting_candidate_ids']) if row['supporting_candidate_ids'] else '(none)'}",
                "",
            ]
        )
    return "\n".join(lines)


def generate_paragraph_plans(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    outline_markdown_path = (
        Path(args.outline_markdown).resolve()
        if args.outline_markdown
        else (paths.root / "outline" / "generated_outline.md")
    )
    argument_graph_path = (
        Path(args.argument_graph_json).resolve()
        if args.argument_graph_json
        else (paths.root / "references" / "index" / "argument_graph.json")
    )
    claims_path = (
        Path(args.claims_jsonl).resolve()
        if args.claims_jsonl
        else (paths.root / "references" / "index" / "claims.jsonl")
    )
    paragraph_plans_dir = (
        Path(args.paragraph_plans_dir).resolve()
        if args.paragraph_plans_dir
        else (paths.root / "draft" / "paragraph_plans")
    )
    overwrite = bool(args.overwrite)

    if not argument_graph_path.exists():
        status = "failed"
        errors.append({"source": "argument_graph_json", "message": f"File not found: {argument_graph_path}"})
        argument_graph: dict[str, Any] = {}
    else:
        try:
            argument_graph = json.loads(argument_graph_path.read_text(encoding="utf-8"))
        except Exception as exc:
            argument_graph = {}
            status = "failed"
            errors.append({"source": "argument_graph_json", "message": str(exc)})

    if not claims_path.exists():
        status = "failed"
        errors.append({"source": "claims_jsonl", "message": f"File not found: {claims_path}"})
        claims_rows: list[dict[str, Any]] = []
    else:
        try:
            claims_rows = load_jsonl(claims_path)
        except Exception as exc:
            claims_rows = []
            status = "failed"
            errors.append({"source": "claims_jsonl", "message": str(exc)})

    if not claims_rows and status == "ok":
        status = "failed"
        errors.append({"source": "claims_jsonl", "message": "No claim rows found."})

    section_plan = argument_graph.get("section_plan", []) if isinstance(argument_graph, dict) else []
    if not isinstance(section_plan, list):
        section_plan = []
    section_source = "argument_graph"
    if not section_plan:
        fallback_sections = parse_sections_from_outline_markdown(outline_markdown_path)
        if fallback_sections:
            section_plan = fallback_sections
            section_source = "outline_markdown"
            warnings.append(
                {
                    "source": "outline_markdown",
                    "message": "section_plan missing in argument_graph; fallback to generated_outline headings.",
                }
            )
    if not section_plan and status == "ok":
        status = "failed"
        errors.append({"source": "section_plan", "message": "No sections available from argument_graph or outline_markdown."})

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "outline_markdown": str(outline_markdown_path),
                "argument_graph_json": str(argument_graph_path),
                "claims_jsonl": str(claims_path),
                "paragraph_plans_dir": str(paragraph_plans_dir),
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    claims_by_id: dict[str, dict[str, Any]] = {}
    claims_by_subquestion: dict[str, list[dict[str, Any]]] = {}
    for claim in claims_rows:
        claim_id = compact_whitespace(claim.get("claim_id"))
        if claim_id and claim_id not in claims_by_id:
            claims_by_id[claim_id] = claim
        subquestion_id = compact_whitespace(claim.get("subquestion_id"))
        if subquestion_id:
            claims_by_subquestion.setdefault(subquestion_id, []).append(claim)
    for subquestion_id in list(claims_by_subquestion.keys()):
        claims_by_subquestion[subquestion_id] = sorted(claims_by_subquestion[subquestion_id], key=claim_sort_key)

    paragraph_count = 0
    paragraph_type_distribution = {paragraph_type: 0 for paragraph_type in PARAGRAPH_TYPES}
    missing_claim_sections: list[str] = []
    write_results: dict[str, str] = {}
    generated_files: list[str] = []

    for index, section in enumerate(section_plan, start=1):
        section_id = compact_whitespace(section.get("section_id")) or f"SEC{index:03d}"
        section_title = compact_whitespace(section.get("section_title")) or f"Section {index}"
        subquestion_id = compact_whitespace(section.get("subquestion_id")) or f"SQ{index:03d}"
        ordered_claims = claims_by_subquestion.get(subquestion_id, [])
        if not ordered_claims:
            missing_claim_sections.append(section_id)

        section_for_plan = {
            "section_id": section_id,
            "section_title": section_title,
            "subquestion_id": subquestion_id,
            "paragraph_points": section.get("paragraph_points") if isinstance(section, dict) else [],
        }
        paragraphs = build_section_paragraph_plans(
            section=section_for_plan,
            ordered_claims=ordered_claims,
            claims_by_id=claims_by_id,
        )
        for row in paragraphs:
            paragraph_type = row["paragraph_type"]
            if paragraph_type in paragraph_type_distribution:
                paragraph_type_distribution[paragraph_type] += 1
        paragraph_count += len(paragraphs)

        slug = slugify_text(section_title, default=f"section-{index}")
        section_file_stem = f"sec_{index:03d}_{slug}"
        section_payload = {
            "section_id": section_id,
            "section_title": section_title,
            "subquestion_id": subquestion_id,
            "section_file_stem": section_file_stem,
            "paragraphs": paragraphs,
        }
        section_json_path = paragraph_plans_dir / f"{section_file_stem}.json"
        section_md_path = paragraph_plans_dir / f"{section_file_stem}.md"
        write_results[str(section_json_path)] = write_text_if_allowed(
            section_json_path,
            json.dumps(section_payload, ensure_ascii=False, indent=2) + "\n",
            overwrite,
        )
        write_results[str(section_md_path)] = write_text_if_allowed(
            section_md_path,
            render_section_paragraph_markdown(section_payload) + "\n",
            overwrite,
        )
        generated_files.extend([str(section_json_path), str(section_md_path)])

    if missing_claim_sections:
        preview = ", ".join(missing_claim_sections[:10])
        if len(missing_claim_sections) > 10:
            preview = f"{preview}, ..."
        warnings.append(
            {
                "source": "claims_linking",
                "message": f"{len(missing_claim_sections)} sections have no linked claims and were generated as minimal plans: {preview}",
            }
        )

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "outline_markdown": str(outline_markdown_path),
            "argument_graph_json": str(argument_graph_path),
            "claims_jsonl": str(claims_path),
            "paragraph_plans_dir": str(paragraph_plans_dir),
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "section_source": section_source,
            "paragraph_plans_dir": str(paragraph_plans_dir),
            "section_count": len(section_plan),
            "paragraph_count": paragraph_count,
            "paragraph_type_distribution": paragraph_type_distribution,
            "missing_claim_sections": missing_claim_sections,
            "generated_files": generated_files,
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "generate_paragraph_plans_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "generate_paragraph_plans_completed",
        {
            "run_id": run_id,
            "status": status,
            "section_count": len(section_plan),
            "paragraph_count": paragraph_count,
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "section_count": len(section_plan),
                "paragraph_count": paragraph_count,
                "paragraph_type_distribution": paragraph_type_distribution,
                "missing_claim_sections": missing_claim_sections,
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cleanup_bib_value(raw_value: str) -> str:
    value = compact_whitespace(raw_value).strip().rstrip(",").strip()
    if not value:
        return ""
    if value.startswith("{") and value.endswith("}"):
        value = value[1:-1]
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return compact_whitespace(value)


def parse_bib_identifier_lookup(raw_bib: str) -> dict[str, dict[str, str]]:
    entry_pattern = re.compile(r"@(\w+)\s*\{\s*([^,]+)\s*,(.*?)\n\}", flags=re.DOTALL)
    doi_to_key: dict[str, str] = {}
    arxiv_to_key: dict[str, str] = {}
    title_year_to_key: dict[str, str] = {}
    key_fields: dict[str, dict[str, str]] = {}
    for match in entry_pattern.finditer(raw_bib):
        citation_key = compact_whitespace(match.group(2))
        body = match.group(3)
        if not citation_key:
            continue
        field_map: dict[str, str] = {}
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if "=" not in line:
                continue
            field_name, value = line.split("=", 1)
            normalized_field = compact_whitespace(field_name).lower()
            field_map[normalized_field] = cleanup_bib_value(value)
        key_fields[citation_key] = field_map

        doi = normalize_identifier(field_map.get("doi"))
        if doi and doi not in doi_to_key:
            doi_to_key[doi] = citation_key

        arxiv_candidates = [
            normalize_identifier(field_map.get("eprint")),
            normalize_identifier(field_map.get("arxiv_id")),
            normalize_identifier(field_map.get("archiveprefix")),
        ]
        for arxiv_candidate in arxiv_candidates:
            if arxiv_candidate and arxiv_candidate not in arxiv_to_key and re.search(r"\d", arxiv_candidate):
                arxiv_to_key[arxiv_candidate] = citation_key

        title = normalize_title(field_map.get("title"))
        year = compact_whitespace(field_map.get("year"))
        if title and year:
            lookup_key = f"{title}:{year}"
            if lookup_key not in title_year_to_key:
                title_year_to_key[lookup_key] = citation_key

    return {
        "doi": doi_to_key,
        "arxiv": arxiv_to_key,
        "title_year": title_year_to_key,
        "fields": key_fields,
    }


def resolve_citation_key(
    candidate_id: str,
    candidate_stub: dict[str, Any],
    bib_lookup: dict[str, dict[str, str]],
) -> str:
    doi = normalize_identifier(candidate_stub.get("doi"))
    if doi and doi in bib_lookup.get("doi", {}):
        return bib_lookup["doi"][doi]
    arxiv_id = normalize_identifier(candidate_stub.get("arxiv_id"))
    if arxiv_id and arxiv_id in bib_lookup.get("arxiv", {}):
        return bib_lookup["arxiv"][arxiv_id]
    title = normalize_title(candidate_stub.get("title"))
    year = compact_whitespace(str(candidate_stub.get("year") or ""))
    if title and year:
        lookup_key = f"{title}:{year}"
        if lookup_key in bib_lookup.get("title_year", {}):
            return bib_lookup["title_year"][lookup_key]
    if candidate_id in bib_lookup.get("fields", {}):
        return candidate_id
    return safe_filename_component(candidate_id) or "ref_unknown"


def find_card_for_claim(
    claim: dict[str, Any],
    cards_by_card_id: dict[str, dict[str, Any]],
    cards_by_candidate: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    card_id = compact_whitespace(claim.get("card_id"))
    candidate_id = compact_whitespace(claim.get("candidate_id"))
    if card_id and card_id in cards_by_card_id:
        return cards_by_card_id[card_id]
    if candidate_id and candidate_id in cards_by_candidate:
        return cards_by_candidate[candidate_id]
    return {}


def candidate_stub_from_included(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": compact_whitespace(row.get("candidate_id")),
        "doi": compact_whitespace(row.get("doi")),
        "arxiv_id": compact_whitespace(row.get("arxiv_id")),
        "title": compact_whitespace(row.get("title")),
        "year": compact_whitespace(row.get("year")),
    }


def card_support_texts(card: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for field_name in ("main_findings", "method", "limitations"):
        value = compact_whitespace(card.get(field_name))
        if value:
            values.append(value)
    citable_points = card.get("citable_points")
    if isinstance(citable_points, list):
        for point in citable_points:
            compact = compact_whitespace(str(point))
            if compact:
                values.append(compact)
    elif isinstance(citable_points, str):
        compact = compact_whitespace(citable_points)
        if compact:
            values.append(compact)
    return deduplicate_preserve_order(values)


def extract_support_points(
    claim_row: dict[str, Any],
    card_row: dict[str, Any],
    paragraph_claim_tokens: set[str],
) -> list[str]:
    points: list[str] = []
    claim_text = compact_whitespace(claim_row.get("claim_text"))
    evidence_snippet = compact_whitespace(claim_row.get("evidence_snippet"))
    if claim_text:
        points.append(claim_text)
    if evidence_snippet and evidence_snippet != claim_text:
        points.append(evidence_snippet)

    for text in card_support_texts(card_row):
        token_overlap = len(set(tokenize_terms(text)).intersection(paragraph_claim_tokens))
        if token_overlap >= 2 or (token_overlap >= 1 and len(paragraph_claim_tokens) <= 4):
            points.append(first_sentence(text) or text)
    return deduplicate_preserve_order([compact_whitespace(point) for point in points if compact_whitespace(point)])[:5]


def direction_signals(text: str) -> set[str]:
    lowered = compact_whitespace(text).lower()
    signals: set[str] = set()
    for label, keywords in CONFLICT_DIRECTION_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            signals.add(label)
    return signals


def has_opposite_direction(base_text: str, candidate_text: str) -> bool:
    base_signals = direction_signals(base_text)
    candidate_signals = direction_signals(candidate_text)
    if not base_signals or not candidate_signals:
        return False
    for left, right in CONFLICT_OPPOSITE_PAIRS:
        if (left in base_signals and right in candidate_signals) or (right in base_signals and left in candidate_signals):
            return True
    return False


def evidence_strength_score(
    confidence_values: list[float],
    evidence_levels: list[str],
    support_count: int,
) -> tuple[int, str]:
    avg_confidence = sum(confidence_values) / len(confidence_values) if confidence_values else 0.0
    level_scores = [EVIDENCE_LEVEL_SCORE.get(compact_whitespace(level), 0.7) for level in evidence_levels]
    avg_level_score = sum(level_scores) / len(level_scores) if level_scores else 0.5
    support_term = min(support_count, 4) / 4
    raw_score = (avg_confidence * 60.0) + (avg_level_score * 25.0) + (support_term * 15.0)
    score = int(round(max(0.0, min(100.0, raw_score))))
    if score >= 75:
        label = "strong"
    elif score >= 45:
        label = "medium"
    else:
        label = "weak"
    return score, label


def load_packet_overrides(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def normalize_ignore_claim_rules(overrides: dict[str, Any]) -> tuple[set[str], dict[str, set[str]]]:
    global_ignore: set[str] = set()
    per_paragraph_ignore: dict[str, set[str]] = {}
    values = overrides.get("ignore_claim_ids") or []
    if not isinstance(values, list):
        return global_ignore, per_paragraph_ignore
    for item in values:
        if isinstance(item, str):
            claim_id = compact_whitespace(item)
            if claim_id:
                global_ignore.add(claim_id)
            continue
        if not isinstance(item, dict):
            continue
        paragraph_id = compact_whitespace(item.get("paragraph_id"))
        claim_id = compact_whitespace(item.get("claim_id"))
        if not claim_id:
            continue
        if paragraph_id:
            per_paragraph_ignore.setdefault(paragraph_id, set()).add(claim_id)
        else:
            global_ignore.add(claim_id)
    return global_ignore, per_paragraph_ignore


def assemble_evidence_packets(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    paragraph_plans_dir = (
        Path(args.paragraph_plans_dir).resolve()
        if args.paragraph_plans_dir
        else (paths.root / "draft" / "paragraph_plans")
    )
    claims_path = (
        Path(args.claims_jsonl).resolve()
        if args.claims_jsonl
        else (paths.root / "references" / "index" / "claims.jsonl")
    )
    cards_path = Path(args.cards_jsonl).resolve() if args.cards_jsonl else paths.cards_jsonl
    included_path = (
        Path(args.included_candidates_csv).resolve()
        if args.included_candidates_csv
        else paths.included_candidates_csv
    )
    bib_path = Path(args.bib_path).resolve() if args.bib_path else (paths.root / "draft" / "latex" / "references.bib")
    overrides_path = (
        Path(args.packet_overrides_json).resolve()
        if args.packet_overrides_json
        else (paths.root / "draft" / "evidence_packets" / "packet_overrides.json")
    )
    evidence_packets_dir = (
        Path(args.evidence_packets_dir).resolve()
        if args.evidence_packets_dir
        else (paths.root / "draft" / "evidence_packets")
    )
    overwrite = bool(args.overwrite)

    if not paragraph_plans_dir.exists():
        status = "failed"
        errors.append({"source": "paragraph_plans_dir", "message": f"Directory not found: {paragraph_plans_dir}"})
        section_plan_files: list[Path] = []
    else:
        section_plan_files = sorted([path for path in paragraph_plans_dir.glob("sec_*.json") if path.is_file()])
    if not section_plan_files and status == "ok":
        status = "failed"
        errors.append({"source": "paragraph_plans_dir", "message": "No section paragraph plan JSON files found."})

    if not claims_path.exists():
        status = "failed"
        errors.append({"source": "claims_jsonl", "message": f"File not found: {claims_path}"})
        claim_rows: list[dict[str, Any]] = []
    else:
        try:
            claim_rows = load_jsonl(claims_path)
        except Exception as exc:
            claim_rows = []
            status = "failed"
            errors.append({"source": "claims_jsonl", "message": str(exc)})

    if not cards_path.exists():
        status = "failed"
        errors.append({"source": "cards_jsonl", "message": f"File not found: {cards_path}"})
        cards: list[dict[str, Any]] = []
    else:
        try:
            cards = load_jsonl(cards_path)
        except Exception as exc:
            cards = []
            status = "failed"
            errors.append({"source": "cards_jsonl", "message": str(exc)})

    if not included_path.exists():
        status = "failed"
        errors.append({"source": "included_candidates_csv", "message": f"File not found: {included_path}"})
        included_rows: list[dict[str, Any]] = []
    else:
        try:
            included_rows = read_csv_rows(included_path)
        except Exception as exc:
            included_rows = []
            status = "failed"
            errors.append({"source": "included_candidates_csv", "message": str(exc)})

    bib_lookup: dict[str, dict[str, str]] = {"doi": {}, "arxiv": {}, "title_year": {}, "fields": {}}
    if bib_path.exists():
        try:
            bib_lookup = parse_bib_identifier_lookup(bib_path.read_text(encoding="utf-8"))
        except Exception as exc:
            warnings.append({"source": "bib_path", "message": f"Failed to parse bib file: {exc}"})
    else:
        warnings.append({"source": "bib_path", "message": f"Bib file not found; fallback to candidate_id citation keys: {bib_path}"})

    overrides: dict[str, Any] = {}
    if overrides_path.exists():
        try:
            overrides = load_packet_overrides(overrides_path)
        except Exception as exc:
            warnings.append({"source": "packet_overrides_json", "message": f"Failed to parse overrides: {exc}"})
            overrides = {}
    else:
        warnings.append({"source": "packet_overrides_json", "message": f"Optional override file not found: {overrides_path}"})

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "paragraph_plans_dir": str(paragraph_plans_dir),
                "claims_jsonl": str(claims_path),
                "cards_jsonl": str(cards_path),
                "included_candidates_csv": str(included_path),
                "bib_path": str(bib_path),
                "packet_overrides_json": str(overrides_path),
                "evidence_packets_dir": str(evidence_packets_dir),
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    include_by_candidate: dict[str, dict[str, Any]] = {}
    for row in included_rows:
        candidate_id = compact_whitespace(row.get("candidate_id"))
        if candidate_id and candidate_id not in include_by_candidate:
            include_by_candidate[candidate_id] = row
    include_candidate_ids = set(include_by_candidate.keys())

    citation_key_by_candidate: dict[str, str] = {}
    for candidate_id, row in include_by_candidate.items():
        citation_key_by_candidate[candidate_id] = resolve_citation_key(
            candidate_id=candidate_id,
            candidate_stub=candidate_stub_from_included(row),
            bib_lookup=bib_lookup,
        )

    cards_by_candidate: dict[str, dict[str, Any]] = {}
    cards_by_card_id: dict[str, dict[str, Any]] = {}
    for card in cards:
        candidate_id = compact_whitespace(card.get("candidate_id"))
        card_id = compact_whitespace(card.get("card_id"))
        if candidate_id and candidate_id not in cards_by_candidate:
            cards_by_candidate[candidate_id] = card
        if card_id and card_id not in cards_by_card_id:
            cards_by_card_id[card_id] = card

    claims_by_id: dict[str, dict[str, Any]] = {}
    claims_by_subquestion: dict[str, list[dict[str, Any]]] = {}
    for claim in claim_rows:
        claim_id = compact_whitespace(claim.get("claim_id"))
        candidate_id = compact_whitespace(claim.get("candidate_id"))
        if not claim_id or candidate_id not in include_candidate_ids:
            continue
        claims_by_id[claim_id] = claim
        subquestion_id = compact_whitespace(claim.get("subquestion_id"))
        if subquestion_id:
            claims_by_subquestion.setdefault(subquestion_id, []).append(claim)
    for subquestion_id in list(claims_by_subquestion.keys()):
        claims_by_subquestion[subquestion_id] = sorted(claims_by_subquestion[subquestion_id], key=claim_sort_key)

    global_ignore_claims, paragraph_ignore_claims = normalize_ignore_claim_rules(overrides)
    force_support_entries = overrides.get("force_support") if isinstance(overrides.get("force_support"), list) else []
    force_conflict_entries = overrides.get("force_conflict") if isinstance(overrides.get("force_conflict"), list) else []

    paragraph_count = 0
    packet_count = 0
    missing_packet_count = 0
    conflict_packet_count = 0
    strength_distribution = {label: 0 for label in EVIDENCE_STRENGTH_LABELS}
    write_results: dict[str, str] = {}
    generated_files: list[str] = []
    overrides_applied_count = 0

    for section_file in section_plan_files:
        try:
            section_payload = json.loads(section_file.read_text(encoding="utf-8"))
        except Exception as exc:
            warnings.append({"source": "paragraph_plan_json", "message": f"Failed to parse {section_file}: {exc}"})
            continue
        if not isinstance(section_payload, dict):
            continue
        section_id = compact_whitespace(section_payload.get("section_id")) or section_file.stem
        section_title = compact_whitespace(section_payload.get("section_title")) or section_file.stem
        subquestion_id = compact_whitespace(section_payload.get("subquestion_id"))
        paragraphs = section_payload.get("paragraphs")
        if not isinstance(paragraphs, list):
            continue

        section_out_dir = evidence_packets_dir / section_file.stem
        for paragraph in paragraphs:
            if not isinstance(paragraph, dict):
                continue
            paragraph_count += 1
            paragraph_id = compact_whitespace(paragraph.get("paragraph_id")) or f"{section_id}-P{paragraph_count:02d}"
            paragraph_type = compact_whitespace(paragraph.get("paragraph_type"))
            core_claim_id = compact_whitespace(paragraph.get("core_claim_id"))
            core_claim_text = compact_whitespace(paragraph.get("core_claim_text"))
            required_ids = deduplicate_preserve_order(
                [
                    compact_whitespace(item)
                    for item in (paragraph.get("required_evidence_ids") or [])
                    if compact_whitespace(item)
                ]
            )
            ignore_claims = set(global_ignore_claims).union(paragraph_ignore_claims.get(paragraph_id, set()))
            filtered_required_ids = [claim_id for claim_id in required_ids if claim_id not in ignore_claims]
            if len(filtered_required_ids) != len(required_ids):
                overrides_applied_count += len(required_ids) - len(filtered_required_ids)

            missing_claim_ids: list[str] = []
            supporting_references: list[dict[str, Any]] = []
            confidence_values: list[float] = []
            evidence_levels: list[str] = []
            paragraph_tokens = set(tokenize_terms(core_claim_text))

            for claim_id in filtered_required_ids:
                claim = claims_by_id.get(claim_id)
                if claim is None:
                    missing_claim_ids.append(claim_id)
                    continue
                candidate_id = compact_whitespace(claim.get("candidate_id"))
                if candidate_id not in include_candidate_ids:
                    missing_claim_ids.append(claim_id)
                    continue
                card = find_card_for_claim(claim, cards_by_card_id=cards_by_card_id, cards_by_candidate=cards_by_candidate)
                support_points = extract_support_points(
                    claim_row=claim,
                    card_row=card,
                    paragraph_claim_tokens=paragraph_tokens,
                )
                citation_key = citation_key_by_candidate.get(candidate_id) or (safe_filename_component(candidate_id) or "ref_unknown")
                include_row = include_by_candidate.get(candidate_id, {})
                supporting_references.append(
                    {
                        "claim_id": claim_id,
                        "candidate_id": candidate_id,
                        "citation_key": citation_key,
                        "doi": compact_whitespace(include_row.get("doi") or claim.get("doi")),
                        "arxiv_id": compact_whitespace(include_row.get("arxiv_id") or claim.get("arxiv_id")),
                        "support_points": support_points,
                    }
                )
                try:
                    confidence_values.append(float(claim.get("confidence", 0)))
                except Exception:
                    confidence_values.append(0.0)
                evidence_levels.append(compact_whitespace(card.get("evidence_level")))

            support_by_candidate: dict[str, dict[str, Any]] = {}
            for row in supporting_references:
                candidate_id = compact_whitespace(row.get("candidate_id"))
                if candidate_id and candidate_id not in support_by_candidate:
                    support_by_candidate[candidate_id] = row
            supporting_references = [support_by_candidate[key] for key in sorted(support_by_candidate.keys())]

            conflicting_evidence: list[dict[str, Any]] = []
            candidates_for_conflict = claims_by_subquestion.get(subquestion_id, [])
            support_claim_ids = {compact_whitespace(row.get("claim_id")) for row in supporting_references}
            for candidate_claim in candidates_for_conflict:
                conflict_claim_id = compact_whitespace(candidate_claim.get("claim_id"))
                if (
                    not conflict_claim_id
                    or conflict_claim_id in support_claim_ids
                    or conflict_claim_id in filtered_required_ids
                    or conflict_claim_id in ignore_claims
                ):
                    continue
                candidate_text = compact_whitespace(candidate_claim.get("claim_text"))
                overlap = len(set(tokenize_terms(candidate_text)).intersection(paragraph_tokens))
                if overlap < 2 and not (overlap >= 1 and len(paragraph_tokens) <= 4):
                    continue
                if not has_opposite_direction(core_claim_text, candidate_text):
                    continue
                candidate_id = compact_whitespace(candidate_claim.get("candidate_id"))
                include_row = include_by_candidate.get(candidate_id, {})
                conflicting_evidence.append(
                    {
                        "claim_id": conflict_claim_id,
                        "candidate_id": candidate_id,
                        "citation_key": citation_key_by_candidate.get(candidate_id) or (safe_filename_component(candidate_id) or "ref_unknown"),
                        "doi": compact_whitespace(include_row.get("doi") or candidate_claim.get("doi")),
                        "arxiv_id": compact_whitespace(include_row.get("arxiv_id") or candidate_claim.get("arxiv_id")),
                        "conflict_point": first_sentence(candidate_text) or candidate_text,
                    }
                )
            conflicting_evidence = sorted(
                conflicting_evidence,
                key=lambda row: (
                    compact_whitespace(row.get("candidate_id")),
                    compact_whitespace(row.get("claim_id")),
                ),
            )

            for forced in force_support_entries:
                if not isinstance(forced, dict):
                    continue
                if compact_whitespace(forced.get("paragraph_id")) != paragraph_id:
                    continue
                forced_claim_id = compact_whitespace(forced.get("claim_id"))
                if forced_claim_id and forced_claim_id in ignore_claims:
                    continue
                forced_candidate_id = compact_whitespace(forced.get("candidate_id"))
                forced_points = forced.get("support_points")
                if isinstance(forced_points, list):
                    normalized_points = [compact_whitespace(str(item)) for item in forced_points if compact_whitespace(str(item))]
                else:
                    normalized_points = [compact_whitespace(str(forced_points))] if compact_whitespace(str(forced_points or "")) else []
                support_row = {
                    "claim_id": forced_claim_id,
                    "candidate_id": forced_candidate_id,
                    "citation_key": compact_whitespace(forced.get("citation_key"))
                    or citation_key_by_candidate.get(forced_candidate_id)
                    or (safe_filename_component(forced_candidate_id) if forced_candidate_id else "ref_unknown"),
                    "doi": compact_whitespace(forced.get("doi")),
                    "arxiv_id": compact_whitespace(forced.get("arxiv_id")),
                    "support_points": normalized_points,
                }
                supporting_references.append(support_row)
                overrides_applied_count += 1

            for forced in force_conflict_entries:
                if not isinstance(forced, dict):
                    continue
                if compact_whitespace(forced.get("paragraph_id")) != paragraph_id:
                    continue
                forced_claim_id = compact_whitespace(forced.get("claim_id"))
                if forced_claim_id and forced_claim_id in ignore_claims:
                    continue
                forced_candidate_id = compact_whitespace(forced.get("candidate_id"))
                conflict_row = {
                    "claim_id": forced_claim_id,
                    "candidate_id": forced_candidate_id,
                    "citation_key": compact_whitespace(forced.get("citation_key"))
                    or citation_key_by_candidate.get(forced_candidate_id)
                    or (safe_filename_component(forced_candidate_id) if forced_candidate_id else "ref_unknown"),
                    "doi": compact_whitespace(forced.get("doi")),
                    "arxiv_id": compact_whitespace(forced.get("arxiv_id")),
                    "conflict_point": compact_whitespace(forced.get("conflict_point")) or "Forced conflict evidence.",
                }
                conflicting_evidence.append(conflict_row)
                overrides_applied_count += 1

            supporting_references = sorted(
                supporting_references,
                key=lambda row: (
                    compact_whitespace(row.get("candidate_id")),
                    compact_whitespace(row.get("claim_id")),
                ),
            )
            dedup_support: dict[tuple[str, str], dict[str, Any]] = {}
            for row in supporting_references:
                dedup_support[(compact_whitespace(row.get("candidate_id")), compact_whitespace(row.get("claim_id")))] = row
            supporting_references = list(dedup_support.values())

            dedup_conflict: dict[tuple[str, str], dict[str, Any]] = {}
            for row in conflicting_evidence:
                dedup_conflict[(compact_whitespace(row.get("candidate_id")), compact_whitespace(row.get("claim_id")))] = row
            conflicting_evidence = list(dedup_conflict.values())

            score, label = evidence_strength_score(
                confidence_values=confidence_values,
                evidence_levels=evidence_levels,
                support_count=len(supporting_references),
            )
            if label in strength_distribution:
                strength_distribution[label] += 1

            is_missing = bool(missing_claim_ids) or not supporting_references
            if is_missing:
                missing_packet_count += 1
            if conflicting_evidence:
                conflict_packet_count += 1

            packet = {
                "section_id": section_id,
                "section_title": section_title,
                "subquestion_id": subquestion_id,
                "paragraph_id": paragraph_id,
                "paragraph_type": paragraph_type,
                "claim": {
                    "core_claim_id": core_claim_id,
                    "core_claim_text": core_claim_text,
                },
                "supporting_references": supporting_references,
                "conflicting_evidence": conflicting_evidence,
                "strength": {
                    "score": score,
                    "label": label,
                },
                "missing_evidence": {
                    "is_missing": is_missing,
                    "missing_claim_ids": sorted(deduplicate_preserve_order(missing_claim_ids)),
                    "reason": (
                        "Missing claim references or no supporting references resolved."
                        if is_missing
                        else ""
                    ),
                },
                "provenance": {
                    "generated_at": now_iso,
                    "inputs": {
                        "paragraph_plan_json": str(section_file),
                        "claims_jsonl": str(claims_path),
                        "cards_jsonl": str(cards_path),
                        "included_candidates_csv": str(included_path),
                        "bib_path": str(bib_path),
                        "packet_overrides_json": str(overrides_path),
                    },
                },
            }
            output_path = section_out_dir / f"{paragraph_id}.json"
            write_results[str(output_path)] = write_text_if_allowed(
                output_path,
                json.dumps(packet, ensure_ascii=False, indent=2) + "\n",
                overwrite,
            )
            generated_files.append(str(output_path))
            packet_count += 1

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "paragraph_plans_dir": str(paragraph_plans_dir),
            "claims_jsonl": str(claims_path),
            "cards_jsonl": str(cards_path),
            "included_candidates_csv": str(included_path),
            "bib_path": str(bib_path),
            "packet_overrides_json": str(overrides_path),
            "evidence_packets_dir": str(evidence_packets_dir),
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "paragraph_count": paragraph_count,
            "packet_count": packet_count,
            "missing_packet_count": missing_packet_count,
            "conflict_packet_count": conflict_packet_count,
            "strength_distribution": strength_distribution,
            "overrides_applied_count": overrides_applied_count,
            "generated_files": generated_files,
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "assemble_evidence_packets_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "assemble_evidence_packets_completed",
        {
            "run_id": run_id,
            "status": status,
            "paragraph_count": paragraph_count,
            "packet_count": packet_count,
            "missing_packet_count": missing_packet_count,
            "conflict_packet_count": conflict_packet_count,
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "paragraph_count": paragraph_count,
                "packet_count": packet_count,
                "missing_packet_count": missing_packet_count,
                "conflict_packet_count": conflict_packet_count,
                "strength_distribution": strength_distribution,
                "overrides_applied_count": overrides_applied_count,
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def infer_section_role(section_title: str, paragraph_types: list[str]) -> str:
    lowered_title = compact_whitespace(section_title).lower()
    lowered_types = [compact_whitespace(item).lower() for item in paragraph_types]
    if any(token in lowered_title for token in ("mechanism", "pathway", "机理", "机制")):
        return "mechanism_explanation"
    if any(token in lowered_title for token in ("compare", "comparison", "对比", "比较")):
        return "comparison_analysis"
    if any(token in lowered_title for token in ("limit", "boundary", "uncertain", "局限", "边界")):
        return "limitation_discussion"
    if any(token in lowered_title for token in ("method", "方法")):
        return "methodological_context"
    if any("比较段" in item for item in lowered_types):
        return "comparison_analysis"
    if any("机制解释段" in item for item in lowered_types):
        return "mechanism_explanation"
    if any("争议/局限段" in item for item in lowered_types):
        return "limitation_discussion"
    if any("方法段" in item for item in lowered_types):
        return "methodological_context"
    return "general_argument"


def load_section_roles(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    if not path.exists():
        return {}, {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}, {}

    section_map: dict[str, str] = {}
    paragraph_map: dict[str, str] = {}
    sections = payload.get("sections")
    if isinstance(sections, dict):
        for key, value in sections.items():
            normalized_key = compact_whitespace(str(key))
            normalized_value = compact_whitespace(str(value))
            if normalized_key and normalized_value:
                section_map[normalized_key] = normalized_value
    else:
        for key, value in payload.items():
            normalized_key = compact_whitespace(str(key))
            normalized_value = compact_whitespace(str(value))
            if normalized_key and normalized_value:
                section_map[normalized_key] = normalized_value

    paragraphs = payload.get("paragraphs")
    if isinstance(paragraphs, dict):
        for key, value in paragraphs.items():
            normalized_key = compact_whitespace(str(key))
            normalized_value = compact_whitespace(str(value))
            if normalized_key and normalized_value:
                paragraph_map[normalized_key] = normalized_value
    return section_map, paragraph_map


def load_section_template(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def apply_section_template(
    template_text: str,
    section_title: str,
    section_label: str,
    paragraph_blocks: str,
) -> tuple[str, bool]:
    if not template_text:
        return "", False
    if all(token in template_text for token in ("{{SECTION_TITLE}}", "{{SECTION_LABEL}}", "{{PARAGRAPH_BLOCKS}}")):
        rendered = template_text.replace("{{SECTION_TITLE}}", latex_escape(section_title))
        rendered = rendered.replace("{{SECTION_LABEL}}", section_label)
        rendered = rendered.replace("{{PARAGRAPH_BLOCKS}}", paragraph_blocks)
        return rendered, True
    return "", False


def paragraph_question_from_purpose(purpose: str, core_claim_text: str, paragraph_type: str) -> str:
    if purpose:
        return purpose
    if core_claim_text:
        return f"What does this paragraph need to establish about: {core_claim_text}?"
    if paragraph_type:
        return f"What is the essential message of this {paragraph_type}?"
    return "What specific evidence-grounded point should this paragraph establish?"


def build_paragraph_explanation(
    paragraph: dict[str, Any],
    packet: dict[str, Any],
    section_role: str,
    paragraph_role_override: str = "",
) -> dict[str, Any]:
    paragraph_id = compact_whitespace(paragraph.get("paragraph_id"))
    paragraph_type = compact_whitespace(paragraph.get("paragraph_type"))
    purpose = compact_whitespace(paragraph.get("purpose"))
    core_claim_text = compact_whitespace(paragraph.get("core_claim_text"))
    claim_payload = packet.get("claim") if isinstance(packet.get("claim"), dict) else {}
    core_claim_id = compact_whitespace(claim_payload.get("core_claim_id") or paragraph.get("core_claim_id"))
    if not core_claim_text:
        core_claim_text = compact_whitespace(claim_payload.get("core_claim_text"))

    supporting_references = packet.get("supporting_references")
    if not isinstance(supporting_references, list):
        supporting_references = []
    evidence_used: list[dict[str, Any]] = []
    for row in supporting_references:
        if not isinstance(row, dict):
            continue
        points = row.get("support_points")
        if isinstance(points, list):
            normalized_points = [compact_whitespace(str(item)) for item in points if compact_whitespace(str(item))]
        else:
            normalized_points = []
        evidence_used.append(
            {
                "claim_id": compact_whitespace(row.get("claim_id")),
                "citation_key": compact_whitespace(row.get("citation_key")),
                "candidate_id": compact_whitespace(row.get("candidate_id")),
                "support_points": normalized_points,
            }
        )

    uncertainties: list[str] = []
    missing_payload = packet.get("missing_evidence") if isinstance(packet.get("missing_evidence"), dict) else {}
    if bool(missing_payload.get("is_missing")):
        reason = compact_whitespace(missing_payload.get("reason")) or "Evidence coverage is incomplete for this paragraph."
        uncertainties.append(reason)
    conflicting = packet.get("conflicting_evidence")
    if isinstance(conflicting, list) and conflicting:
        uncertainties.append("Conflicting evidence exists and should be interpreted with caution.")

    strength_payload = packet.get("strength") if isinstance(packet.get("strength"), dict) else {}
    strength_label = compact_whitespace(strength_payload.get("label")).lower()
    if strength_label in HIGH_RISK_STRENGTH_LABELS:
        uncertainties.append("Evidence strength is weak; avoid definitive causal language.")

    overclaim_guardrails = [
        "Avoid absolute or universal claims beyond cited conditions.",
        "Separate observed correlation from causal certainty unless mechanisms are directly evidenced.",
        "State scope boundaries explicitly when evidence is partial or conflicting.",
    ]
    if not evidence_used:
        overclaim_guardrails.append("Do not assert strong conclusions without direct support references.")

    return {
        "paragraph_id": paragraph_id,
        "paragraph_type": paragraph_type,
        "question_to_answer": paragraph_question_from_purpose(
            purpose=purpose,
            core_claim_text=core_claim_text,
            paragraph_type=paragraph_type,
        ),
        "main_conclusion": core_claim_text,
        "evidence_used": evidence_used,
        "uncertainties": deduplicate_preserve_order(uncertainties),
        "overclaim_guardrails": overclaim_guardrails,
        "section_role": paragraph_role_override or section_role,
        "core_claim_id": core_claim_id,
        "strength_label": strength_label or "unknown",
        "missing_evidence": bool(missing_payload.get("is_missing")),
        "has_conflict": bool(conflicting),
    }


def render_paragraph_tex(explanation: dict[str, Any], packet: dict[str, Any], paragraph_index: int) -> str:
    paragraph_title = f"Paragraph {paragraph_index}"
    claim_text = latex_escape(compact_whitespace(explanation.get("main_conclusion")) or "Evidence-grounded claim is pending clarification.")
    role_text = latex_escape(compact_whitespace(explanation.get("section_role")) or "general_argument")
    question_text = latex_escape(compact_whitespace(explanation.get("question_to_answer")))
    uncertainties = explanation.get("uncertainties") if isinstance(explanation.get("uncertainties"), list) else []
    uncertainty_text = latex_escape(uncertainties[0]) if uncertainties else "Interpretation remains bounded by currently assembled evidence."
    guardrails = explanation.get("overclaim_guardrails") if isinstance(explanation.get("overclaim_guardrails"), list) else []
    guardrail_text = latex_escape(guardrails[0]) if guardrails else "Avoid claims that exceed the cited evidence scope."

    lines = [
        f"\\paragraph{{{paragraph_title}}}",
        f"\\textbf{{Claim.}} {claim_text}\\\\",
        f"This paragraph addresses the role of {role_text} through the question: {question_text}",
    ]

    supporting_references = packet.get("supporting_references")
    if not isinstance(supporting_references, list):
        supporting_references = []
    evidence_sentence_count = 0
    for support in supporting_references[:2]:
        if not isinstance(support, dict):
            continue
        citation_key = compact_whitespace(support.get("citation_key"))
        support_points = support.get("support_points")
        if isinstance(support_points, list) and support_points:
            evidence_text = compact_whitespace(str(support_points[0]))
        else:
            evidence_text = compact_whitespace(explanation.get("main_conclusion"))
        if not evidence_text:
            continue
        sentence = latex_escape(evidence_text)
        if citation_key:
            sentence = f"{sentence} \\cite{{{citation_key}}}"
        lines.append(sentence)
        evidence_sentence_count += 1

    if evidence_sentence_count == 0:
        lines.append("Available evidence suggests this claim, but direct support references remain incomplete.")

    lines.append(f"Uncertainty note: {uncertainty_text}")
    lines.append(f"Scope guardrail: {guardrail_text}")
    lines.append("")
    return "\n".join(lines)


def generate_section_drafts(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    paragraph_plans_dir = (
        Path(args.paragraph_plans_dir).resolve()
        if args.paragraph_plans_dir
        else (paths.root / "draft" / "paragraph_plans")
    )
    evidence_packets_dir = (
        Path(args.evidence_packets_dir).resolve()
        if args.evidence_packets_dir
        else (paths.root / "draft" / "evidence_packets")
    )
    section_roles_path = (
        Path(args.section_roles_json).resolve()
        if args.section_roles_json
        else (paths.root / "draft" / "section_roles.json")
    )
    latex_sections_dir = (
        Path(args.latex_sections_dir).resolve()
        if args.latex_sections_dir
        else (paths.root / "draft" / "latex" / "sections")
    )
    latex_template_path = Path(args.latex_template_path).resolve() if args.latex_template_path else Path()
    overwrite = bool(args.overwrite)

    if not paragraph_plans_dir.exists():
        status = "failed"
        errors.append({"source": "paragraph_plans_dir", "message": f"Directory not found: {paragraph_plans_dir}"})
        section_plan_files: list[Path] = []
    else:
        section_plan_files = sorted([path for path in paragraph_plans_dir.glob("sec_*.json") if path.is_file()])
    if not section_plan_files and status == "ok":
        status = "failed"
        errors.append({"source": "paragraph_plans_dir", "message": "No section paragraph plan JSON files found."})

    if not evidence_packets_dir.exists():
        status = "failed"
        errors.append({"source": "evidence_packets_dir", "message": f"Directory not found: {evidence_packets_dir}"})

    section_roles_map: dict[str, str] = {}
    paragraph_roles_map: dict[str, str] = {}
    if section_roles_path.exists():
        try:
            section_roles_map, paragraph_roles_map = load_section_roles(section_roles_path)
        except Exception as exc:
            warnings.append({"source": "section_roles_json", "message": f"Failed to parse roles file: {exc}"})
    else:
        warnings.append({"source": "section_roles_json", "message": f"Optional section roles file not found: {section_roles_path}"})

    template_text = ""
    if args.latex_template_path:
        if latex_template_path.exists():
            try:
                template_text = load_section_template(latex_template_path)
            except Exception as exc:
                warnings.append({"source": "latex_template_path", "message": f"Failed to read template: {exc}"})
        else:
            warnings.append({"source": "latex_template_path", "message": f"Template file not found: {latex_template_path}"})

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "paragraph_plans_dir": str(paragraph_plans_dir),
                "evidence_packets_dir": str(evidence_packets_dir),
                "section_roles_json": str(section_roles_path),
                "latex_sections_dir": str(latex_sections_dir),
                "latex_template_path": str(latex_template_path) if args.latex_template_path else "",
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    section_count = 0
    paragraph_count = 0
    drafted_paragraph_count = 0
    missing_evidence_paragraph_count = 0
    high_risk_paragraph_count = 0
    write_results: dict[str, str] = {}
    generated_section_files: list[str] = []

    section_drafts_dir = run_dir / "section_drafts"
    section_drafts_dir.mkdir(parents=True, exist_ok=True)

    for section_file in section_plan_files:
        try:
            section_payload = json.loads(section_file.read_text(encoding="utf-8"))
        except Exception as exc:
            warnings.append({"source": "paragraph_plan_json", "message": f"Failed to parse {section_file}: {exc}"})
            continue
        if not isinstance(section_payload, dict):
            continue

        section_count += 1
        section_stem = section_file.stem
        section_id = compact_whitespace(section_payload.get("section_id")) or section_stem
        section_title = compact_whitespace(section_payload.get("section_title")) or section_stem
        paragraphs = section_payload.get("paragraphs")
        if not isinstance(paragraphs, list):
            paragraphs = []

        paragraph_types = [compact_whitespace(row.get("paragraph_type")) for row in paragraphs if isinstance(row, dict)]
        section_role = (
            section_roles_map.get(section_stem)
            or section_roles_map.get(section_id)
            or section_roles_map.get(section_title)
            or infer_section_role(section_title=section_title, paragraph_types=paragraph_types)
        )

        explanation_rows: list[dict[str, Any]] = []
        paragraph_blocks: list[str] = []
        for paragraph_index, paragraph in enumerate(paragraphs, start=1):
            if not isinstance(paragraph, dict):
                continue
            paragraph_count += 1
            paragraph_id = compact_whitespace(paragraph.get("paragraph_id")) or f"{section_id}-P{paragraph_index:02d}"
            packet_path = evidence_packets_dir / section_stem / f"{paragraph_id}.json"
            if packet_path.exists():
                try:
                    packet = json.loads(packet_path.read_text(encoding="utf-8"))
                except Exception as exc:
                    packet = {}
                    warnings.append({"source": "evidence_packet_json", "message": f"Failed to parse {packet_path}: {exc}"})
            else:
                packet = {
                    "claim": {
                        "core_claim_id": compact_whitespace(paragraph.get("core_claim_id")),
                        "core_claim_text": compact_whitespace(paragraph.get("core_claim_text")),
                    },
                    "supporting_references": [],
                    "conflicting_evidence": [],
                    "strength": {"score": 0, "label": "weak"},
                    "missing_evidence": {
                        "is_missing": True,
                        "missing_claim_ids": [compact_whitespace(item) for item in (paragraph.get("required_evidence_ids") or []) if compact_whitespace(item)],
                        "reason": "Evidence packet file missing.",
                    },
                }
                warnings.append({"source": "evidence_packet_json", "message": f"Evidence packet not found: {packet_path}"})

            paragraph_role_override = paragraph_roles_map.get(paragraph_id, "")
            explanation = build_paragraph_explanation(
                paragraph=paragraph,
                packet=packet,
                section_role=section_role,
                paragraph_role_override=paragraph_role_override,
            )
            explanation_rows.append(explanation)
            paragraph_tex = render_paragraph_tex(
                explanation=explanation,
                packet=packet,
                paragraph_index=paragraph_index,
            )
            paragraph_blocks.append(paragraph_tex)
            drafted_paragraph_count += 1

            if bool(explanation.get("missing_evidence")):
                missing_evidence_paragraph_count += 1
            if bool(explanation.get("has_conflict")) or compact_whitespace(explanation.get("strength_label")) in HIGH_RISK_STRENGTH_LABELS:
                high_risk_paragraph_count += 1

        section_label = f"sec:{slugify_text(section_title, default='section')}"
        body_text = "\n".join(paragraph_blocks).rstrip() + "\n"
        rendered_from_template, template_applied = apply_section_template(
            template_text=template_text,
            section_title=section_title,
            section_label=section_label,
            paragraph_blocks=body_text,
        )
        if args.latex_template_path and not template_applied:
            warnings.append(
                {
                    "source": "latex_template_path",
                    "message": "Template missing required placeholders {{SECTION_TITLE}}, {{SECTION_LABEL}}, {{PARAGRAPH_BLOCKS}}; fallback to built-in template.",
                }
            )

        if template_applied:
            section_tex = rendered_from_template
        else:
            section_tex = "\n".join(
                [
                    f"\\section{{{latex_escape(section_title)}}}",
                    f"\\label{{{section_label}}}",
                    body_text,
                ]
            ).rstrip() + "\n"

        output_tex_path = latex_sections_dir / f"{section_stem}.tex"
        write_results[str(output_tex_path)] = write_text_if_allowed(output_tex_path, section_tex, overwrite)
        generated_section_files.append(str(output_tex_path))

        section_record = {
            "section_id": section_id,
            "section_title": section_title,
            "section_stem": section_stem,
            "section_role": section_role,
            "generated_at": now_iso,
            "paragraphs": explanation_rows,
        }
        section_record_path = section_drafts_dir / f"{section_stem}.json"
        section_record_path.write_text(json.dumps(section_record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "paragraph_plans_dir": str(paragraph_plans_dir),
            "evidence_packets_dir": str(evidence_packets_dir),
            "section_roles_json": str(section_roles_path),
            "latex_sections_dir": str(latex_sections_dir),
            "latex_template_path": str(latex_template_path) if args.latex_template_path else "",
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "section_count": section_count,
            "paragraph_count": paragraph_count,
            "drafted_paragraph_count": drafted_paragraph_count,
            "missing_evidence_paragraph_count": missing_evidence_paragraph_count,
            "high_risk_paragraph_count": high_risk_paragraph_count,
            "section_drafts_dir": str(section_drafts_dir),
            "generated_section_files": generated_section_files,
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "generate_section_drafts_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "generate_section_drafts_completed",
        {
            "run_id": run_id,
            "status": status,
            "section_count": section_count,
            "paragraph_count": paragraph_count,
            "drafted_paragraph_count": drafted_paragraph_count,
            "missing_evidence_paragraph_count": missing_evidence_paragraph_count,
            "high_risk_paragraph_count": high_risk_paragraph_count,
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "section_count": section_count,
                "paragraph_count": paragraph_count,
                "drafted_paragraph_count": drafted_paragraph_count,
                "missing_evidence_paragraph_count": missing_evidence_paragraph_count,
                "high_risk_paragraph_count": high_risk_paragraph_count,
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def resolve_latest_section_drafts_dir(runs_dir: Path, current_run_dir: Path) -> Path | None:
    run_dirs = sorted([path for path in runs_dir.glob("run_*") if path.is_dir()], reverse=True)
    for run_dir in run_dirs:
        if run_dir == current_run_dir:
            continue
        candidate = run_dir / "section_drafts"
        if candidate.exists() and any(candidate.glob("sec_*.json")):
            return candidate
    return None


def parse_section_paragraph_units(section_text: str) -> tuple[list[str], list[list[str]]]:
    lines = section_text.splitlines()
    preamble: list[str] = []
    paragraphs: list[list[str]] = []
    current_paragraph: list[str] = []

    for line in lines:
        if re.match(r"\s*\\paragraph\{", line):
            if current_paragraph:
                paragraphs.append(current_paragraph)
            current_paragraph = [line]
            continue
        if current_paragraph:
            current_paragraph.append(line)
        else:
            preamble.append(line)

    if current_paragraph:
        paragraphs.append(current_paragraph)

    return preamble, paragraphs


def compose_section_from_units(preamble: list[str], paragraphs: list[list[str]]) -> str:
    lines: list[str] = []
    lines.extend(preamble)
    for paragraph in paragraphs:
        lines.extend(paragraph)
    if not lines:
        return ""
    return "\n".join(lines).rstrip() + "\n"


def normalize_sentence_key(text: str) -> str:
    normalized = re.sub(r"\\cite\w*\{[^}]*\}", " ", text)
    normalized = re.sub(r"\\[A-Za-z]+\*?\{([^}]*)\}", r"\1", normalized)
    normalized = re.sub(r"\\[A-Za-z]+\*?", " ", normalized)
    normalized = normalized.replace("{", " ").replace("}", " ")
    normalized = compact_whitespace(normalized).lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return compact_whitespace(normalized)


def narrative_line_indices(paragraph_lines: list[str]) -> list[int]:
    indices: list[int] = []
    for idx, line in enumerate(paragraph_lines):
        normalized = compact_whitespace(line)
        if not normalized:
            continue
        if normalized.startswith("\\paragraph{") or normalized.startswith("\\section{") or normalized.startswith("\\label{"):
            continue
        if "\\textbf{Claim.}" in normalized:
            continue
        indices.append(idx)
    return indices


def has_transition_marker(paragraph_lines: list[str]) -> bool:
    plain_lines = extract_text_lines_for_assertion(paragraph_lines)
    if not plain_lines:
        return False
    first = plain_lines[0].lower()
    return any(first.startswith(marker) for marker in CONSISTENCY_TRANSITION_MARKERS)


def choose_transition_sentence(paragraph_type: str) -> str:
    lowered = compact_whitespace(paragraph_type).lower()
    if any(token in lowered for token in ("limitation", "boundary", "uncertain", "争议", "局限")):
        return "However, boundaries and uncertainties should be stated before extending the claim."
    if any(token in lowered for token in ("comparison", "比较", "对比")):
        return "By contrast, the following evidence clarifies comparative performance."
    if any(token in lowered for token in ("method", "方法")):
        return "Methodologically, the next point specifies how the evidence was obtained."
    if any(token in lowered for token in ("mechanism", "机理", "机制")):
        return "Mechanistically, the next paragraph explains the evidence-linked pathway."
    if any(token in lowered for token in ("summary", "小结")):
        return "Taken together, the preceding results motivate the following synthesis."
    return "Building on the previous paragraph, the next evidence-based point is presented."


def rewrite_claim_line_to_evidence_led(claim_line: str) -> tuple[str, bool]:
    match = re.match(r"(\s*\\textbf\{Claim\.\}\s*)(.+)$", claim_line)
    if not match:
        return claim_line, False

    prefix = match.group(1)
    body = compact_whitespace(match.group(2))
    if not body:
        return claim_line, False
    lowered = body.lower()
    if lowered.startswith("evidence suggests") or lowered.startswith("available evidence suggests"):
        return claim_line, False

    trailing = ""
    if body.endswith("\\\\"):
        trailing = " \\\\"
        body = compact_whitespace(body[:-2])
    if not body:
        return claim_line, False

    rewritten = f"{prefix}Evidence suggests that {body}{trailing}"
    return rewritten, True


def ensure_missing_evidence_safeguards(paragraph_lines: list[str]) -> tuple[list[str], bool]:
    joined = "\n".join(paragraph_lines)
    lowered = joined.lower()
    has_boundary = CONSISTENCY_BOUNDARY_SENTENCE.lower() in lowered
    has_uncertainty = CONSISTENCY_UNCERTAINTY_SENTENCE.lower() in lowered

    if has_boundary and has_uncertainty:
        return paragraph_lines, False

    updated = list(paragraph_lines)
    insert_at = len(updated)
    while insert_at > 0 and not compact_whitespace(updated[insert_at - 1]):
        insert_at -= 1

    if not has_boundary:
        updated.insert(insert_at, CONSISTENCY_BOUNDARY_SENTENCE)
        insert_at += 1
    if not has_uncertainty:
        updated.insert(insert_at, CONSISTENCY_UNCERTAINTY_SENTENCE)
    return updated, True


def paragraph_has_citation(paragraph_lines: list[str]) -> bool:
    return bool(extract_citation_keys("\n".join(paragraph_lines)))


def extract_section_title_from_text(section_text: str, fallback: str) -> str:
    for line in section_text.splitlines():
        match = re.search(r"\\section\{([^}]*)\}", line)
        if match:
            title = compact_whitespace(match.group(1))
            if title:
                return title
    return fallback


def apply_term_consistency_rewrites(section_text: str) -> tuple[str, list[dict[str, str]]]:
    token_pattern = re.compile(r"\b[A-Za-z][A-Za-z\-]{2,}\b")
    canonical_variants: dict[str, list[str]] = {}
    first_seen: dict[str, str] = {}

    for match in token_pattern.finditer(section_text):
        token = compact_whitespace(match.group(0))
        if not token:
            continue
        canonical = token.lower().replace("-", "")
        if len(canonical) < 4:
            continue
        if canonical not in canonical_variants:
            canonical_variants[canonical] = []
            first_seen[canonical] = token
        if token not in canonical_variants[canonical]:
            canonical_variants[canonical].append(token)

    updated_text = section_text
    events: list[dict[str, str]] = []
    for canonical, variants in canonical_variants.items():
        if len(variants) <= 1:
            continue
        has_case_diff = len(set(variants)) > 1 and len({item.lower() for item in variants}) == 1
        contains_hyphen = any("-" in item for item in variants)
        has_hyphen_diff = any("-" in item for item in variants) and any("-" not in item for item in variants)
        if not ((contains_hyphen and has_case_diff) or has_hyphen_diff):
            continue

        primary = first_seen[canonical]
        replaced_variants: list[str] = []
        for variant in variants[1:]:
            candidate = re.sub(rf"\b{re.escape(variant)}\b", primary, updated_text)
            if candidate != updated_text:
                replaced_variants.append(variant)
                updated_text = candidate
        if replaced_variants:
            events.append(
                {
                    "primary": primary,
                    "replaced": ", ".join(replaced_variants),
                    "canonical": canonical,
                }
            )

    return updated_text, events


def score_consistency_issues(issues: list[dict[str, Any]]) -> tuple[int, str]:
    score = 100
    for issue in issues:
        issue_type = compact_whitespace(issue.get("type"))
        score -= CONSISTENCY_ISSUE_PENALTY.get(issue_type, 5)
    score = max(0, min(100, score))
    if score >= 85:
        risk_level = "low"
    elif score >= 60:
        risk_level = "medium"
    else:
        risk_level = "high"
    return score, risk_level


def revise_section_consistency(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    strictness = compact_whitespace(args.strictness).lower() or "soft"
    overwrite = bool(args.overwrite)

    latex_sections_dir = (
        Path(args.latex_sections_dir).resolve()
        if args.latex_sections_dir
        else (paths.root / "draft" / "latex" / "sections")
    )
    argument_graph_path = (
        Path(args.argument_graph_json).resolve()
        if args.argument_graph_json
        else (paths.root / "references" / "index" / "argument_graph.json")
    )
    if args.section_drafts_dir:
        section_drafts_dir = Path(args.section_drafts_dir).resolve()
    else:
        discovered = resolve_latest_section_drafts_dir(paths.runs_dir, run_dir)
        section_drafts_dir = discovered if discovered else Path()
    report_path = (
        Path(args.consistency_report_json).resolve()
        if args.consistency_report_json
        else (run_dir / "section_consistency_report.json")
    )

    if strictness not in {"soft", "hard"}:
        status = "failed"
        errors.append({"source": "strictness", "message": f"Unsupported strictness: {strictness}"})

    section_files = sorted([path for path in latex_sections_dir.glob("sec_*.tex") if path.is_file()])
    if not section_files:
        status = "failed"
        errors.append({"source": "latex_sections_dir", "message": f"No section tex files found under: {latex_sections_dir}"})

    if not argument_graph_path.exists():
        status = "failed"
        errors.append({"source": "argument_graph_json", "message": f"File not found: {argument_graph_path}"})
        argument_graph: dict[str, Any] = {}
    else:
        try:
            argument_graph = json.loads(argument_graph_path.read_text(encoding="utf-8"))
        except Exception as exc:
            argument_graph = {}
            status = "failed"
            errors.append({"source": "argument_graph_json", "message": str(exc)})

    if not section_drafts_dir or not section_drafts_dir.exists():
        status = "failed"
        if args.section_drafts_dir:
            message = f"Section drafts directory not found: {section_drafts_dir}"
        else:
            message = "No previous run with section_drafts found under draft/runs."
        errors.append({"source": "section_drafts_dir", "message": message})

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "latex_sections_dir": str(latex_sections_dir),
                "argument_graph_json": str(argument_graph_path),
                "section_drafts_dir": str(section_drafts_dir),
                "consistency_report_json": str(report_path),
                "strictness": strictness,
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    section_plan = argument_graph.get("section_plan", []) if isinstance(argument_graph, dict) else []
    if not isinstance(section_plan, list):
        section_plan = []

    revised_section_count = 0
    skipped_section_count = 0
    paragraph_count = 0
    auto_fixed_count = 0
    manual_review_count = 0
    high_risk_section_count = 0
    issue_counts_by_type: dict[str, int] = {}
    write_results: dict[str, str] = {}
    section_reports: list[dict[str, Any]] = []
    severity_counts = {"high": 0, "medium": 0, "low": 0}
    issue_counter = 1

    def next_issue_id() -> str:
        nonlocal issue_counter
        issue_id = f"SCF{issue_counter:04d}"
        issue_counter += 1
        return issue_id

    for section_index, section_file in enumerate(section_files, start=1):
        mapped = section_plan[section_index - 1] if section_index - 1 < len(section_plan) and isinstance(section_plan[section_index - 1], dict) else {}
        section_stem = section_file.stem
        section_text_original = section_file.read_text(encoding="utf-8")
        fallback_title = compact_whitespace(mapped.get("section_title")) or section_stem
        section_title = extract_section_title_from_text(section_text_original, fallback=fallback_title)

        section_record_path = section_drafts_dir / f"{section_stem}.json"
        section_issues: list[dict[str, Any]] = []

        if not section_record_path.exists():
            skipped_section_count += 1
            manual_review_count += 1
            write_results[str(section_file)] = "skipped"
            warnings.append({"source": "section_drafts_dir", "message": f"Missing section draft record: {section_record_path}"})
            section_score, section_risk_level = score_consistency_issues(section_issues)
            section_reports.append(
                {
                    "section_stem": section_stem,
                    "section_title": section_title,
                    "issues": section_issues,
                    "section_score": section_score,
                    "section_risk_level": section_risk_level,
                    "paragraph_count": 0,
                    "write_result": "skipped",
                    "manual_review": True,
                }
            )
            continue

        try:
            section_record = json.loads(section_record_path.read_text(encoding="utf-8"))
        except Exception as exc:
            skipped_section_count += 1
            manual_review_count += 1
            write_results[str(section_file)] = "skipped"
            warnings.append({"source": "section_draft_json", "message": f"Failed to parse {section_record_path}: {exc}"})
            section_score, section_risk_level = score_consistency_issues(section_issues)
            section_reports.append(
                {
                    "section_stem": section_stem,
                    "section_title": section_title,
                    "issues": section_issues,
                    "section_score": section_score,
                    "section_risk_level": section_risk_level,
                    "paragraph_count": 0,
                    "write_result": "skipped",
                    "manual_review": True,
                }
            )
            continue

        paragraph_explanations = section_record.get("paragraphs")
        if not isinstance(paragraph_explanations, list):
            paragraph_explanations = []

        preamble_lines, paragraph_blocks = parse_section_paragraph_units(section_text_original)
        if not paragraph_blocks:
            warnings.append({"source": "section_tex", "message": f"No paragraph blocks found in {section_file}"})

        section_paragraph_count = len(paragraph_blocks)
        paragraph_count += section_paragraph_count

        if paragraph_explanations and len(paragraph_explanations) != len(paragraph_blocks):
            warnings.append(
                {
                    "source": "section_alignment",
                    "message": (
                        f"Paragraph count mismatch for {section_stem}: "
                        f"tex={len(paragraph_blocks)}, section_drafts={len(paragraph_explanations)}"
                    ),
                }
            )

        previous_sentence_keys: set[str] = set()
        for paragraph_idx, paragraph_lines in enumerate(paragraph_blocks):
            paragraph_meta = paragraph_explanations[paragraph_idx] if paragraph_idx < len(paragraph_explanations) and isinstance(paragraph_explanations[paragraph_idx], dict) else {}
            paragraph_id = compact_whitespace(paragraph_meta.get("paragraph_id")) or f"{section_stem}-P{paragraph_idx + 1:02d}"
            paragraph_type = compact_whitespace(paragraph_meta.get("paragraph_type"))

            if paragraph_idx > 0 and not has_transition_marker(paragraph_lines):
                transition_line = choose_transition_sentence(paragraph_type)
                insert_at = 1
                if len(paragraph_lines) > 1 and "\\textbf{Claim.}" in paragraph_lines[1]:
                    insert_at = 2
                paragraph_lines.insert(insert_at, transition_line)
                section_issues.append(
                    {
                        "issue_id": next_issue_id(),
                        "type": "logical_jump",
                        "severity": "medium",
                        "paragraph_id": paragraph_id,
                        "message": "Inserted transition sentence between adjacent paragraphs.",
                        "before_excerpt": "",
                        "after_excerpt": transition_line,
                        "auto_fixed": True,
                    }
                )

            claim_line_index = -1
            for line_index, line in enumerate(paragraph_lines):
                if "\\textbf{Claim.}" in line:
                    claim_line_index = line_index
                    break
            if claim_line_index >= 0 and paragraph_has_citation(paragraph_lines):
                claim_before = paragraph_lines[claim_line_index]
                claim_after, changed = rewrite_claim_line_to_evidence_led(claim_before)
                if changed:
                    paragraph_lines[claim_line_index] = claim_after
                    section_issues.append(
                        {
                            "issue_id": next_issue_id(),
                            "type": "claim_evidence_order",
                            "severity": "medium",
                            "paragraph_id": paragraph_id,
                            "message": "Adjusted claim sentence to evidence-led wording.",
                            "before_excerpt": compact_whitespace(claim_before),
                            "after_excerpt": compact_whitespace(claim_after),
                            "auto_fixed": True,
                        }
                    )

            narrative_indices = narrative_line_indices(paragraph_lines)
            duplicate_indices: list[int] = []
            duplicate_lines: list[str] = []
            for line_index in narrative_indices:
                key = normalize_sentence_key(paragraph_lines[line_index])
                if key and key in previous_sentence_keys:
                    duplicate_indices.append(line_index)
                    duplicate_lines.append(compact_whitespace(paragraph_lines[line_index]))

            if duplicate_indices:
                if len(duplicate_indices) == len(narrative_indices):
                    duplicate_indices = duplicate_indices[1:]
                    duplicate_lines = duplicate_lines[1:]
                if duplicate_indices:
                    for line_index in sorted(duplicate_indices, reverse=True):
                        del paragraph_lines[line_index]
                    section_issues.append(
                        {
                            "issue_id": next_issue_id(),
                            "type": "adjacent_duplication",
                            "severity": "medium",
                            "paragraph_id": paragraph_id,
                            "message": "Removed duplicated adjacent sentence(s).",
                            "before_excerpt": "; ".join(duplicate_lines[:2]),
                            "after_excerpt": "Duplicated sentence removed from later paragraph.",
                            "auto_fixed": True,
                        }
                    )

            requires_missing_evidence_guard = bool(paragraph_meta.get("missing_evidence")) or not paragraph_has_citation(paragraph_lines)
            if requires_missing_evidence_guard:
                paragraph_before = "\n".join(paragraph_lines)
                paragraph_lines, safeguard_changed = ensure_missing_evidence_safeguards(paragraph_lines)
                if safeguard_changed:
                    section_issues.append(
                        {
                            "issue_id": next_issue_id(),
                            "type": "overclaim_without_support",
                            "severity": "high",
                            "paragraph_id": paragraph_id,
                            "message": "Added uncertainty and boundary guardrails for unsupported paragraph.",
                            "before_excerpt": compact_whitespace(paragraph_before)[:220],
                            "after_excerpt": f"{CONSISTENCY_BOUNDARY_SENTENCE} {CONSISTENCY_UNCERTAINTY_SENTENCE}",
                            "auto_fixed": True,
                        }
                    )

            paragraph_blocks[paragraph_idx] = paragraph_lines

            previous_sentence_keys = set()
            for line_index in narrative_line_indices(paragraph_lines):
                key = normalize_sentence_key(paragraph_lines[line_index])
                if key:
                    previous_sentence_keys.add(key)

        revised_section_text = compose_section_from_units(preamble_lines, paragraph_blocks)
        revised_section_text, term_events = apply_term_consistency_rewrites(revised_section_text)
        for event in term_events:
            section_issues.append(
                {
                    "issue_id": next_issue_id(),
                    "type": "term_consistency",
                    "severity": "low",
                    "paragraph_id": "",
                    "message": "Unified inconsistent term forms within section.",
                    "before_excerpt": event["replaced"],
                    "after_excerpt": event["primary"],
                    "auto_fixed": True,
                }
            )

        write_result = write_text_if_allowed(section_file, revised_section_text, overwrite)
        write_results[str(section_file)] = write_result
        if write_result == "written":
            revised_section_count += 1
        else:
            skipped_section_count += 1
            manual_review_count += max(1, len(section_issues))
            for issue in section_issues:
                issue["auto_fixed"] = False

        for issue in section_issues:
            issue_type = compact_whitespace(issue.get("type"))
            issue_counts_by_type[issue_type] = issue_counts_by_type.get(issue_type, 0) + 1
            severity = compact_whitespace(issue.get("severity")).lower()
            if severity in severity_counts:
                severity_counts[severity] += 1
            if issue.get("auto_fixed"):
                auto_fixed_count += 1
            else:
                manual_review_count += 1

        section_score, section_risk_level = score_consistency_issues(section_issues)
        if section_risk_level == "high":
            high_risk_section_count += 1
        section_reports.append(
            {
                "section_stem": section_stem,
                "section_title": section_title,
                "issues": section_issues,
                "section_score": section_score,
                "section_risk_level": section_risk_level,
                "paragraph_count": section_paragraph_count,
                "write_result": write_result,
                "manual_review": write_result != "written",
            }
        )

    all_issues: list[dict[str, Any]] = []
    for section in section_reports:
        issues = section.get("issues")
        if isinstance(issues, list):
            all_issues.extend(issues)
    overall_score, overall_risk_level = score_consistency_issues(all_issues)

    if strictness == "hard" and severity_counts["high"] > 0:
        status = "failed"

    report_payload = {
        "run_id": run_id,
        "timestamp": now_iso,
        "strictness": strictness,
        "summary": {
            "section_count": len(section_reports),
            "paragraph_count": paragraph_count,
            "issue_counts_by_type": issue_counts_by_type,
            "auto_fixed_count": auto_fixed_count,
            "manual_review_count": manual_review_count,
            "score": overall_score,
            "risk_level": overall_risk_level,
            "severity_counts": severity_counts,
        },
        "sections": section_reports,
        "warnings": warnings,
        "errors": errors,
    }

    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        status = "failed"
        errors.append({"source": "consistency_report_json", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "latex_sections_dir": str(latex_sections_dir),
            "argument_graph_json": str(argument_graph_path),
            "section_drafts_dir": str(section_drafts_dir),
            "consistency_report_json": str(report_path),
            "strictness": strictness,
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "section_consistency_report_json": str(report_path),
            "revised_section_count": revised_section_count,
            "skipped_section_count": skipped_section_count,
            "issue_counts_by_type": issue_counts_by_type,
            "high_risk_section_count": high_risk_section_count,
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "revise_section_consistency_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "revise_section_consistency_completed",
        {
            "run_id": run_id,
            "status": status,
            "section_count": len(section_reports),
            "paragraph_count": paragraph_count,
            "overall_score": overall_score,
            "overall_risk_level": overall_risk_level,
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "summary": report_payload["summary"],
                "outputs": {
                    "section_consistency_report_json": str(report_path),
                },
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def render_csv_table_to_tex(csv_path: Path, caption: str, label: str) -> str:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = list(csv.reader(handle))
    if not reader:
        return (
            f"% Empty CSV source: {csv_path.name}\n"
            "\\begin{table}[htbp]\n\\centering\n"
            f"\\caption{{{latex_escape(caption)}}}\n"
            f"\\label{{tab:{slugify_text(label, default='table')}}}\n"
            "\\begin{tabular}{l}\n\\toprule\nNo data\\\\\n\\bottomrule\n"
            "\\end{tabular}\n\\end{table}\n"
        )

    header = [latex_escape(cell) for cell in reader[0]]
    rows = reader[1:]
    column_count = max(1, len(header))
    alignment = "l" * column_count
    lines = [
        "\\begin{table}[htbp]",
        "\\centering",
        f"\\caption{{{latex_escape(caption)}}}",
        f"\\label{{tab:{slugify_text(label, default='table')}}}",
        f"\\begin{{tabular}}{{{alignment}}}",
        "\\toprule",
        " & ".join(header) + " \\\\",
        "\\midrule",
    ]
    if rows:
        for row in rows:
            normalized = [latex_escape(cell) for cell in row[:column_count]]
            if len(normalized) < column_count:
                normalized.extend([""] * (column_count - len(normalized)))
            lines.append(" & ".join(normalized) + " \\\\")
    else:
        lines.append("No data " + ("& " * (column_count - 1)).rstrip() + " \\\\")
    lines.extend(
        [
            "\\bottomrule",
            "\\end{tabular}",
            "\\end{table}",
            "",
        ]
    )
    return "\n".join(lines)


def generate_latex_draft(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    argument_graph_path = (
        Path(args.argument_graph_json).resolve()
        if args.argument_graph_json
        else (paths.root / "references" / "index" / "argument_graph.json")
    )
    claims_path = (
        Path(args.claims_jsonl).resolve()
        if args.claims_jsonl
        else (paths.root / "references" / "index" / "claims.jsonl")
    )
    included_path = (
        Path(args.included_candidates_csv).resolve()
        if args.included_candidates_csv
        else paths.included_candidates_csv
    )
    records_path = Path(args.records_jsonl).resolve() if args.records_jsonl else paths.records_jsonl
    refs_bib_path = Path(args.refs_bib).resolve() if args.refs_bib else paths.refs_bib
    latex_dir = Path(args.latex_dir).resolve() if args.latex_dir else (paths.root / "draft" / "latex")
    figures_dir = Path(args.figures_dir).resolve() if args.figures_dir else (paths.root / "figures")
    tables_dir = Path(args.tables_dir).resolve() if args.tables_dir else (paths.root / "tables")
    overwrite = bool(args.overwrite)

    if not argument_graph_path.exists():
        status = "failed"
        errors.append({"source": "argument_graph_json", "message": f"File not found: {argument_graph_path}"})
        argument_graph: dict[str, Any] = {}
    else:
        try:
            argument_graph = json.loads(argument_graph_path.read_text(encoding="utf-8"))
        except Exception as exc:
            argument_graph = {}
            status = "failed"
            errors.append({"source": "argument_graph_json", "message": str(exc)})

    if not claims_path.exists():
        status = "failed"
        errors.append({"source": "claims_jsonl", "message": f"File not found: {claims_path}"})
        claims_rows: list[dict[str, Any]] = []
    else:
        try:
            claims_rows = load_jsonl(claims_path)
        except Exception as exc:
            claims_rows = []
            status = "failed"
            errors.append({"source": "claims_jsonl", "message": str(exc)})

    if not included_path.exists():
        status = "failed"
        errors.append({"source": "included_candidates_csv", "message": f"File not found: {included_path}"})
        included_rows: list[dict[str, Any]] = []
    else:
        try:
            included_rows = read_csv_rows(included_path)
        except Exception as exc:
            included_rows = []
            status = "failed"
            errors.append({"source": "included_candidates_csv", "message": str(exc)})

    if not records_path.exists():
        status = "failed"
        errors.append({"source": "records_jsonl", "message": f"File not found: {records_path}"})
        records: list[dict[str, Any]] = []
    else:
        try:
            records = load_records(records_path)
        except Exception as exc:
            records = []
            status = "failed"
            errors.append({"source": "records_jsonl", "message": str(exc)})

    if refs_bib_path.exists():
        supplemental_bib_entries = parse_bib_entries_by_key(refs_bib_path.read_text(encoding="utf-8"))
    else:
        supplemental_bib_entries = {}
        warnings.append({"source": "refs_bib", "message": f"Supplemental refs not found: {refs_bib_path}"})

    section_plan = argument_graph.get("section_plan", []) if isinstance(argument_graph, dict) else []
    if not isinstance(section_plan, list):
        section_plan = []
    if not section_plan and status == "ok":
        status = "failed"
        errors.append({"source": "argument_graph_json", "message": "section_plan is empty."})

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "argument_graph_json": str(argument_graph_path),
                "claims_jsonl": str(claims_path),
                "included_candidates_csv": str(included_path),
                "records_jsonl": str(records_path),
                "refs_bib": str(refs_bib_path),
                "latex_dir": str(latex_dir),
                "figures_dir": str(figures_dir),
                "tables_dir": str(tables_dir),
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    records_index = index_records(records)
    claim_rows_by_subquestion: dict[str, list[dict[str, Any]]] = {}
    for claim in claims_rows:
        sqid = compact_whitespace(claim.get("subquestion_id"))
        if sqid:
            claim_rows_by_subquestion.setdefault(sqid, []).append(claim)

    unique_include_rows: list[dict[str, Any]] = []
    seen_candidate_ids: set[str] = set()
    for row in included_rows:
        candidate_id = compact_whitespace(row.get("candidate_id"))
        if not candidate_id or candidate_id in seen_candidate_ids:
            continue
        seen_candidate_ids.add(candidate_id)
        unique_include_rows.append(row)

    include_by_candidate = {compact_whitespace(row.get("candidate_id")): row for row in unique_include_rows}

    bibliography_entries: list[str] = []
    candidate_citation_key: dict[str, str] = {}
    seen_keys: set[str] = set()
    for candidate_id, included_row in sorted(include_by_candidate.items()):
        candidate_stub = {
            "doi": compact_whitespace(included_row.get("doi")),
            "arxiv_id": compact_whitespace(included_row.get("arxiv_id")),
            "title": compact_whitespace(included_row.get("title")),
            "year": str(included_row.get("year") or ""),
        }
        record = matched_record_for_candidate(candidate_stub, records_index)
        citation_key = stable_citation_key(candidate_id, record)
        if citation_key in seen_keys:
            candidate_citation_key[candidate_id] = citation_key
            continue

        seen_keys.add(citation_key)
        candidate_citation_key[candidate_id] = citation_key
        if citation_key in supplemental_bib_entries:
            bibliography_entries.append(supplemental_bib_entries[citation_key].strip())
            continue

        source_record = record or {
            "title": candidate_stub["title"] or f"Candidate {candidate_id}",
            "authors": [],
            "year": parse_year(candidate_stub["year"]) or candidate_stub["year"] or "n.d.",
            "venue": "",
            "doi": candidate_stub["doi"],
            "arxiv_id": candidate_stub["arxiv_id"],
            "pdf_url": "",
        }
        generated = {
            "citation_key": citation_key,
            "title": compact_whitespace(source_record.get("title")) or f"Candidate {candidate_id}",
            "authors": source_record.get("authors") or [],
            "year": source_record.get("year"),
            "venue": compact_whitespace(source_record.get("venue")),
            "doi": compact_whitespace(source_record.get("doi")),
            "arxiv_id": compact_whitespace(source_record.get("arxiv_id")),
            "pdf_url": compact_whitespace(source_record.get("pdf_url")),
        }
        bibliography_entries.append(to_bibtex(generated).strip())

    bibliography_text = "\n\n".join([entry for entry in bibliography_entries if compact_whitespace(entry)]) + "\n"

    section_files: list[str] = []
    section_contents: list[tuple[Path, str]] = []
    for index, section in enumerate(section_plan, start=1):
        title = compact_whitespace(section.get("section_title")) or f"Section {index}"
        subquestion_id = compact_whitespace(section.get("subquestion_id"))
        slug = slugify_text(title, default=f"section-{index}")
        file_name = f"sec_{index:03d}_{slug}.tex"
        section_files.append(file_name)
        section_path = latex_dir / "sections" / file_name

        lines = [
            f"\\section{{{latex_escape(title)}}}",
            f"\\label{{sec:{slugify_text(title, default='section')}}}",
            "",
        ]
        points = section.get("paragraph_points")
        if not isinstance(points, list) or not points:
            lines.append("% TODO: fill section details based on evidence clusters.")
        else:
            related_claims = claim_rows_by_subquestion.get(subquestion_id, [])
            related_candidate_ids = deduplicate_preserve_order(
                [
                    compact_whitespace(claim.get("candidate_id"))
                    for claim in related_claims
                    if compact_whitespace(claim.get("candidate_id"))
                ]
            )
            citation_keys = [candidate_citation_key[cid] for cid in related_candidate_ids if cid in candidate_citation_key]
            citation_text = ", ".join(citation_keys)
            for point_index, point in enumerate(points, start=1):
                claim_text = latex_escape(compact_whitespace(point.get("claim")) or "TODO")
                evidence_text = latex_escape(compact_whitespace(point.get("evidence")) or "TODO")
                limitation_text = latex_escape(compact_whitespace(point.get("limitation_or_boundary")) or "TODO")
                lines.extend(
                    [
                        f"\\paragraph{{Point {point_index}}}",
                        f"\\textbf{{Claim.}} {claim_text}\\\\",
                        f"\\textbf{{Evidence.}} {evidence_text}\\\\",
                        f"\\textbf{{Limitation/Boundary.}} {limitation_text}",
                    ]
                )
                if citation_text:
                    lines.append(f"\\textit{{Related citations:}} \\cite{{{citation_text}}}")
                lines.append("")
        lines.append("")
        section_contents.append((section_path, "\n".join(lines)))

    figure_files: list[Path] = []
    if figures_dir.exists():
        for path in sorted(figures_dir.iterdir()):
            if path.is_file() and path.suffix.lower() in {".png", ".jpg", ".jpeg", ".pdf", ".eps", ".svg"}:
                figure_files.append(path)
    figures_lines = [
        "\\section{Figure References}",
        "\\label{sec:figures}",
        "",
    ]
    if not figure_files:
        figures_lines.append("% No figures found. Add files under project-root figures/ directory.")
    else:
        for index, figure_path in enumerate(figure_files, start=1):
            base = figure_path.name
            label = slugify_text(figure_path.stem, default=f"fig-{index}")
            figures_lines.extend(
                [
                    "\\begin{figure}[htbp]",
                    "\\centering",
                    f"\\includegraphics[width=0.8\\linewidth]{{{latex_escape(base)}}}",
                    f"\\caption{{TODO: caption for {latex_escape(figure_path.stem)}}}",
                    f"\\label{{fig:{label}}}",
                    "\\end{figure}",
                    "",
                ]
            )

    tables_dir.mkdir(parents=True, exist_ok=True)
    csv_sources = sorted([path for path in tables_dir.glob("*.csv") if path.is_file()])
    if not csv_sources:
        placeholder_csv = tables_dir / "table_manifest.csv"
        if not placeholder_csv.exists():
            placeholder_csv.write_text(
                "table_id,title,value\nplaceholder,TODO table,0\n",
                encoding="utf-8",
            )
        csv_sources = [placeholder_csv]
        warnings.append({"source": "tables_dir", "message": "No CSV found; created placeholder table_manifest.csv"})

    table_input_lines = [
        "\\section{Table References}",
        "\\label{sec:tables}",
        "",
    ]
    table_tex_contents: list[tuple[Path, str]] = []
    for csv_source in csv_sources:
        stem = slugify_text(csv_source.stem, default="table")
        table_tex_path = latex_dir / "tables" / f"{stem}.tex"
        caption = f"TODO: caption for {csv_source.stem}"
        tex_content = render_csv_table_to_tex(csv_source, caption=caption, label=stem)
        table_tex_contents.append((table_tex_path, tex_content))
        table_input_lines.append(f"\\input{{tables/{stem}.tex}}")
    table_input_lines.append("")

    outline_lines = ["% Auto-generated outline include file", ""]
    for file_name in section_files:
        outline_lines.append(f"\\input{{sections/{file_name}}}")
    outline_lines.append("\\input{sections/figures.tex}")
    outline_lines.append("\\input{sections/tables.tex}")
    outline_lines.append("")

    main_text = "\n".join(
        [
            "\\documentclass[11pt]{article}",
            "",
            "\\usepackage[margin=1in]{geometry}",
            "\\usepackage{graphicx}",
            "\\usepackage{booktabs}",
            "\\usepackage[hidelinks]{hyperref}",
            "\\usepackage[backend=biber,style=numeric]{biblatex}",
            "\\addbibresource{references.bib}",
            "\\graphicspath{{../../figures/}{../figures/}{figures/}}",
            "",
            "\\title{Draft Manuscript Skeleton}",
            "\\author{TODO}",
            "\\date{\\today}",
            "",
            "\\begin{document}",
            "\\maketitle",
            "\\tableofcontents",
            "",
            "\\input{outline.tex}",
            "",
            "\\printbibliography",
            "\\end{document}",
            "",
        ]
    )

    write_results: dict[str, str] = {}
    try:
        write_results[str(latex_dir / "main.tex")] = write_text_if_allowed(latex_dir / "main.tex", main_text, overwrite)
        write_results[str(latex_dir / "outline.tex")] = write_text_if_allowed(
            latex_dir / "outline.tex", "\n".join(outline_lines), overwrite
        )
        write_results[str(latex_dir / "references.bib")] = write_text_if_allowed(
            latex_dir / "references.bib", bibliography_text, overwrite
        )
        write_results[str(latex_dir / "sections" / "figures.tex")] = write_text_if_allowed(
            latex_dir / "sections" / "figures.tex", "\n".join(figures_lines) + "\n", overwrite
        )
        write_results[str(latex_dir / "sections" / "tables.tex")] = write_text_if_allowed(
            latex_dir / "sections" / "tables.tex", "\n".join(table_input_lines) + "\n", overwrite
        )
        for path, content in section_contents:
            write_results[str(path)] = write_text_if_allowed(path, content, overwrite)
        for path, content in table_tex_contents:
            write_results[str(path)] = write_text_if_allowed(path, content, overwrite)
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "argument_graph_json": str(argument_graph_path),
            "claims_jsonl": str(claims_path),
            "included_candidates_csv": str(included_path),
            "records_jsonl": str(records_path),
            "refs_bib": str(refs_bib_path),
            "latex_dir": str(latex_dir),
            "figures_dir": str(figures_dir),
            "tables_dir": str(tables_dir),
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "main_tex": str(latex_dir / "main.tex"),
            "outline_tex": str(latex_dir / "outline.tex"),
            "sections_count": len(section_files),
            "references_bib": str(latex_dir / "references.bib"),
            "references_count": len(bibliography_entries),
            "figure_fragment_count": len(figure_files),
            "table_fragment_count": len(table_tex_contents),
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "generate_latex_draft_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "generate_latex_draft_completed",
        {
            "run_id": run_id,
            "status": status,
            "sections_count": len(section_files),
            "references_count": len(bibliography_entries),
            "figure_fragment_count": len(figure_files),
            "table_fragment_count": len(table_tex_contents),
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "sections_count": len(section_files),
                "references_count": len(bibliography_entries),
                "figure_fragment_count": len(figure_files),
                "table_fragment_count": len(table_tex_contents),
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def section_citation_audit(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    strictness = compact_whitespace(args.strictness).lower() or "soft"
    section_tex_path = Path(args.section_tex).resolve() if args.section_tex else Path()
    bib_path = Path(args.bib_path).resolve() if args.bib_path else (paths.root / "draft" / "latex" / "references.bib")
    evidence_packets_dir = (
        Path(args.evidence_packets_dir).resolve()
        if args.evidence_packets_dir
        else (paths.root / "draft" / "evidence_packets")
    )
    claims_path = Path(args.claims_jsonl).resolve() if args.claims_jsonl else (paths.root / "references" / "index" / "claims.jsonl")
    records_path = Path(args.records_jsonl).resolve() if args.records_jsonl else paths.records_jsonl
    if args.section_drafts_dir:
        section_drafts_dir = Path(args.section_drafts_dir).resolve()
    else:
        discovered = resolve_latest_section_drafts_dir(paths.runs_dir, run_dir)
        section_drafts_dir = discovered if discovered else Path()
    audit_output_dir = Path(args.audit_output_dir).resolve() if args.audit_output_dir else (paths.root / "draft" / "latex" / "audit")

    if strictness not in {"soft", "hard"}:
        status = "failed"
        errors.append({"source": "strictness", "message": f"Unsupported strictness: {strictness}"})

    if not section_tex_path.exists():
        status = "failed"
        errors.append({"source": "section_tex", "message": f"Section tex file not found: {section_tex_path}"})
    section_stem = section_tex_path.stem if section_tex_path else ""

    if not bib_path.exists():
        status = "failed"
        errors.append({"source": "bib_path", "message": f"Bib file not found: {bib_path}"})

    if not claims_path.exists():
        status = "failed"
        errors.append({"source": "claims_jsonl", "message": f"Claims JSONL not found: {claims_path}"})

    if not records_path.exists():
        status = "failed"
        errors.append({"source": "records_jsonl", "message": f"Records JSONL not found: {records_path}"})

    section_packet_dir = evidence_packets_dir / section_stem if section_stem else evidence_packets_dir
    if not section_packet_dir.exists():
        status = "failed"
        errors.append(
            {
                "source": "evidence_packets_dir",
                "message": f"Section evidence packet directory not found: {section_packet_dir}",
            }
        )

    if section_drafts_dir and not section_drafts_dir.exists():
        warnings.append({"source": "section_drafts_dir", "message": f"Section drafts directory not found: {section_drafts_dir}"})
        section_drafts_dir = Path()
    if not section_drafts_dir:
        warnings.append({"source": "section_drafts_dir", "message": "No section_drafts directory resolved; fallback to paragraph order."})

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "section_tex": str(section_tex_path),
                "bib_path": str(bib_path),
                "evidence_packets_dir": str(evidence_packets_dir),
                "claims_jsonl": str(claims_path),
                "records_jsonl": str(records_path),
                "section_drafts_dir": str(section_drafts_dir),
                "audit_output_dir": str(audit_output_dir),
                "strictness": strictness,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    section_text = section_tex_path.read_text(encoding="utf-8")
    section_title, blocks = extract_section_blocks(section_tex_path)
    if not section_title:
        section_title = section_stem
    filtered_blocks: list[dict[str, Any]] = []
    for block in blocks:
        claim_text = compact_whitespace(block.get("claim_text"))
        citation_keys = block.get("citation_keys") if isinstance(block.get("citation_keys"), list) else []
        raw_text = compact_whitespace(block.get("raw_text"))
        if not claim_text and not citation_keys and not bool(block.get("is_important")) and "\\paragraph{" not in raw_text:
            continue
        filtered_blocks.append(block)
    blocks = filtered_blocks

    bib_metadata = parse_bib_entry_metadata(bib_path.read_text(encoding="utf-8"))
    records = load_records(records_path)
    claims_rows = load_jsonl(claims_path)

    claim_by_id: dict[str, dict[str, Any]] = {}
    claims_by_candidate: dict[str, list[dict[str, Any]]] = {}
    for row in claims_rows:
        claim_id = compact_whitespace(row.get("claim_id"))
        if claim_id:
            claim_by_id[claim_id] = row
        candidate_id = compact_whitespace(row.get("candidate_id"))
        if candidate_id:
            claims_by_candidate.setdefault(candidate_id, []).append(row)

    paragraph_rows = load_section_draft_paragraphs(section_drafts_dir, section_stem) if section_drafts_dir else []
    paragraph_ids_by_index = [compact_whitespace(row.get("paragraph_id")) for row in paragraph_rows]

    packet_files = sorted([path for path in section_packet_dir.glob("*.json") if path.is_file()])
    if not packet_files:
        status = "failed"
        errors.append({"source": "evidence_packets_dir", "message": f"No packet files under: {section_packet_dir}"})
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "section_tex": str(section_tex_path),
                "bib_path": str(bib_path),
                "evidence_packets_dir": str(evidence_packets_dir),
                "claims_jsonl": str(claims_path),
                "records_jsonl": str(records_path),
                "section_drafts_dir": str(section_drafts_dir),
                "audit_output_dir": str(audit_output_dir),
                "strictness": strictness,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    packet_by_paragraph: dict[str, dict[str, Any]] = {}
    packets_in_order: list[dict[str, Any]] = []
    for packet_file in packet_files:
        try:
            payload = json.loads(packet_file.read_text(encoding="utf-8"))
        except Exception as exc:
            warnings.append({"source": "packet_json", "message": f"Failed to parse {packet_file}: {exc}"})
            continue
        if not isinstance(payload, dict):
            continue
        paragraph_id = compact_whitespace(payload.get("paragraph_id")) or packet_file.stem
        payload["_packet_path"] = str(packet_file)
        payload["_paragraph_id"] = paragraph_id
        packets_in_order.append(payload)
        if paragraph_id and paragraph_id not in packet_by_paragraph:
            packet_by_paragraph[paragraph_id] = payload

    record_tokens_by_key: dict[str, set[str]] = {}
    for record in records:
        keys = [
            compact_whitespace(record.get("citation_key")),
            compact_whitespace(record.get("paper_id")),
        ]
        token_source = " ".join(
            [
                compact_whitespace(record.get("title")),
                compact_whitespace(record.get("abstract")),
            ]
        )
        token_set = set(tokenize_terms(token_source))
        for key in keys:
            if key:
                existing = record_tokens_by_key.get(key, set())
                existing.update(token_set)
                record_tokens_by_key[key] = existing

    findings: list[dict[str, Any]] = []
    finding_counter = 1
    key_claim_count = 0
    supported_keys: set[str] = set()
    cited_keys: set[str] = set()

    def push_finding(
        finding_type: str,
        severity: str,
        paragraph_id: str,
        claim_text: str,
        citation_keys: list[str],
        message: str,
        evidence_refs: list[str],
    ) -> None:
        nonlocal finding_counter
        findings.append(
            {
                "finding_id": f"SF{finding_counter:04d}",
                "type": finding_type,
                "severity": severity,
                "paragraph_id": paragraph_id,
                "claim_text": claim_text,
                "citation_keys": citation_keys,
                "message": message,
                "evidence_refs": evidence_refs,
            }
        )
        finding_counter += 1

    for block_index, block in enumerate(blocks):
        paragraph_id = ""
        if block_index < len(paragraph_ids_by_index):
            paragraph_id = compact_whitespace(paragraph_ids_by_index[block_index])
        packet: dict[str, Any] | None = None
        if paragraph_id and paragraph_id in packet_by_paragraph:
            packet = packet_by_paragraph[paragraph_id]
        elif block_index < len(packets_in_order):
            packet = packets_in_order[block_index]
            if not paragraph_id:
                paragraph_id = compact_whitespace(packet.get("_paragraph_id"))
        if not paragraph_id:
            paragraph_id = compact_whitespace(block.get("block_id")) or f"P{block_index + 1:03d}"

        claim_text = compact_whitespace(block.get("claim_text"))
        citation_keys = [compact_whitespace(str(item)) for item in (block.get("citation_keys") or []) if compact_whitespace(str(item))]
        cited_keys.update(citation_keys)
        is_important = bool(block.get("is_important"))

        if citation_keys and not is_important:
            push_finding(
                finding_type="isolated_citation",
                severity="medium",
                paragraph_id=paragraph_id,
                claim_text=claim_text,
                citation_keys=citation_keys,
                message="Citation appears without a key claim anchor in this paragraph.",
                evidence_refs=[],
            )

        if is_important:
            key_claim_count += 1
            if not citation_keys:
                push_finding(
                    finding_type="coverage",
                    severity="high",
                    paragraph_id=paragraph_id,
                    claim_text=claim_text,
                    citation_keys=[],
                    message="Key claim has no citation.",
                    evidence_refs=[],
                )

        support_refs = packet.get("supporting_references") if isinstance(packet, dict) else []
        if not isinstance(support_refs, list):
            support_refs = []

        packet_refs_by_key: dict[str, set[str]] = {}
        for support in support_refs:
            if not isinstance(support, dict):
                continue
            support_key = compact_whitespace(support.get("citation_key"))
            if not support_key:
                continue
            key_tokens = packet_refs_by_key.get(support_key, set())
            for point in support.get("support_points") or []:
                point_text = compact_whitespace(str(point))
                if point_text:
                    key_tokens.update(tokenize_terms(point_text))
            claim_id = compact_whitespace(support.get("claim_id"))
            if claim_id and claim_id in claim_by_id:
                mapped_claim = claim_by_id[claim_id]
                key_tokens.update(tokenize_terms(compact_whitespace(mapped_claim.get("claim_text"))))
                key_tokens.update(tokenize_terms(compact_whitespace(mapped_claim.get("evidence_snippet"))))
            candidate_id = compact_whitespace(support.get("candidate_id"))
            if candidate_id and candidate_id in claims_by_candidate:
                for candidate_claim in claims_by_candidate[candidate_id]:
                    key_tokens.update(tokenize_terms(compact_whitespace(candidate_claim.get("claim_text"))))
            packet_refs_by_key[support_key] = key_tokens

        supported_in_block: list[str] = []
        claim_tokens = set(tokenize_terms(claim_text))
        for citation_key in citation_keys:
            evidence_tokens = set(record_tokens_by_key.get(citation_key, set()))
            evidence_tokens.update(packet_refs_by_key.get(citation_key, set()))
            overlap = len(claim_tokens.intersection(evidence_tokens))
            if overlap >= 2 or (overlap >= 1 and len(claim_tokens) <= 4):
                supported_in_block.append(citation_key)
                supported_keys.add(citation_key)

        if is_important and citation_keys and not supported_in_block:
            evidence_refs = [compact_whitespace((packet or {}).get("_packet_path"))] if packet else []
            push_finding(
                finding_type="support",
                severity="high",
                paragraph_id=paragraph_id,
                claim_text=claim_text,
                citation_keys=citation_keys,
                message="Citations do not provide sufficient support overlap for this key claim.",
                evidence_refs=[item for item in evidence_refs if item],
            )

        if is_important and is_strong_claim_text(claim_text):
            strength_label, strength_score = parse_packet_strength(packet or {})
            weak_strength = strength_label == "weak" or strength_score < 50 or len(support_refs) == 0
            if weak_strength:
                evidence_refs = [compact_whitespace((packet or {}).get("_packet_path"))] if packet else []
                push_finding(
                    finding_type="strong_claim_weak_evidence",
                    severity="high",
                    paragraph_id=paragraph_id,
                    claim_text=claim_text,
                    citation_keys=citation_keys,
                    message="Strong claim tone is paired with weak evidence strength.",
                    evidence_refs=[item for item in evidence_refs if item],
                )

        if is_important and is_overgeneralization(claim_text, compact_whitespace(block.get("raw_text"))):
            push_finding(
                finding_type="overgeneralization",
                severity="high",
                paragraph_id=paragraph_id,
                claim_text=claim_text,
                citation_keys=citation_keys,
                message="Absolute claim language lacks explicit boundary or uncertainty qualifiers.",
                evidence_refs=[],
            )

    cited_not_used_keys = sorted([key for key in cited_keys if key and key not in supported_keys])
    for key in cited_not_used_keys:
        if key in bib_metadata:
            message = f"Cited key is not used to support any key claim in this section: {key}"
        else:
            message = f"Cited key is unresolved in bibliography and unsupported for key claims: {key}"
        push_finding(
            finding_type="cited_not_used",
            severity="medium",
            paragraph_id="",
            claim_text="",
            citation_keys=[key],
            message=message,
            evidence_refs=[],
        )

    findings = sorted(findings, key=lambda row: row["finding_id"])
    finding_counts_by_type: dict[str, int] = {finding_type: 0 for finding_type in SECTION_AUDIT_FINDING_TYPES}
    high_risk_finding_count = 0
    for row in findings:
        finding_type = compact_whitespace(row.get("type"))
        if finding_type:
            finding_counts_by_type[finding_type] = finding_counts_by_type.get(finding_type, 0) + 1
        if compact_whitespace(row.get("severity")).lower() == "high":
            high_risk_finding_count += 1

    score, risk_level = score_section_citation_findings(findings)
    if strictness == "hard" and high_risk_finding_count > 0:
        status = "failed"

    summary = {
        "section_stem": section_stem,
        "key_claim_count": key_claim_count,
        "finding_counts_by_type": finding_counts_by_type,
        "score": score,
        "risk_level": risk_level,
    }
    citation_sets = {
        "cited_keys": sorted(cited_keys),
        "supported_keys": sorted(supported_keys),
        "cited_not_used_keys": cited_not_used_keys,
    }
    report_payload = {
        "run_id": run_id,
        "timestamp": now_iso,
        "strictness": strictness,
        "section_stem": section_stem,
        "section_title": section_title,
        "summary": summary,
        "findings": findings,
        "citation_sets": citation_sets,
        "warnings": warnings,
        "errors": errors,
    }

    audit_output_dir.mkdir(parents=True, exist_ok=True)
    audit_json_path = audit_output_dir / f"section_{section_stem}_audit.json"
    audit_md_path = audit_output_dir / f"section_{section_stem}_audit.md"
    try:
        audit_json_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        audit_md_path.write_text(
            render_section_citation_audit_report(
                section_stem=section_stem,
                section_title=section_title,
                summary=summary,
                findings=findings,
                citation_sets=citation_sets,
            ),
            encoding="utf-8",
        )
    except Exception as exc:
        status = "failed"
        errors.append({"source": "audit_output", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "section_tex": str(section_tex_path),
            "bib_path": str(bib_path),
            "evidence_packets_dir": str(evidence_packets_dir),
            "claims_jsonl": str(claims_path),
            "records_jsonl": str(records_path),
            "section_drafts_dir": str(section_drafts_dir),
            "audit_output_dir": str(audit_output_dir),
            "strictness": strictness,
        },
        "status": status,
        "outputs": {
            "audited_section_count": 1,
            "key_claim_count": key_claim_count,
            "finding_counts_by_type": finding_counts_by_type,
            "high_risk_finding_count": high_risk_finding_count,
            "score": score,
            "risk_level": risk_level,
            "section_audit_json": str(audit_json_path),
            "section_audit_md": str(audit_md_path),
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "section_citation_audit_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "section_citation_audit_completed",
        {
            "run_id": run_id,
            "status": status,
            "section_stem": section_stem,
            "score": score,
            "risk_level": risk_level,
            "finding_count": len(findings),
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "summary": summary,
                "outputs": {
                    "section_audit_json": str(audit_json_path),
                    "section_audit_md": str(audit_md_path),
                },
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def citation_audit(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"
    strictness = compact_whitespace(args.strictness).lower() or "soft"

    latex_dir = Path(args.latex_dir).resolve() if args.latex_dir else (paths.root / "draft" / "latex")
    main_tex_path = Path(args.main_tex).resolve() if args.main_tex else (latex_dir / "main.tex")
    outline_tex_path = Path(args.outline_tex).resolve() if args.outline_tex else (latex_dir / "outline.tex")
    bib_path = Path(args.bib_path).resolve() if args.bib_path else (latex_dir / "references.bib")
    argument_graph_path = (
        Path(args.argument_graph_json).resolve()
        if args.argument_graph_json
        else (paths.root / "references" / "index" / "argument_graph.json")
    )
    claims_path = (
        Path(args.claims_jsonl).resolve()
        if args.claims_jsonl
        else (paths.root / "references" / "index" / "claims.jsonl")
    )
    records_path = Path(args.records_jsonl).resolve() if args.records_jsonl else paths.records_jsonl
    included_path = (
        Path(args.included_candidates_csv).resolve()
        if args.included_candidates_csv
        else paths.included_candidates_csv
    )
    overrides_path = (
        Path(args.audit_overrides_json).resolve()
        if args.audit_overrides_json
        else (latex_dir / "audit_overrides.json")
    )
    output_dir = Path(args.audit_output_dir).resolve() if args.audit_output_dir else (latex_dir / "audit")

    if strictness not in {"soft", "hard"}:
        status = "failed"
        errors.append({"source": "strictness", "message": f"Unsupported strictness: {strictness}"})

    sections_dir = latex_dir / "sections"
    section_files = sorted([path for path in sections_dir.glob("sec_*.tex") if path.is_file()])
    if not section_files:
        status = "failed"
        errors.append({"source": "latex_sections", "message": f"No section files found under: {sections_dir}"})

    if not bib_path.exists():
        status = "failed"
        errors.append({"source": "bib_path", "message": f"Bib file not found: {bib_path}"})
        bib_text = ""
    else:
        bib_text = bib_path.read_text(encoding="utf-8")

    if not argument_graph_path.exists():
        status = "failed"
        errors.append({"source": "argument_graph_json", "message": f"File not found: {argument_graph_path}"})
        argument_graph: dict[str, Any] = {}
    else:
        try:
            argument_graph = json.loads(argument_graph_path.read_text(encoding="utf-8"))
        except Exception as exc:
            argument_graph = {}
            status = "failed"
            errors.append({"source": "argument_graph_json", "message": str(exc)})

    try:
        claims_rows = load_jsonl(claims_path)
    except Exception as exc:
        claims_rows = []
        status = "failed"
        errors.append({"source": "claims_jsonl", "message": str(exc)})

    try:
        records = load_records(records_path)
    except Exception as exc:
        records = []
        status = "failed"
        errors.append({"source": "records_jsonl", "message": str(exc)})

    try:
        included_rows = read_csv_rows(included_path)
    except Exception as exc:
        included_rows = []
        status = "failed"
        errors.append({"source": "included_candidates_csv", "message": str(exc)})

    overrides: dict[str, Any] = {}
    if overrides_path.exists():
        try:
            decoded = json.loads(overrides_path.read_text(encoding="utf-8"))
            if isinstance(decoded, dict):
                overrides = decoded
        except Exception as exc:
            status = "failed"
            errors.append({"source": "audit_overrides_json", "message": str(exc)})
    else:
        warnings.append({"source": "audit_overrides_json", "message": f"Optional override file not found: {overrides_path}"})

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "latex_dir": str(latex_dir),
                "main_tex": str(main_tex_path),
                "outline_tex": str(outline_tex_path),
                "bib_path": str(bib_path),
                "argument_graph_json": str(argument_graph_path),
                "claims_jsonl": str(claims_path),
                "records_jsonl": str(records_path),
                "included_candidates_csv": str(included_path),
                "audit_overrides_json": str(overrides_path),
                "audit_output_dir": str(output_dir),
                "strictness": strictness,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    section_plan = argument_graph.get("section_plan", []) if isinstance(argument_graph, dict) else []
    if not isinstance(section_plan, list):
        section_plan = []

    claims_by_candidate: dict[str, list[dict[str, Any]]] = {}
    for claim in claims_rows:
        candidate_id = compact_whitespace(claim.get("candidate_id"))
        if candidate_id:
            claims_by_candidate.setdefault(candidate_id, []).append(claim)

    records_index = index_records(records)
    include_by_candidate: dict[str, dict[str, Any]] = {}
    for row in included_rows:
        candidate_id = compact_whitespace(row.get("candidate_id"))
        if candidate_id and candidate_id not in include_by_candidate:
            include_by_candidate[candidate_id] = row

    citation_key_to_candidate: dict[str, str] = {}
    evidence_tokens_by_citation_key: dict[str, set[str]] = {}
    for candidate_id, include_row in include_by_candidate.items():
        candidate_stub = {
            "doi": compact_whitespace(include_row.get("doi")),
            "arxiv_id": compact_whitespace(include_row.get("arxiv_id")),
            "title": compact_whitespace(include_row.get("title")),
            "year": str(include_row.get("year") or ""),
        }
        record = matched_record_for_candidate(candidate_stub, records_index)
        citation_key = stable_citation_key(candidate_id, record)
        citation_key_to_candidate[citation_key] = candidate_id
        token_source_parts = [
            compact_whitespace((record or {}).get("title")),
            compact_whitespace((record or {}).get("abstract")),
        ]
        candidate_claims = claims_by_candidate.get(candidate_id, [])
        token_source_parts.extend([compact_whitespace(claim.get("claim_text")) for claim in candidate_claims])
        token_pool = set(tokenize_terms(" ".join([part for part in token_source_parts if part])))
        evidence_tokens_by_citation_key[citation_key] = token_pool

    bib_metadata = parse_bib_entry_metadata(bib_text)
    bib_keys = sorted(bib_metadata.keys())

    findings: list[dict[str, Any]] = []
    finding_counter = 1

    def push_finding(
        category: str,
        severity: str,
        section_id: str,
        section_title: str,
        file_path: str,
        claim_text: str,
        citation_keys: list[str],
        message: str,
    ) -> None:
        nonlocal finding_counter
        findings.append(
            {
                "finding_id": f"F{finding_counter:04d}",
                "category": category,
                "severity": severity,
                "section_id": section_id,
                "section_title": section_title,
                "file_path": file_path,
                "claim_text": claim_text,
                "citation_keys": citation_keys,
                "message": message,
            }
        )
        finding_counter += 1

    important_claim_count = 0
    coverage_missing_count = 0
    support_failed_count = 0

    all_text_citations: set[str] = set()
    for tex_path in [main_tex_path, outline_tex_path]:
        if tex_path.exists():
            all_text_citations.update(extract_citation_keys(tex_path.read_text(encoding="utf-8")))
        else:
            warnings.append({"source": "latex_path", "message": f"Optional tex file missing: {tex_path}"})

    section_scores: list[dict[str, Any]] = []
    for index, section_file in enumerate(sorted(section_files), start=1):
        mapped_section = section_plan[index - 1] if index - 1 < len(section_plan) else {}
        section_id = compact_whitespace(mapped_section.get("section_id")) or f"SEC{index:03d}"
        mapped_title = compact_whitespace(mapped_section.get("section_title"))
        parsed_title, blocks = extract_section_blocks(section_file)
        blocks = apply_audit_overrides(blocks, overrides)
        section_title = mapped_title or parsed_title or section_file.stem

        section_findings_before = len(findings)
        for block in blocks:
            all_text_citations.update(block.get("citation_keys") or [])
            claim_text = compact_whitespace(block.get("claim_text"))
            citation_keys = block.get("citation_keys") or []
            if not block.get("is_important"):
                continue
            important_claim_count += 1
            if not citation_keys:
                coverage_missing_count += 1
                push_finding(
                    category="coverage",
                    severity="high",
                    section_id=section_id,
                    section_title=section_title,
                    file_path=str(section_file),
                    claim_text=claim_text,
                    citation_keys=[],
                    message="Important claim has no citation.",
                )
                continue

            claim_tokens = set(tokenize_terms(claim_text))
            supported_keys: list[str] = []
            for key in citation_keys:
                evidence_tokens = evidence_tokens_by_citation_key.get(key, set())
                overlap = len(claim_tokens.intersection(evidence_tokens))
                if overlap >= 2 or (overlap >= 1 and len(claim_tokens) <= 4):
                    supported_keys.append(key)
            if not supported_keys:
                support_failed_count += 1
                push_finding(
                    category="support",
                    severity="high",
                    section_id=section_id,
                    section_title=section_title,
                    file_path=str(section_file),
                    claim_text=claim_text,
                    citation_keys=citation_keys,
                    message="Citations do not have sufficient evidence overlap for this claim.",
                )

        section_findings = findings[section_findings_before:]
        section_score, section_risk = score_from_findings(section_findings)
        section_scores.append(
            {
                "section_id": section_id,
                "section_title": section_title,
                "score": section_score,
                "risk_level": section_risk,
                "finding_count": len(section_findings),
            }
        )

    text_only_keys = sorted([key for key in all_text_citations if key not in bib_metadata])
    bib_unused_keys = sorted([key for key in bib_metadata.keys() if key not in all_text_citations])

    for key in text_only_keys:
        push_finding(
            category="text_only",
            severity="high",
            section_id="GLOBAL",
            section_title="Global",
            file_path=str(main_tex_path),
            claim_text="",
            citation_keys=[key],
            message=f"Citation key appears in text but not in bib: {key}",
        )

    for key in bib_unused_keys:
        push_finding(
            category="bib_unused",
            severity="medium",
            section_id="GLOBAL",
            section_title="Global",
            file_path=str(bib_path),
            claim_text="",
            citation_keys=[key],
            message=f"Bib entry not used in text: {key}",
        )

    bib_missing_field_count = 0
    for key, metadata in sorted(bib_metadata.items()):
        entry_type = compact_whitespace(metadata.get("entry_type")).lower()
        fields = set(metadata.get("fields") or [])
        missing_fields: list[str] = []
        if entry_type == "article":
            for required in ("title", "author", "year", "journal"):
                if required not in fields:
                    missing_fields.append(required)
        elif entry_type == "misc":
            for required in ("title", "author", "year"):
                if required not in fields:
                    missing_fields.append(required)
            if not any(optional in fields for optional in ("doi", "url", "eprint")):
                missing_fields.append("doi|url|eprint")
        else:
            for required in ("title", "author", "year"):
                if required not in fields:
                    missing_fields.append(required)
        if missing_fields:
            bib_missing_field_count += 1
            push_finding(
                category="bib_fields",
                severity="medium",
                section_id="GLOBAL",
                section_title="Global",
                file_path=str(bib_path),
                claim_text="",
                citation_keys=[key],
                message=f"Bib entry {key} missing fields: {', '.join(missing_fields)}",
            )

    findings = sorted(findings, key=lambda row: row["finding_id"])
    overall_score, overall_risk = score_from_findings(findings)
    severity_counts = {
        "high": len([row for row in findings if row["severity"] == "high"]),
        "medium": len([row for row in findings if row["severity"] == "medium"]),
        "low": len([row for row in findings if row["severity"] == "low"]),
    }

    if strictness == "hard" and severity_counts["high"] > 0:
        status = "failed"

    audit_payload = {
        "strictness": strictness,
        "timestamp": now_iso,
        "overall": {
            "score": overall_score,
            "risk_level": overall_risk,
            "findings_count": len(findings),
            "severity_counts": severity_counts,
        },
        "section_scores": section_scores,
        "summary_counts": {
            "important_claim_count": important_claim_count,
            "coverage_missing_count": coverage_missing_count,
            "support_failed_count": support_failed_count,
            "bib_missing_field_count": bib_missing_field_count,
            "text_only_count": len(text_only_keys),
            "bib_unused_count": len(bib_unused_keys),
        },
        "citation_sets": {
            "text_cited_keys": sorted(all_text_citations),
            "bib_keys": bib_keys,
            "text_only_keys": text_only_keys,
            "bib_unused_keys": bib_unused_keys,
        },
        "evidence_mapping": {
            "citation_key_to_candidate": citation_key_to_candidate,
            "mapped_citation_count": len(citation_key_to_candidate),
        },
        "findings": findings,
        "warnings": warnings,
        "errors": errors,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    findings_csv_path = output_dir / "citation_findings.csv"
    audit_json_path = output_dir / "citation_audit.json"
    report_md_path = output_dir / "citation_audit_report.md"

    finding_rows = [
        {
            "finding_id": row["finding_id"],
            "category": row["category"],
            "severity": row["severity"],
            "section_id": row["section_id"],
            "section_title": row["section_title"],
            "file_path": row["file_path"],
            "claim_text": row["claim_text"],
            "citation_keys": ",".join(row["citation_keys"]),
            "message": row["message"],
        }
        for row in findings
    ]

    try:
        write_csv_rows(findings_csv_path, CITATION_AUDIT_FINDINGS_COLUMNS, finding_rows)
        audit_json_path.write_text(json.dumps(audit_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        report_md_path.write_text(
            render_citation_audit_report(
                overall_score=overall_score,
                overall_risk=overall_risk,
                section_scores=section_scores,
                findings=findings,
            ),
            encoding="utf-8",
        )
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "latex_dir": str(latex_dir),
            "main_tex": str(main_tex_path),
            "outline_tex": str(outline_tex_path),
            "bib_path": str(bib_path),
            "argument_graph_json": str(argument_graph_path),
            "claims_jsonl": str(claims_path),
            "records_jsonl": str(records_path),
            "included_candidates_csv": str(included_path),
            "audit_overrides_json": str(overrides_path),
            "audit_output_dir": str(output_dir),
            "strictness": strictness,
        },
        "status": status,
        "outputs": {
            "citation_audit_json": str(audit_json_path),
            "citation_findings_csv": str(findings_csv_path),
            "citation_audit_report_md": str(report_md_path),
            "important_claim_count": important_claim_count,
            "coverage_missing_count": coverage_missing_count,
            "support_failed_count": support_failed_count,
            "bib_missing_field_count": bib_missing_field_count,
            "text_only_count": len(text_only_keys),
            "bib_unused_count": len(bib_unused_keys),
            "overall_score": overall_score,
            "overall_risk_level": overall_risk,
            "severity_counts": severity_counts,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "citation_audit_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "citation_audit_completed",
        {
            "run_id": run_id,
            "status": status,
            "overall_score": overall_score,
            "overall_risk": overall_risk,
            "findings_count": len(findings),
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "overall_score": overall_score,
                "overall_risk_level": overall_risk,
                "findings_count": len(findings),
                "outputs": {
                    "citation_audit_json": str(audit_json_path),
                    "citation_findings_csv": str(findings_csv_path),
                    "citation_audit_report_md": str(report_md_path),
                },
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def render_section_gate_fixlist(
    section_stem: str,
    decision: str,
    score: int,
    risk_level: str,
    must_fix: list[dict[str, str]],
    should_fix: list[dict[str, str]],
) -> str:
    lines = [
        f"# Section Release Gate: {section_stem}",
        "",
        f"- Decision: {decision}",
        f"- Score: {score}",
        f"- Risk Level: {risk_level}",
        "",
        "## Must Fix",
    ]
    if not must_fix:
        lines.append("- None.")
    else:
        for row in must_fix:
            lines.append(f"- [{row.get('source', 'unknown')}] {row.get('message', '')}")
    lines.extend(["", "## Should Fix"])
    if not should_fix:
        lines.append("- None.")
    else:
        for row in should_fix:
            lines.append(f"- [{row.get('source', 'unknown')}] {row.get('message', '')}")
    lines.append("")
    return "\n".join(lines)


def section_release_gate(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    strictness = compact_whitespace(args.strictness).lower() or "soft"
    overwrite = bool(args.overwrite)
    section_stem = compact_whitespace(args.section_stem)
    if not section_stem:
        status = "failed"
        errors.append({"source": "section_stem", "message": "section_stem is required."})

    if args.section_consistency_report_json:
        consistency_report_path = Path(args.section_consistency_report_json).resolve()
    else:
        discovered = resolve_latest_run_file(paths.runs_dir, run_dir, "section_consistency_report.json")
        consistency_report_path = discovered if discovered else (paths.runs_dir / "__missing_section_consistency_report.json")
    section_audit_json_path = (
        Path(args.section_audit_json).resolve()
        if args.section_audit_json
        else (paths.root / "draft" / "latex" / "audit" / f"section_{section_stem}_audit.json")
    )
    gate_output_json = (
        Path(args.gate_output_json).resolve()
        if args.gate_output_json
        else (paths.root / "draft" / "gates" / f"section_{section_stem}_gate.json")
    )
    gate_fixlist_md = (
        Path(args.gate_fixlist_md).resolve()
        if args.gate_fixlist_md
        else (paths.root / "draft" / "gates" / f"section_{section_stem}_fixlist.md")
    )

    if strictness not in {"soft", "hard"}:
        status = "failed"
        errors.append({"source": "strictness", "message": f"Unsupported strictness: {strictness}"})

    if not consistency_report_path or not consistency_report_path.exists() or not consistency_report_path.is_file():
        status = "failed"
        errors.append({"source": "section_consistency_report_json", "message": f"File not found: {consistency_report_path}"})
        consistency_payload: dict[str, Any] = {}
    else:
        try:
            decoded = load_json_file(consistency_report_path)
            consistency_payload = decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            status = "failed"
            errors.append({"source": "section_consistency_report_json", "message": str(exc)})
            consistency_payload = {}

    if not section_audit_json_path.exists() or not section_audit_json_path.is_file():
        status = "failed"
        errors.append({"source": "section_audit_json", "message": f"File not found: {section_audit_json_path}"})
        section_audit_payload: dict[str, Any] = {}
    else:
        try:
            decoded = load_json_file(section_audit_json_path)
            section_audit_payload = decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            status = "failed"
            errors.append({"source": "section_audit_json", "message": str(exc)})
            section_audit_payload = {}

    section_entry = {}
    for row in consistency_payload.get("sections", []) if isinstance(consistency_payload, dict) else []:
        if not isinstance(row, dict):
            continue
        if compact_whitespace(row.get("section_stem")) == section_stem:
            section_entry = row
            break
    if not section_entry and status == "ok":
        status = "failed"
        errors.append({"source": "section_consistency_report_json", "message": f"section_stem not found: {section_stem}"})

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "section_stem": section_stem,
                "section_consistency_report_json": str(consistency_report_path),
                "section_audit_json": str(section_audit_json_path),
                "gate_output_json": str(gate_output_json),
                "gate_fixlist_md": str(gate_fixlist_md),
                "strictness": strictness,
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    must_fix: list[dict[str, str]] = []
    should_fix: list[dict[str, str]] = []
    low_fix_count = 0
    for issue in section_entry.get("issues", []):
        if not isinstance(issue, dict):
            continue
        severity = normalize_issue_severity(issue.get("severity"))
        message = compact_whitespace(issue.get("message"))
        item = {"source": "consistency", "message": message or "Consistency issue"}
        if severity == "high":
            must_fix.append(item)
        elif severity == "medium":
            should_fix.append(item)
        else:
            low_fix_count += 1

    for finding in section_audit_payload.get("findings", []) if isinstance(section_audit_payload, dict) else []:
        if not isinstance(finding, dict):
            continue
        severity = normalize_issue_severity(finding.get("severity"))
        message = compact_whitespace(finding.get("message"))
        item = {"source": "citation_audit", "message": message or "Citation finding"}
        if severity == "high":
            must_fix.append(item)
        elif severity == "medium":
            should_fix.append(item)
        else:
            low_fix_count += 1

    score = 100
    score -= len(must_fix) * SECTION_GATE_DEDUCTIONS["high"]
    score -= len(should_fix) * SECTION_GATE_DEDUCTIONS["medium"]
    score -= low_fix_count * SECTION_GATE_DEDUCTIONS["low"]
    score = max(0, min(100, score))
    if score >= 85:
        risk_level = "low"
    elif score >= 60:
        risk_level = "medium"
    else:
        risk_level = "high"

    if must_fix:
        decision = "block"
    elif should_fix:
        decision = "revise"
    else:
        decision = "go"

    if strictness == "hard" and decision != "go":
        status = "failed"

    gate_payload = {
        "section_stem": section_stem,
        "timestamp": now_iso,
        "decision": decision,
        "score": score,
        "risk_level": risk_level,
        "must_fix": must_fix,
        "should_fix": should_fix,
        "consistency_report": str(consistency_report_path),
        "section_audit_json": str(section_audit_json_path),
    }
    fixlist_text = render_section_gate_fixlist(section_stem, decision, score, risk_level, must_fix, should_fix)

    write_results: dict[str, str] = {}
    try:
        if gate_output_json.exists() and not overwrite:
            write_results[str(gate_output_json)] = "skipped"
        else:
            gate_output_json.parent.mkdir(parents=True, exist_ok=True)
            gate_output_json.write_text(json.dumps(gate_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            write_results[str(gate_output_json)] = "written"
        write_results[str(gate_fixlist_md)] = write_text_if_allowed(gate_fixlist_md, fixlist_text, overwrite)
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "section_stem": section_stem,
            "section_consistency_report_json": str(consistency_report_path),
            "section_audit_json": str(section_audit_json_path),
            "gate_output_json": str(gate_output_json),
            "gate_fixlist_md": str(gate_fixlist_md),
            "strictness": strictness,
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "decision": decision,
            "score": score,
            "risk_level": risk_level,
            "must_fix_count": len(must_fix),
            "should_fix_count": len(should_fix),
            "gate_output_json": str(gate_output_json),
            "gate_fixlist_md": str(gate_fixlist_md),
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)
    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "outputs": manifest["outputs"],
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def generate_cross_section_bridges(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"
    overwrite = bool(args.overwrite)

    latex_sections_dir = (
        Path(args.latex_sections_dir).resolve()
        if args.latex_sections_dir
        else (paths.root / "draft" / "latex" / "sections")
    )
    argument_graph_path = (
        Path(args.argument_graph_json).resolve()
        if args.argument_graph_json
        else (paths.root / "references" / "index" / "argument_graph.json")
    )
    bridge_plan_json = (
        Path(args.bridge_plan_json).resolve()
        if args.bridge_plan_json
        else (paths.root / "draft" / "bridges" / "bridge_plan.json")
    )
    bridges_tex_path = (
        Path(args.bridges_tex).resolve()
        if args.bridges_tex
        else (paths.root / "draft" / "latex" / "sections" / "bridges.tex")
    )

    section_files = sorted([path for path in latex_sections_dir.glob("sec_*.tex") if path.is_file()], key=section_sort_key)
    if len(section_files) < 2:
        status = "failed"
        errors.append({"source": "latex_sections_dir", "message": f"Need at least two section files under: {latex_sections_dir}"})

    if not argument_graph_path.exists():
        status = "failed"
        errors.append({"source": "argument_graph_json", "message": f"File not found: {argument_graph_path}"})
        argument_graph: dict[str, Any] = {}
    else:
        try:
            decoded = load_json_file(argument_graph_path)
            argument_graph = decoded if isinstance(decoded, dict) else {}
        except Exception as exc:
            status = "failed"
            errors.append({"source": "argument_graph_json", "message": str(exc)})
            argument_graph = {}

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "latex_sections_dir": str(latex_sections_dir),
                "argument_graph_json": str(argument_graph_path),
                "bridge_plan_json": str(bridge_plan_json),
                "bridges_tex": str(bridges_tex_path),
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    section_titles = [extract_section_title_from_text(path.read_text(encoding="utf-8"), fallback=path.stem) for path in section_files]
    section_plan = argument_graph.get("section_plan", []) if isinstance(argument_graph, dict) else []
    if isinstance(section_plan, list) and len(section_plan) >= len(section_files):
        for idx, row in enumerate(section_plan[: len(section_files)]):
            if not isinstance(row, dict):
                continue
            title = compact_whitespace(row.get("section_title"))
            if title:
                section_titles[idx] = title
    elif not section_plan:
        warnings.append({"source": "argument_graph_json", "message": "section_plan missing; fallback to section tex order."})

    bridge_rows: list[dict[str, Any]] = []
    tex_lines = ["\\section*{Cross-Section Bridges}", "\\label{sec:cross-section-bridges}", ""]
    for idx in range(len(section_files) - 1):
        from_stem = section_files[idx].stem
        to_stem = section_files[idx + 1].stem
        from_title = section_titles[idx]
        to_title = section_titles[idx + 1]
        sentence = (
            f"Building on Section {idx + 1} ({from_title}), "
            f"the next section ({to_title}) narrows interpretation with bounded evidence continuity."
        )
        bridge_id = f"BRG{idx + 1:03d}"
        bridge_rows.append(
            {
                "bridge_id": bridge_id,
                "from_section_stem": from_stem,
                "from_section_title": from_title,
                "to_section_stem": to_stem,
                "to_section_title": to_title,
                "bridge_sentence": sentence,
            }
        )
        tex_lines.extend([f"\\paragraph{{Bridge {idx + 1}}}", sentence, ""])

    plan_payload = {
        "timestamp": now_iso,
        "section_count": len(section_files),
        "bridge_count": len(bridge_rows),
        "bridges": bridge_rows,
    }

    write_results: dict[str, str] = {}
    try:
        if bridge_plan_json.exists() and not overwrite:
            write_results[str(bridge_plan_json)] = "skipped"
        else:
            bridge_plan_json.parent.mkdir(parents=True, exist_ok=True)
            bridge_plan_json.write_text(json.dumps(plan_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            write_results[str(bridge_plan_json)] = "written"
        write_results[str(bridges_tex_path)] = write_text_if_allowed(bridges_tex_path, "\n".join(tex_lines) + "\n", overwrite)
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "latex_sections_dir": str(latex_sections_dir),
            "argument_graph_json": str(argument_graph_path),
            "bridge_plan_json": str(bridge_plan_json),
            "bridges_tex": str(bridges_tex_path),
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "bridge_plan_json": str(bridge_plan_json),
            "bridges_tex": str(bridges_tex_path),
            "section_count": len(section_files),
            "bridge_count": len(bridge_rows),
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)
    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "outputs": manifest["outputs"],
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def export_claim_trace_matrix(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    strictness = compact_whitespace(args.strictness).lower() or "soft"
    overwrite = bool(args.overwrite)
    claims_path = Path(args.claims_jsonl).resolve() if args.claims_jsonl else (paths.root / "references" / "index" / "claims.jsonl")
    paragraph_plans_dir = (
        Path(args.paragraph_plans_dir).resolve()
        if args.paragraph_plans_dir
        else (paths.root / "draft" / "paragraph_plans")
    )
    evidence_packets_dir = (
        Path(args.evidence_packets_dir).resolve()
        if args.evidence_packets_dir
        else (paths.root / "draft" / "evidence_packets")
    )
    bib_path = Path(args.bib_path).resolve() if args.bib_path else (paths.root / "draft" / "latex" / "references.bib")
    output_csv = (
        Path(args.claim_trace_matrix_csv).resolve()
        if args.claim_trace_matrix_csv
        else (paths.root / "draft" / "audit" / "claim_trace_matrix.csv")
    )
    output_json = (
        Path(args.claim_trace_matrix_json).resolve()
        if args.claim_trace_matrix_json
        else (paths.root / "draft" / "audit" / "claim_trace_matrix.json")
    )

    if strictness not in {"soft", "hard"}:
        status = "failed"
        errors.append({"source": "strictness", "message": f"Unsupported strictness: {strictness}"})
    if not claims_path.exists():
        status = "failed"
        errors.append({"source": "claims_jsonl", "message": f"File not found: {claims_path}"})
        claims_rows: list[dict[str, Any]] = []
    else:
        try:
            claims_rows = load_jsonl(claims_path)
        except Exception as exc:
            status = "failed"
            errors.append({"source": "claims_jsonl", "message": str(exc)})
            claims_rows = []
    if not paragraph_plans_dir.exists():
        status = "failed"
        errors.append({"source": "paragraph_plans_dir", "message": f"Directory not found: {paragraph_plans_dir}"})
    if not evidence_packets_dir.exists():
        status = "failed"
        errors.append({"source": "evidence_packets_dir", "message": f"Directory not found: {evidence_packets_dir}"})

    if not bib_path.exists():
        warnings.append({"source": "bib_path", "message": f"Bib file not found: {bib_path}"})
        bib_keys: set[str] = set()
    else:
        bib_keys = set(parse_bib_entry_metadata(bib_path.read_text(encoding="utf-8")).keys())

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "claims_jsonl": str(claims_path),
                "paragraph_plans_dir": str(paragraph_plans_dir),
                "evidence_packets_dir": str(evidence_packets_dir),
                "bib_path": str(bib_path),
                "claim_trace_matrix_csv": str(output_csv),
                "claim_trace_matrix_json": str(output_json),
                "strictness": strictness,
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    plan_links: dict[str, list[dict[str, str]]] = {}
    for plan_file in sorted(paragraph_plans_dir.glob("sec_*.json"), key=section_sort_key):
        try:
            payload = load_json_file(plan_file)
        except Exception as exc:
            warnings.append({"source": "paragraph_plan_json", "message": f"Failed to parse {plan_file}: {exc}"})
            continue
        paragraphs = payload.get("paragraphs", []) if isinstance(payload, dict) else []
        for paragraph in paragraphs:
            if not isinstance(paragraph, dict):
                continue
            paragraph_id = compact_whitespace(paragraph.get("paragraph_id"))
            candidate_ids = paragraph.get("supporting_candidate_ids") if isinstance(paragraph.get("supporting_candidate_ids"), list) else []
            required = paragraph.get("required_evidence_ids") if isinstance(paragraph.get("required_evidence_ids"), list) else []
            for claim_id in required:
                claim_key = compact_whitespace(str(claim_id))
                if not claim_key:
                    continue
                plan_links.setdefault(claim_key, []).append(
                    {
                        "section_stem": plan_file.stem,
                        "paragraph_id": paragraph_id,
                        "candidate_id": compact_whitespace(str(candidate_ids[0])) if candidate_ids else "",
                        "citation_key": "",
                        "source": "paragraph_plan",
                    }
                )

    packet_links: dict[str, list[dict[str, str]]] = {}
    for packet_file in collect_packet_files(evidence_packets_dir):
        try:
            payload = load_json_file(packet_file)
        except Exception as exc:
            warnings.append({"source": "packet_json", "message": f"Failed to parse {packet_file}: {exc}"})
            continue
        if not isinstance(payload, dict):
            continue
        paragraph_id = compact_whitespace(payload.get("paragraph_id")) or packet_file.stem
        section_stem = packet_file.parent.name
        supports = payload.get("supporting_references") if isinstance(payload.get("supporting_references"), list) else []
        for support in supports:
            if not isinstance(support, dict):
                continue
            claim_id = compact_whitespace(support.get("claim_id"))
            if not claim_id:
                claim_id = compact_whitespace((payload.get("claim") or {}).get("core_claim_id"))
            if not claim_id:
                continue
            packet_links.setdefault(claim_id, []).append(
                {
                    "section_stem": section_stem,
                    "paragraph_id": paragraph_id,
                    "candidate_id": compact_whitespace(support.get("candidate_id")),
                    "citation_key": compact_whitespace(support.get("citation_key")),
                    "source": "evidence_packet",
                }
            )

    rows: list[dict[str, str]] = []
    gap_count = 0
    for claim in sorted(claims_rows, key=claim_sort_key):
        claim_id = compact_whitespace(claim.get("claim_id"))
        candidate_id = compact_whitespace(claim.get("candidate_id"))
        claim_text = compact_whitespace(claim.get("claim_text"))
        subquestion_id = compact_whitespace(claim.get("subquestion_id"))
        links = packet_links.get(claim_id, []) or plan_links.get(claim_id, [])
        if not links:
            gap_count += 1
            rows.append(
                {
                    "claim_id": claim_id,
                    "candidate_id": candidate_id,
                    "subquestion_id": subquestion_id,
                    "paragraph_id": "",
                    "section_stem": "",
                    "citation_key": "",
                    "evidence_source": "",
                    "status": "gap",
                    "notes": "No paragraph/evidence packet mapping found.",
                    "claim_text": claim_text,
                }
            )
            continue
        for link in links:
            citation_key = compact_whitespace(link.get("citation_key"))
            row_status = "traced"
            notes = ""
            if not citation_key:
                row_status = "gap"
                notes = "Missing citation_key."
            elif bib_keys and citation_key not in bib_keys:
                row_status = "gap"
                notes = f"citation_key not in bib: {citation_key}"
            if row_status == "gap":
                gap_count += 1
            rows.append(
                {
                    "claim_id": claim_id,
                    "candidate_id": candidate_id or compact_whitespace(link.get("candidate_id")),
                    "subquestion_id": subquestion_id,
                    "paragraph_id": compact_whitespace(link.get("paragraph_id")),
                    "section_stem": compact_whitespace(link.get("section_stem")),
                    "citation_key": citation_key,
                    "evidence_source": compact_whitespace(link.get("source")),
                    "status": row_status,
                    "notes": notes,
                    "claim_text": claim_text,
                }
            )

    if strictness == "hard" and gap_count > 0:
        status = "failed"

    payload = {
        "timestamp": now_iso,
        "strictness": strictness,
        "summary": {
            "claim_count": len(claims_rows),
            "trace_row_count": len(rows),
            "gap_count": gap_count,
        },
        "rows": rows,
        "warnings": warnings,
        "errors": errors,
    }
    columns = [
        "claim_id",
        "candidate_id",
        "subquestion_id",
        "paragraph_id",
        "section_stem",
        "citation_key",
        "evidence_source",
        "status",
        "notes",
        "claim_text",
    ]

    write_results: dict[str, str] = {}
    try:
        if output_csv.exists() and not overwrite:
            write_results[str(output_csv)] = "skipped"
        else:
            write_csv_rows(output_csv, columns, rows)
            write_results[str(output_csv)] = "written"
        if output_json.exists() and not overwrite:
            write_results[str(output_json)] = "skipped"
        else:
            output_json.parent.mkdir(parents=True, exist_ok=True)
            output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            write_results[str(output_json)] = "written"
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "claims_jsonl": str(claims_path),
            "paragraph_plans_dir": str(paragraph_plans_dir),
            "evidence_packets_dir": str(evidence_packets_dir),
            "bib_path": str(bib_path),
            "claim_trace_matrix_csv": str(output_csv),
            "claim_trace_matrix_json": str(output_json),
            "strictness": strictness,
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "claim_trace_matrix_csv": str(output_csv),
            "claim_trace_matrix_json": str(output_json),
            "claim_count": len(claims_rows),
            "trace_row_count": len(rows),
            "gap_count": gap_count,
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)
    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "outputs": manifest["outputs"],
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def ground_figure_table_links(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    strictness = compact_whitespace(args.strictness).lower() or "soft"
    overwrite = bool(args.overwrite)
    latex_sections_dir = (
        Path(args.latex_sections_dir).resolve()
        if args.latex_sections_dir
        else (paths.root / "draft" / "latex" / "sections")
    )
    figures_dir = Path(args.figures_dir).resolve() if args.figures_dir else (paths.root / "figures")
    tables_dir = Path(args.tables_dir).resolve() if args.tables_dir else (paths.root / "tables")
    evidence_packets_dir = (
        Path(args.evidence_packets_dir).resolve()
        if args.evidence_packets_dir
        else (paths.root / "draft" / "evidence_packets")
    )
    output_md = (
        Path(args.figure_table_grounding_md).resolve()
        if args.figure_table_grounding_md
        else (paths.root / "draft" / "reports" / "figure_table_grounding.md")
    )
    output_json = (
        Path(args.figure_table_manifest_json).resolve()
        if args.figure_table_manifest_json
        else (paths.root / "draft" / "reports" / "figure_table_manifest.json")
    )

    if strictness not in {"soft", "hard"}:
        status = "failed"
        errors.append({"source": "strictness", "message": f"Unsupported strictness: {strictness}"})
    section_files = sorted([path for path in latex_sections_dir.glob("sec_*.tex") if path.is_file()], key=section_sort_key)
    if not section_files:
        status = "failed"
        errors.append({"source": "latex_sections_dir", "message": f"No section files found under: {latex_sections_dir}"})
    if not evidence_packets_dir.exists():
        warnings.append({"source": "evidence_packets_dir", "message": f"Directory not found: {evidence_packets_dir}"})

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "latex_sections_dir": str(latex_sections_dir),
                "figures_dir": str(figures_dir),
                "tables_dir": str(tables_dir),
                "evidence_packets_dir": str(evidence_packets_dir),
                "figure_table_grounding_md": str(output_md),
                "figure_table_manifest_json": str(output_json),
                "strictness": strictness,
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    claim_linked_refs: set[str] = set()
    referenced_keys: set[str] = set()
    label_keys: set[str] = set()
    source_note_keys: set[str] = set()
    for section_file in section_files:
        text = section_file.read_text(encoding="utf-8")
        ref_keys = extract_reference_keys(text)
        labels = extract_label_keys(text)
        referenced_keys.update(ref_keys)
        label_keys.update(labels)

        for label in labels:
            pattern = re.compile(rf"(.{{0,160}})\\label\{{{re.escape(label)}\}}", re.IGNORECASE | re.DOTALL)
            match = pattern.search(text)
            snippet = compact_whitespace(match.group(1).lower()) if match else ""
            if "source" in snippet or "adapted" in snippet or "data from" in snippet:
                source_note_keys.add(label)

        for paragraph in collect_paragraph_blocks_with_ids(text):
            paragraph_text = paragraph["text"]
            if "\\textbf{Claim.}" not in paragraph_text:
                continue
            refs = extract_reference_keys(paragraph_text)
            for ref in refs:
                claim_linked_refs.add(ref)

    figure_files = []
    if figures_dir.exists():
        figure_files = sorted([path for path in figures_dir.iterdir() if path.is_file() and path.suffix.lower() in FIGURE_EXTENSIONS])
    table_files = []
    if tables_dir.exists():
        table_files = sorted([path for path in tables_dir.glob("*.csv") if path.is_file()])

    figure_assets_expected = {f"fig:{slugify_text(path.stem, default='fig')}" for path in figure_files}
    table_assets_expected = {f"tab:{slugify_text(path.stem, default='table')}" for path in table_files}

    figure_table_labels = sorted([key for key in label_keys if key.startswith("fig:") or key.startswith("tab:")])
    figure_table_refs = sorted([key for key in referenced_keys if key.startswith("fig:") or key.startswith("tab:")])
    missing_label_refs = sorted([key for key in figure_table_refs if key not in label_keys])
    unreferenced_labels = sorted([key for key in figure_table_labels if key not in referenced_keys])
    unlinked_claim_refs = sorted([key for key in figure_table_refs if key not in claim_linked_refs])
    missing_source_note = sorted([key for key in figure_table_labels if key not in source_note_keys])
    orphan_assets = sorted((figure_assets_expected | table_assets_expected) - set(figure_table_labels))

    issues = {
        "missing_label_refs": missing_label_refs,
        "unreferenced_labels": unreferenced_labels,
        "unlinked_claim_refs": unlinked_claim_refs,
        "missing_source_note": missing_source_note,
        "orphan_assets": orphan_assets,
    }
    issue_count = sum(len(values) for values in issues.values())
    score = max(0, 100 - issue_count * 8)
    if score >= 85:
        risk_level = "low"
    elif score >= 60:
        risk_level = "medium"
    else:
        risk_level = "high"
    if strictness == "hard" and issue_count > 0:
        status = "failed"

    manifest_payload = {
        "timestamp": now_iso,
        "summary": {
            "section_count": len(section_files),
            "figure_asset_count": len(figure_files),
            "table_asset_count": len(table_files),
            "issue_count": issue_count,
            "score": score,
            "risk_level": risk_level,
        },
        "issues": issues,
        "warnings": warnings,
        "errors": errors,
    }

    lines = [
        "# Figure/Table Grounding Report",
        "",
        f"- Score: {score}",
        f"- Risk Level: {risk_level}",
        f"- Issue Count: {issue_count}",
        "",
    ]
    for key, values in issues.items():
        lines.append(f"## {key}")
        if not values:
            lines.append("- None.")
        else:
            for value in values:
                lines.append(f"- {value}")
        lines.append("")

    write_results: dict[str, str] = {}
    try:
        write_results[str(output_md)] = write_text_if_allowed(output_md, "\n".join(lines), overwrite)
        if output_json.exists() and not overwrite:
            write_results[str(output_json)] = "skipped"
        else:
            output_json.parent.mkdir(parents=True, exist_ok=True)
            output_json.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            write_results[str(output_json)] = "written"
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "latex_sections_dir": str(latex_sections_dir),
            "figures_dir": str(figures_dir),
            "tables_dir": str(tables_dir),
            "evidence_packets_dir": str(evidence_packets_dir),
            "figure_table_grounding_md": str(output_md),
            "figure_table_manifest_json": str(output_json),
            "strictness": strictness,
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "figure_table_grounding_md": str(output_md),
            "figure_table_manifest_json": str(output_json),
            "issue_count": issue_count,
            "score": score,
            "risk_level": risk_level,
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)
    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "outputs": manifest["outputs"],
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def latex_build_qa(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    strictness = compact_whitespace(args.strictness).lower() or "soft"
    overwrite = bool(args.overwrite)
    target = compact_whitespace(args.target).lower() or "full"
    main_tex_path = Path(args.main_tex).resolve() if args.main_tex else (paths.root / "draft" / "main.tex")
    full_draft_tex_path = Path(args.full_draft_tex).resolve() if args.full_draft_tex else (paths.root / "draft" / "full_draft_v1.tex")
    bib_path = Path(args.bib_path).resolve() if args.bib_path else (paths.root / "draft" / "latex" / "references.bib")
    report_path = (
        Path(args.latex_build_report_md).resolve()
        if args.latex_build_report_md
        else (paths.root / "draft" / "reports" / "latex_build_report.md")
    )
    log_path = (
        Path(args.latex_build_log_txt).resolve()
        if args.latex_build_log_txt
        else (paths.root / "draft" / "reports" / "latex_build_log.txt")
    )
    run_compiler = bool(args.run_compiler)

    if strictness not in {"soft", "hard"}:
        status = "failed"
        errors.append({"source": "strictness", "message": f"Unsupported strictness: {strictness}"})
    if target not in {"main", "full"}:
        status = "failed"
        errors.append({"source": "target", "message": f"Unsupported target: {target}"})
    target_tex_path = main_tex_path if target == "main" else full_draft_tex_path
    if not target_tex_path.exists():
        status = "failed"
        errors.append({"source": "target_tex", "message": f"File not found: {target_tex_path}"})
        target_text = ""
    else:
        target_text = target_tex_path.read_text(encoding="utf-8")
    if not bib_path.exists():
        warnings.append({"source": "bib_path", "message": f"Bib file not found: {bib_path}"})
        bib_keys: set[str] = set()
    else:
        bib_keys = set(parse_bib_entry_metadata(bib_path.read_text(encoding="utf-8")).keys())

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "target": target,
                "main_tex": str(main_tex_path),
                "full_draft_tex": str(full_draft_tex_path),
                "bib_path": str(bib_path),
                "latex_build_report_md": str(report_path),
                "latex_build_log_txt": str(log_path),
                "run_compiler": run_compiler,
                "strictness": strictness,
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    label_keys = set(extract_label_keys(target_text))
    ref_keys = set(extract_reference_keys(target_text))
    cite_keys = set(extract_citation_keys(target_text))
    missing_ref_keys = sorted([key for key in ref_keys if key not in label_keys])
    missing_cite_keys = sorted([key for key in cite_keys if bib_keys and key not in bib_keys])

    compiler_status = "skipped"
    compiler_return_code = 0
    compiler_log = "Compiler not run."
    if run_compiler:
        pdflatex = shutil.which("pdflatex")
        if not pdflatex:
            compiler_status = "tool_missing"
            warnings.append({"source": "compiler", "message": "pdflatex not found; skipped compile."})
            compiler_log = "pdflatex not found."
        else:
            try:
                result = subprocess.run(
                    [pdflatex, "-interaction=nonstopmode", "-halt-on-error", target_tex_path.name],
                    cwd=str(target_tex_path.parent),
                    text=True,
                    capture_output=True,
                    timeout=120,
                )
                compiler_return_code = int(result.returncode)
                compiler_log = (result.stdout or "") + "\n" + (result.stderr or "")
                compiler_status = "success" if result.returncode == 0 else "failed"
            except Exception as exc:
                compiler_status = "failed"
                compiler_return_code = 1
                compiler_log = str(exc)
                warnings.append({"source": "compiler", "message": f"compile exception: {exc}"})

    issue_count = len(missing_ref_keys) + len(missing_cite_keys) + (1 if compiler_status == "failed" else 0)
    score = max(0, 100 - len(missing_ref_keys) * 12 - len(missing_cite_keys) * 12 - (20 if compiler_status == "failed" else 0))
    if score >= 85:
        risk_level = "low"
    elif score >= 60:
        risk_level = "medium"
    else:
        risk_level = "high"
    if strictness == "hard" and issue_count > 0:
        status = "failed"

    report_lines = [
        "# LaTeX Build QA Report",
        "",
        f"- Target: {target}",
        f"- Score: {score}",
        f"- Risk Level: {risk_level}",
        f"- Missing Ref Keys: {len(missing_ref_keys)}",
        f"- Missing Cite Keys: {len(missing_cite_keys)}",
        f"- Compiler Status: {compiler_status}",
        "",
        "## Missing Ref Keys",
    ]
    if missing_ref_keys:
        report_lines.extend([f"- {key}" for key in missing_ref_keys])
    else:
        report_lines.append("- None.")
    report_lines.extend(["", "## Missing Cite Keys"])
    if missing_cite_keys:
        report_lines.extend([f"- {key}" for key in missing_cite_keys])
    else:
        report_lines.append("- None.")
    report_lines.append("")

    write_results: dict[str, str] = {}
    try:
        write_results[str(report_path)] = write_text_if_allowed(report_path, "\n".join(report_lines), overwrite)
        write_results[str(log_path)] = write_text_if_allowed(log_path, compiler_log, overwrite)
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "target": target,
            "main_tex": str(main_tex_path),
            "full_draft_tex": str(full_draft_tex_path),
            "bib_path": str(bib_path),
            "latex_build_report_md": str(report_path),
            "latex_build_log_txt": str(log_path),
            "run_compiler": run_compiler,
            "strictness": strictness,
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": {
            "latex_build_report_md": str(report_path),
            "latex_build_log_txt": str(log_path),
            "missing_ref_count": len(missing_ref_keys),
            "missing_cite_count": len(missing_cite_keys),
            "compiler_status": compiler_status,
            "compiler_return_code": compiler_return_code,
            "score": score,
            "risk_level": risk_level,
            "write_results": write_results,
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)
    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "outputs": manifest["outputs"],
                "warnings": warnings,
                "errors": errors,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def assemble_full_draft(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    strictness = compact_whitespace(args.strictness).lower() or "soft"
    overwrite = bool(args.overwrite)
    latex_sections_dir = (
        Path(args.latex_sections_dir).resolve()
        if args.latex_sections_dir
        else (paths.root / "draft" / "latex" / "sections")
    )
    section_audit_dir = (
        Path(args.section_audit_dir).resolve()
        if args.section_audit_dir
        else (paths.root / "draft" / "latex" / "audit")
    )
    bib_path = Path(args.bib_path).resolve() if args.bib_path else (paths.root / "draft" / "latex" / "references.bib")
    abstract_template_path = (
        Path(args.abstract_template).resolve()
        if args.abstract_template
        else (paths.root / "draft" / "templates" / "abstract.tex")
    )
    conclusion_template_path = (
        Path(args.conclusion_template).resolve()
        if args.conclusion_template
        else (paths.root / "draft" / "templates" / "conclusion.tex")
    )
    output_main_tex = Path(args.output_main_tex).resolve() if args.output_main_tex else (paths.root / "draft" / "main.tex")
    output_full_draft_tex = (
        Path(args.output_full_draft_tex).resolve()
        if args.output_full_draft_tex
        else (paths.root / "draft" / "full_draft_v1.tex")
    )
    full_draft_review_md = (
        Path(args.full_draft_review_md).resolve()
        if args.full_draft_review_md
        else (paths.root / "draft" / "reports" / "full_draft_review.md")
    )

    if strictness not in {"soft", "hard"}:
        status = "failed"
        errors.append({"source": "strictness", "message": f"Unsupported strictness: {strictness}"})

    section_files = sorted([path for path in latex_sections_dir.glob("sec_*.tex") if path.is_file()], key=section_sort_key)
    if not section_files:
        status = "failed"
        errors.append({"source": "latex_sections_dir", "message": f"No section files found under: {latex_sections_dir}"})

    if not bib_path.exists():
        status = "failed"
        errors.append({"source": "bib_path", "message": f"Bib file not found: {bib_path}"})
        bib_metadata: dict[str, dict[str, Any]] = {}
    else:
        bib_metadata = parse_bib_entry_metadata(bib_path.read_text(encoding="utf-8"))

    if status != "ok":
        manifest = {
            "run_id": run_id,
            "timestamp": now_iso,
            "inputs": {
                "latex_sections_dir": str(latex_sections_dir),
                "section_audit_dir": str(section_audit_dir),
                "bib_path": str(bib_path),
                "abstract_template": str(abstract_template_path),
                "conclusion_template": str(conclusion_template_path),
                "output_main_tex": str(output_main_tex),
                "output_full_draft_tex": str(output_full_draft_tex),
                "full_draft_review_md": str(full_draft_review_md),
                "strictness": strictness,
                "overwrite": overwrite,
            },
            "status": status,
            "outputs": {},
            "warnings": warnings,
            "errors": errors,
        }
        manifest_path = write_manifest(run_dir, manifest)
        print(
            json.dumps(
                {
                    "status": status,
                    "run_id": run_id,
                    "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                    "errors": errors,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    sections: list[dict[str, Any]] = []
    for section_file in section_files:
        text = section_file.read_text(encoding="utf-8")
        title = extract_section_title_from_text(text, fallback=section_file.stem)
        sections.append(
            {
                "path": section_file,
                "stem": section_file.stem,
                "name": section_file.name,
                "title": title,
                "text": text,
            }
        )

    section_audit_rows: list[dict[str, Any]] = []
    unresolved_high_risk_count = 0
    for section in sections:
        stem = compact_whitespace(section.get("stem"))
        audit_path = section_audit_dir / f"section_{stem}_audit.json"
        if not audit_path.exists():
            warnings.append({"source": "section_audit_dir", "message": f"Section audit not found: {audit_path}"})
            continue
        try:
            payload = json.loads(audit_path.read_text(encoding="utf-8"))
        except Exception as exc:
            warnings.append({"source": "section_audit_json", "message": f"Failed to parse {audit_path}: {exc}"})
            continue
        summary = payload.get("summary") if isinstance(payload, dict) else {}
        if not isinstance(summary, dict):
            summary = {}
        findings = payload.get("findings") if isinstance(payload, dict) else []
        if not isinstance(findings, list):
            findings = []
        high_findings = len(
            [
                row
                for row in findings
                if isinstance(row, dict) and compact_whitespace(row.get("severity")).lower() == "high"
            ]
        )
        if high_findings > 0:
            unresolved_high_risk_count += high_findings
        elif compact_whitespace(summary.get("risk_level")).lower() == "high":
            unresolved_high_risk_count += 1
        section_audit_rows.append(
            {
                "section_stem": stem,
                "section_title": compact_whitespace(summary.get("section_title")) or compact_whitespace(section.get("title")),
                "score": int(summary.get("score", 0) or 0),
                "risk_level": compact_whitespace(summary.get("risk_level")) or "unknown",
                "key_claim_count": int(summary.get("key_claim_count", 0) or 0),
                "high_severity_findings": high_findings,
                "audit_path": str(audit_path),
            }
        )

    default_abstract = (
        "\\begin{abstract}\n"
        "TODO: Summarize research objective, evidence-backed findings, and scope boundaries.\n"
        f"{CONSISTENCY_BOUNDARY_SENTENCE}\n"
        "\\end{abstract}\n"
    )
    if abstract_template_path.exists():
        raw_abstract = abstract_template_path.read_text(encoding="utf-8")
    else:
        raw_abstract = ""
        warnings.append({"source": "abstract_template", "message": f"Template not found; fallback placeholder used: {abstract_template_path}"})
    raw_abstract = raw_abstract.strip()
    if not raw_abstract:
        abstract_block = default_abstract
    elif "\\begin{abstract}" in raw_abstract:
        abstract_block = raw_abstract.rstrip() + "\n"
    else:
        abstract_block = "\\begin{abstract}\n" + raw_abstract.rstrip() + "\n\\end{abstract}\n"

    default_conclusion_body = (
        "TODO: Synthesize chapter-level evidence into bounded final claims.\n"
        f"{CONSISTENCY_BOUNDARY_SENTENCE}\n"
    )
    if conclusion_template_path.exists():
        raw_conclusion = conclusion_template_path.read_text(encoding="utf-8").strip()
    else:
        raw_conclusion = ""
        warnings.append({"source": "conclusion_template", "message": f"Template not found; fallback placeholder used: {conclusion_template_path}"})
    if not raw_conclusion:
        conclusion_block = "\\section{Conclusion}\n" + default_conclusion_body
    elif "\\section{" in raw_conclusion:
        conclusion_block = raw_conclusion.rstrip() + "\n"
    else:
        conclusion_block = "\\section{Conclusion}\n" + raw_conclusion.rstrip() + "\n"
    conclusion_block += (
        "Taken together, the chapter-level evidence indicates a directionally consistent yet bounded interpretation.\n"
        "Remaining high-risk points are tracked in the review report for targeted follow-up.\n"
    )

    section_titles = [compact_whitespace(str(section.get("title"))) for section in sections if compact_whitespace(str(section.get("title")))]
    scope_titles = ", ".join(section_titles[:6]) if section_titles else "available sections"
    intro_block = (
        "\\section{Introduction}\n"
        "This full draft assembles chapter drafts into one evidence-bounded manuscript.\n"
        f"Current chapter scope covers: {latex_escape(scope_titles)}.\n"
        "The narrative keeps claims traceable to explicit citations and keeps uncertainty visible.\n"
        f"To avoid overclaiming, interpretations remain constrained to reported conditions; unresolved high-risk findings: {unresolved_high_risk_count}.\n"
    )

    optional_section_files: list[Path] = []
    for name in ("figures.tex", "tables.tex"):
        candidate = latex_sections_dir / name
        if candidate.exists():
            optional_section_files.append(candidate)

    main_section_inputs = [f"\\input{{latex/sections/{section['name']}}}" for section in sections]
    main_section_inputs.extend([f"\\input{{latex/sections/{path.name}}}" for path in optional_section_files])
    main_section_inputs_text = "\n".join(main_section_inputs)

    inline_section_blocks = [section["text"].rstrip() for section in sections]
    for path in optional_section_files:
        inline_section_blocks.append(path.read_text(encoding="utf-8").rstrip())
    inline_sections_text = "\n\n".join([block for block in inline_section_blocks if compact_whitespace(block)])

    assembled_body = "\n\n".join(
        [
            intro_block.rstrip(),
            inline_sections_text.rstrip(),
            conclusion_block.rstrip(),
        ]
    ).rstrip() + "\n"
    assembled_body, terminology_events = apply_term_consistency_rewrites(assembled_body)

    defined_labels = set(extract_label_keys(assembled_body))
    used_refs = set(extract_reference_keys(assembled_body))
    crossref_missing_keys = sorted([key for key in used_refs if key not in defined_labels])

    figure_table_refs = sorted([key for key in used_refs if key.startswith("fig:") or key.startswith("tab:")])
    figure_table_labels = sorted([key for key in defined_labels if key.startswith("fig:") or key.startswith("tab:")])
    missing_figure_table_refs = sorted([key for key in figure_table_refs if key not in defined_labels])
    unused_figure_table_labels = sorted([key for key in figure_table_labels if key not in used_refs])

    cited_keys = set(extract_citation_keys(assembled_body))
    citation_key_missing_keys = sorted([key for key in cited_keys if key not in bib_metadata])

    crossref_issue_count = len(crossref_missing_keys)
    figure_table_ref_issue_count = len(missing_figure_table_refs) + len(unused_figure_table_labels)
    score, risk_level = score_full_draft_review(
        unresolved_high_risk_count=unresolved_high_risk_count,
        crossref_issue_count=crossref_issue_count,
        figure_table_ref_issue_count=figure_table_ref_issue_count,
        citation_key_missing_count=len(citation_key_missing_keys),
    )

    if strictness == "hard" and (
        unresolved_high_risk_count > 0
        or len(citation_key_missing_keys) > 0
        or risk_level == "high"
    ):
        status = "failed"

    full_draft_text = "\n".join(
        [
            "\\documentclass[11pt]{article}",
            "",
            "\\usepackage[margin=1in]{geometry}",
            "\\usepackage{graphicx}",
            "\\usepackage{booktabs}",
            "\\usepackage[hidelinks]{hyperref}",
            "\\usepackage[backend=biber,style=numeric]{biblatex}",
            "\\addbibresource{latex/references.bib}",
            "\\graphicspath{{figures/}{latex/figures/}{../figures/}}",
            "",
            "\\title{Full Draft v1}",
            "\\author{TODO}",
            "\\date{\\today}",
            "",
            "\\begin{document}",
            "\\maketitle",
            abstract_block.rstrip(),
            "",
            assembled_body.rstrip(),
            "",
            "\\printbibliography",
            "\\end{document}",
            "",
        ]
    )

    main_text = "\n".join(
        [
            "\\documentclass[11pt]{article}",
            "",
            "\\usepackage[margin=1in]{geometry}",
            "\\usepackage{graphicx}",
            "\\usepackage{booktabs}",
            "\\usepackage[hidelinks]{hyperref}",
            "\\usepackage[backend=biber,style=numeric]{biblatex}",
            "\\addbibresource{latex/references.bib}",
            "\\graphicspath{{figures/}{latex/figures/}{../figures/}}",
            "",
            "\\title{Main Draft Entry}",
            "\\author{TODO}",
            "\\date{\\today}",
            "",
            "\\begin{document}",
            "\\maketitle",
            abstract_block.rstrip(),
            "",
            intro_block.rstrip(),
            "",
            main_section_inputs_text,
            "",
            conclusion_block.rstrip(),
            "",
            "\\printbibliography",
            "\\end{document}",
            "",
        ]
    )

    review_text = render_full_draft_review_report(
        score=score,
        risk_level=risk_level,
        section_audit_rows=section_audit_rows,
        unresolved_high_risk_count=unresolved_high_risk_count,
        terminology_events=terminology_events,
        crossref_missing_keys=crossref_missing_keys,
        missing_figure_table_refs=missing_figure_table_refs,
        unused_figure_table_labels=unused_figure_table_labels,
        citation_key_missing_keys=citation_key_missing_keys,
        warnings=warnings,
    )

    write_results: dict[str, str] = {}
    try:
        write_results[str(output_main_tex)] = write_text_if_allowed(output_main_tex, main_text, overwrite)
        write_results[str(output_full_draft_tex)] = write_text_if_allowed(output_full_draft_tex, full_draft_text, overwrite)
        write_results[str(full_draft_review_md)] = write_text_if_allowed(full_draft_review_md, review_text, overwrite)
    except Exception as exc:
        status = "failed"
        errors.append({"source": "write_outputs", "message": str(exc)})

    outputs = {
        "output_main_tex": str(output_main_tex),
        "output_full_draft_tex": str(output_full_draft_tex),
        "full_draft_review_md": str(full_draft_review_md),
        "section_count": len(sections),
        "section_audit_count": len(section_audit_rows),
        "unresolved_high_risk_count": unresolved_high_risk_count,
        "terminology_fix_count": len(terminology_events),
        "figure_table_ref_issue_count": figure_table_ref_issue_count,
        "crossref_issue_count": crossref_issue_count,
        "citation_key_missing_count": len(citation_key_missing_keys),
        "score": score,
        "risk_level": risk_level,
        "write_results": write_results,
    }
    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "latex_sections_dir": str(latex_sections_dir),
            "section_audit_dir": str(section_audit_dir),
            "bib_path": str(bib_path),
            "abstract_template": str(abstract_template_path),
            "conclusion_template": str(conclusion_template_path),
            "output_main_tex": str(output_main_tex),
            "output_full_draft_tex": str(output_full_draft_tex),
            "full_draft_review_md": str(full_draft_review_md),
            "strictness": strictness,
            "overwrite": overwrite,
        },
        "status": status,
        "outputs": outputs,
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "assemble_full_draft_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "assemble_full_draft_completed",
        {
            "run_id": run_id,
            "status": status,
            "section_count": len(sections),
            "score": score,
            "risk_level": risk_level,
        },
    )

    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "outputs": outputs,
                "warnings": warnings,
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


def run_pipeline(args: argparse.Namespace) -> int:
    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    paths = resolve_paths(base_dir=base_dir)
    ensure_workspace(paths)

    run_id, run_dir = create_run_dir(paths.runs_dir)
    now_iso = local_now().isoformat(timespec="seconds")
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    status = "ok"

    from_stage = normalize_pipeline_stage_name(args.from_stage)
    to_stage = normalize_pipeline_stage_name(args.to_stage)
    with_fulltext = bool(args.with_fulltext)
    continue_on_error = bool(args.continue_on_error)
    strictness = compact_whitespace(args.strictness).lower() or "soft"
    overwrite = bool(args.overwrite)
    run_compiler = bool(args.run_compiler)

    stage_chain = list_pipeline_stages(with_fulltext=with_fulltext)
    if from_stage not in stage_chain:
        status = "failed"
        errors.append({"source": "from_stage", "message": f"Unsupported from-stage in current mode: {from_stage}"})
    if to_stage not in stage_chain:
        status = "failed"
        errors.append({"source": "to_stage", "message": f"Unsupported to-stage in current mode: {to_stage}"})

    if status == "ok":
        from_index = stage_chain.index(from_stage)
        to_index = stage_chain.index(to_stage)
        if from_index > to_index:
            status = "failed"
            errors.append({"source": "stage_range", "message": f"from-stage {from_stage} is after to-stage {to_stage}"})
            selected_stages: list[str] = []
        else:
            selected_stages = stage_chain[from_index : to_index + 1]
    else:
        selected_stages = []

    requires_topic_frame = any(stage in {"search-candidates", "outline-from-evidence"} for stage in selected_stages)
    topic_frame_json = compact_whitespace(args.topic_frame_json)
    if requires_topic_frame and not topic_frame_json:
        status = "failed"
        errors.append({"source": "topic_frame_json", "message": "topic_frame_json is required for selected stage range."})
    if requires_topic_frame and topic_frame_json:
        topic_frame_path = Path(topic_frame_json).resolve()
        if not topic_frame_path.exists():
            status = "failed"
            errors.append({"source": "topic_frame_json", "message": f"File not found: {topic_frame_path}"})

    stage_results: list[dict[str, Any]] = []
    failed_stage: dict[str, Any] | None = None
    section_consistency_report_json = ""
    section_audit_json_by_stem: dict[str, str] = {}

    if status == "ok":
        should_stop = False
        for stage_name in selected_stages:
            if should_stop:
                break

            if stage_name in {"section-citation-audit", "section-release-gate"}:
                section_files = sorted(
                    [path for path in (paths.root / "draft" / "latex" / "sections").glob("sec_*.tex") if path.is_file()],
                    key=section_sort_key,
                )
                if not section_files:
                    result = {
                        "stage": stage_name,
                        "target": "",
                        "status": "failed",
                        "exit_code": 1,
                        "manifest": "",
                        "manifest_status": "",
                        "duration_seconds": 0,
                        "error": f"No section tex files found under {(paths.root / 'draft' / 'latex' / 'sections')}",
                        "outputs": {},
                    }
                    stage_results.append(result)
                    if failed_stage is None:
                        failed_stage = result
                    if not continue_on_error:
                        should_stop = True
                    continue

                for section_file in section_files:
                    section_stem = section_file.stem
                    stage_args = build_pipeline_stage_args(
                        stage_name,
                        args,
                        paths,
                        section_tex=str(section_file),
                        section_stem=section_stem,
                        section_consistency_report_json=section_consistency_report_json,
                        section_audit_json=section_audit_json_by_stem.get(section_stem, ""),
                    )
                    result = run_pipeline_stage(stage_name, stage_args, paths)
                    result["target"] = section_stem
                    stage_results.append(result)

                    if stage_name == "section-citation-audit":
                        outputs = result.get("outputs", {}) if isinstance(result.get("outputs"), dict) else {}
                        section_audit_json = compact_whitespace(outputs.get("section_audit_json"))
                        if section_audit_json:
                            section_audit_json_by_stem[section_stem] = section_audit_json

                    if result.get("status") == "failed":
                        if failed_stage is None:
                            failed_stage = result
                        if not continue_on_error:
                            should_stop = True
                            break
                continue

            stage_args = build_pipeline_stage_args(stage_name, args, paths)
            result = run_pipeline_stage(stage_name, stage_args, paths)
            result["target"] = ""
            stage_results.append(result)

            if stage_name == "revise-section-consistency":
                outputs = result.get("outputs", {}) if isinstance(result.get("outputs"), dict) else {}
                section_consistency_report_json = compact_whitespace(outputs.get("section_consistency_report_json"))

            if result.get("status") == "failed":
                if failed_stage is None:
                    failed_stage = result
                if not continue_on_error:
                    should_stop = True

    if any(row.get("status") == "failed" for row in stage_results):
        status = "failed"

    summary_payload = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": {
            "from_stage": from_stage,
            "to_stage": to_stage,
            "with_fulltext": with_fulltext,
            "strictness": strictness,
            "overwrite": overwrite,
            "run_compiler": run_compiler,
            "continue_on_error": continue_on_error,
            "topic_frame_json": topic_frame_json,
        },
        "status": status,
        "selected_stages": selected_stages,
        "executed_stage_count": len(stage_results),
        "stage_results": stage_results,
        "failure_point": {
            "stage": failed_stage.get("stage"),
            "target": failed_stage.get("target"),
            "error": failed_stage.get("error"),
        }
        if failed_stage
        else None,
        "warnings": warnings,
        "errors": errors,
    }

    summary_json_path = run_dir / "pipeline_run_summary.json"
    summary_md_path = run_dir / "pipeline_run_summary.md"
    try:
        summary_json_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        summary_md_path.write_text(
            render_pipeline_run_summary_markdown(
                selected_stages=selected_stages,
                stage_results=stage_results,
                overall_status=status,
                failed_stage=failed_stage,
            ),
            encoding="utf-8",
        )
    except Exception as exc:
        status = "failed"
        errors.append({"source": "pipeline_run_summary", "message": str(exc)})

    manifest = {
        "run_id": run_id,
        "timestamp": now_iso,
        "inputs": summary_payload["inputs"],
        "status": status,
        "outputs": {
            "pipeline_run_summary_json": str(summary_json_path),
            "pipeline_run_summary_md": str(summary_md_path),
            "selected_stage_count": len(selected_stages),
            "executed_stage_count": len(stage_results),
            "failed_stage_count": len([row for row in stage_results if row.get("status") == "failed"]),
        },
        "warnings": warnings,
        "errors": errors,
    }
    manifest_path = write_manifest(run_dir, manifest)

    if errors:
        append_log(paths, "WARN", "run_pipeline_with_errors", {"run_id": run_id, "errors": errors})
    append_log(
        paths,
        "INFO",
        "run_pipeline_completed",
        {
            "run_id": run_id,
            "status": status,
            "selected_stage_count": len(selected_stages),
            "executed_stage_count": len(stage_results),
            "failed_stage_count": len([row for row in stage_results if row.get("status") == "failed"]),
        },
    )
    print(
        json.dumps(
            {
                "status": status,
                "run_id": run_id,
                "manifest": str(manifest_path.relative_to(paths.root)) if paths.root in manifest_path.parents else str(manifest_path),
                "outputs": manifest["outputs"],
                "warnings": warnings,
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

    paragraph_plans_parser = subparsers.add_parser(
        "generate-paragraph-plans",
        help="Decompose section plan into paragraph-level writing units.",
    )
    paragraph_plans_parser.add_argument(
        "--outline-markdown",
        default="",
        help="Path to outline Markdown. Defaults to outline/generated_outline.md.",
    )
    paragraph_plans_parser.add_argument(
        "--argument-graph-json",
        default="",
        help="Path to argument graph JSON. Defaults to references/index/argument_graph.json.",
    )
    paragraph_plans_parser.add_argument(
        "--claims-jsonl",
        default="",
        help="Path to claims JSONL. Defaults to references/index/claims.jsonl.",
    )
    paragraph_plans_parser.add_argument(
        "--paragraph-plans-dir",
        default="",
        help="Output directory for paragraph plans. Defaults to draft/paragraph_plans.",
    )
    paragraph_plans_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing paragraph plan files. Default false to preserve manual edits.",
    )
    paragraph_plans_parser.set_defaults(func=generate_paragraph_plans)

    evidence_packets_parser = subparsers.add_parser(
        "assemble-evidence-packets",
        help="Assemble paragraph-level evidence packets before prose drafting.",
    )
    evidence_packets_parser.add_argument(
        "--paragraph-plans-dir",
        default="",
        help="Directory of section paragraph plans. Defaults to draft/paragraph_plans.",
    )
    evidence_packets_parser.add_argument(
        "--claims-jsonl",
        default="",
        help="Path to claims JSONL. Defaults to references/index/claims.jsonl.",
    )
    evidence_packets_parser.add_argument(
        "--cards-jsonl",
        default="",
        help="Path to cards JSONL. Defaults to references/index/cards.jsonl.",
    )
    evidence_packets_parser.add_argument(
        "--included-candidates-csv",
        default="",
        help="Path to included candidates CSV. Defaults to references/index/included_candidates.csv.",
    )
    evidence_packets_parser.add_argument(
        "--bib-path",
        default="",
        help="Path to bibliography file. Defaults to draft/latex/references.bib.",
    )
    evidence_packets_parser.add_argument(
        "--packet-overrides-json",
        default="",
        help="Optional packet overrides JSON. Defaults to draft/evidence_packets/packet_overrides.json.",
    )
    evidence_packets_parser.add_argument(
        "--evidence-packets-dir",
        default="",
        help="Output directory for evidence packets. Defaults to draft/evidence_packets.",
    )
    evidence_packets_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing packet files. Default false to preserve manual edits.",
    )
    evidence_packets_parser.set_defaults(func=assemble_evidence_packets)

    section_drafts_parser = subparsers.add_parser(
        "generate-section-drafts",
        help="Generate paragraph-level section drafts using paragraph plans and evidence packets.",
    )
    section_drafts_parser.add_argument(
        "--paragraph-plans-dir",
        default="",
        help="Directory of section paragraph plans. Defaults to draft/paragraph_plans.",
    )
    section_drafts_parser.add_argument(
        "--evidence-packets-dir",
        default="",
        help="Directory of assembled evidence packets. Defaults to draft/evidence_packets.",
    )
    section_drafts_parser.add_argument(
        "--section-roles-json",
        default="",
        help="Optional section roles JSON. Defaults to draft/section_roles.json.",
    )
    section_drafts_parser.add_argument(
        "--latex-sections-dir",
        default="",
        help="Output directory for section tex files. Defaults to draft/latex/sections.",
    )
    section_drafts_parser.add_argument(
        "--latex-template-path",
        default="",
        help="Optional section template file using placeholders {{SECTION_TITLE}}, {{SECTION_LABEL}}, {{PARAGRAPH_BLOCKS}}.",
    )
    section_drafts_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing section draft files. Default false to preserve manual edits.",
    )
    section_drafts_parser.set_defaults(func=generate_section_drafts)

    section_consistency_parser = subparsers.add_parser(
        "revise-section-consistency",
        help="Revise section-level consistency using section drafts and argument graph context.",
    )
    section_consistency_parser.add_argument(
        "--latex-sections-dir",
        default="",
        help="Directory containing section tex files. Defaults to draft/latex/sections.",
    )
    section_consistency_parser.add_argument(
        "--argument-graph-json",
        default="",
        help="Path to argument graph JSON. Defaults to references/index/argument_graph.json.",
    )
    section_consistency_parser.add_argument(
        "--section-drafts-dir",
        default="",
        help="Optional section_drafts directory. Defaults to latest draft/runs/run_*/section_drafts.",
    )
    section_consistency_parser.add_argument(
        "--consistency-report-json",
        default="",
        help="Path to consistency report JSON. Defaults to draft/runs/run_*/section_consistency_report.json.",
    )
    section_consistency_parser.add_argument(
        "--strictness",
        default="soft",
        help="Consistency strictness policy: soft or hard (default: soft).",
    )
    section_consistency_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite section tex files in place. Default false to preserve manual edits.",
    )
    section_consistency_parser.set_defaults(func=revise_section_consistency)

    section_citation_audit_parser = subparsers.add_parser(
        "section-citation-audit",
        help="Audit citation-evidence consistency for a single section tex before next chapter drafting.",
    )
    section_citation_audit_parser.add_argument(
        "--section-tex",
        required=True,
        help="Path to target section tex file (required).",
    )
    section_citation_audit_parser.add_argument(
        "--bib-path",
        default="",
        help="Path to bibliography file. Defaults to draft/latex/references.bib.",
    )
    section_citation_audit_parser.add_argument(
        "--evidence-packets-dir",
        default="",
        help="Directory containing evidence packets. Defaults to draft/evidence_packets.",
    )
    section_citation_audit_parser.add_argument(
        "--claims-jsonl",
        default="",
        help="Path to claims JSONL. Defaults to references/index/claims.jsonl.",
    )
    section_citation_audit_parser.add_argument(
        "--records-jsonl",
        default="",
        help="Path to records JSONL. Defaults to references/index/records.jsonl.",
    )
    section_citation_audit_parser.add_argument(
        "--section-drafts-dir",
        default="",
        help="Optional section_drafts directory. Defaults to latest draft/runs/run_*/section_drafts.",
    )
    section_citation_audit_parser.add_argument(
        "--audit-output-dir",
        default="",
        help="Output directory for section audit artifacts. Defaults to draft/latex/audit.",
    )
    section_citation_audit_parser.add_argument(
        "--strictness",
        default="soft",
        help="Audit strictness policy: soft or hard (default: soft).",
    )
    section_citation_audit_parser.set_defaults(func=section_citation_audit)

    fetch_fulltext_parser = subparsers.add_parser(
        "fetch-fulltext",
        help="Download fulltext PDFs for included candidates and write audit logs.",
    )
    fetch_fulltext_parser.add_argument(
        "--included-candidates-csv",
        default="",
        help="Path to included candidates CSV. Defaults to references/index/included_candidates.csv.",
    )
    fetch_fulltext_parser.add_argument(
        "--cards-jsonl",
        default="",
        help="Path to cards JSONL. Defaults to references/index/cards.jsonl.",
    )
    fetch_fulltext_parser.add_argument(
        "--records-jsonl",
        default="",
        help="Path to records JSONL. Defaults to references/index/records.jsonl.",
    )
    fetch_fulltext_parser.add_argument(
        "--download-log-csv",
        default="",
        help="Path to fulltext fetch log CSV. Defaults to references/index/fulltext_fetch_log.csv.",
    )
    fetch_fulltext_parser.add_argument(
        "--downloaded-index-csv",
        default="",
        help="Path to downloaded fulltexts index CSV. Defaults to references/index/downloaded_fulltexts.csv.",
    )
    fetch_fulltext_parser.add_argument(
        "--library-dir",
        default="",
        help="Directory for downloaded PDFs. Defaults to references/library/.",
    )
    fetch_fulltext_parser.add_argument(
        "--max-retries",
        type=int,
        default=1,
        help="Retry count per candidate when download fails (default: 1).",
    )
    fetch_fulltext_parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=REQUEST_TIMEOUT_SECONDS,
        help=f"HTTP timeout in seconds (default: {REQUEST_TIMEOUT_SECONDS}).",
    )
    fetch_fulltext_parser.set_defaults(func=fetch_fulltext)

    latex_draft_parser = subparsers.add_parser(
        "generate-latex-draft",
        help="Generate modular LaTeX draft project from argument graph and evidence outputs.",
    )
    latex_draft_parser.add_argument(
        "--argument-graph-json",
        default="",
        help="Path to argument graph JSON. Defaults to references/index/argument_graph.json.",
    )
    latex_draft_parser.add_argument(
        "--claims-jsonl",
        default="",
        help="Path to claims JSONL. Defaults to references/index/claims.jsonl.",
    )
    latex_draft_parser.add_argument(
        "--included-candidates-csv",
        default="",
        help="Path to included candidates CSV. Defaults to references/index/included_candidates.csv.",
    )
    latex_draft_parser.add_argument(
        "--records-jsonl",
        default="",
        help="Path to records JSONL. Defaults to references/index/records.jsonl.",
    )
    latex_draft_parser.add_argument(
        "--refs-bib",
        default="",
        help="Path to supplemental refs bib. Defaults to draft/latex/refs.bib.",
    )
    latex_draft_parser.add_argument(
        "--latex-dir",
        default="",
        help="Output LaTeX project directory. Defaults to draft/latex.",
    )
    latex_draft_parser.add_argument(
        "--figures-dir",
        default="",
        help="Figures source directory. Defaults to figures/.",
    )
    latex_draft_parser.add_argument(
        "--tables-dir",
        default="",
        help="Tables CSV source directory. Defaults to tables/.",
    )
    latex_draft_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing generated files. Default false to preserve manual edits.",
    )
    latex_draft_parser.set_defaults(func=generate_latex_draft)

    citation_audit_parser = subparsers.add_parser(
        "citation-audit",
        help="Audit conclusion-citation-evidence chain consistency for LaTeX draft outputs.",
    )
    citation_audit_parser.add_argument(
        "--latex-dir",
        default="",
        help="LaTeX project directory. Defaults to draft/latex.",
    )
    citation_audit_parser.add_argument(
        "--main-tex",
        default="",
        help="Path to main tex file. Defaults to draft/latex/main.tex.",
    )
    citation_audit_parser.add_argument(
        "--outline-tex",
        default="",
        help="Path to outline tex file. Defaults to draft/latex/outline.tex.",
    )
    citation_audit_parser.add_argument(
        "--bib-path",
        default="",
        help="Path to bibliography file. Defaults to draft/latex/references.bib.",
    )
    citation_audit_parser.add_argument(
        "--argument-graph-json",
        default="",
        help="Path to argument graph JSON. Defaults to references/index/argument_graph.json.",
    )
    citation_audit_parser.add_argument(
        "--claims-jsonl",
        default="",
        help="Path to claims JSONL. Defaults to references/index/claims.jsonl.",
    )
    citation_audit_parser.add_argument(
        "--records-jsonl",
        default="",
        help="Path to records JSONL. Defaults to references/index/records.jsonl.",
    )
    citation_audit_parser.add_argument(
        "--included-candidates-csv",
        default="",
        help="Path to included candidates CSV. Defaults to references/index/included_candidates.csv.",
    )
    citation_audit_parser.add_argument(
        "--audit-overrides-json",
        default="",
        help="Optional audit override JSON. Defaults to draft/latex/audit_overrides.json.",
    )
    citation_audit_parser.add_argument(
        "--audit-output-dir",
        default="",
        help="Audit output directory. Defaults to draft/latex/audit.",
    )
    citation_audit_parser.add_argument(
        "--strictness",
        default="soft",
        choices=["soft", "hard"],
        help="Audit strictness policy: soft or hard (default: soft).",
    )
    citation_audit_parser.set_defaults(func=citation_audit)

    full_draft_parser = subparsers.add_parser(
        "assemble-full-draft",
        help="Assemble full draft tex and run manuscript-level conservative revision checks.",
    )
    full_draft_parser.add_argument(
        "--latex-sections-dir",
        default="",
        help="Directory containing section tex files. Defaults to draft/latex/sections.",
    )
    full_draft_parser.add_argument(
        "--section-audit-dir",
        default="",
        help="Directory containing section audit JSON files. Defaults to draft/latex/audit.",
    )
    full_draft_parser.add_argument(
        "--bib-path",
        default="",
        help="Path to bibliography file. Defaults to draft/latex/references.bib.",
    )
    full_draft_parser.add_argument(
        "--abstract-template",
        default="",
        help="Optional abstract template path. Defaults to draft/templates/abstract.tex.",
    )
    full_draft_parser.add_argument(
        "--conclusion-template",
        default="",
        help="Optional conclusion template path. Defaults to draft/templates/conclusion.tex.",
    )
    full_draft_parser.add_argument(
        "--output-main-tex",
        default="",
        help="Output path for assembled main tex. Defaults to draft/main.tex.",
    )
    full_draft_parser.add_argument(
        "--output-full-draft-tex",
        default="",
        help="Output path for full inline draft tex. Defaults to draft/full_draft_v1.tex.",
    )
    full_draft_parser.add_argument(
        "--full-draft-review-md",
        default="",
        help="Output path for full draft review report. Defaults to draft/reports/full_draft_review.md.",
    )
    full_draft_parser.add_argument(
        "--strictness",
        default="soft",
        choices=["soft", "hard"],
        help="Assembly strictness policy: soft or hard (default: soft).",
    )
    full_draft_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite generated outputs. Default false to preserve manual edits.",
    )
    full_draft_parser.set_defaults(func=assemble_full_draft)

    section_release_gate_parser = subparsers.add_parser(
        "section-release-gate",
        help="Decide section go/revise/block from consistency and section citation audits.",
    )
    section_release_gate_parser.add_argument("--section-stem", required=True, help="Section stem, e.g. sec_001_mechanism-pathways.")
    section_release_gate_parser.add_argument(
        "--section-consistency-report-json",
        default="",
        help="Path to section consistency report JSON. Defaults to latest draft/runs/run_*/section_consistency_report.json.",
    )
    section_release_gate_parser.add_argument(
        "--section-audit-json",
        default="",
        help="Path to section citation audit JSON. Defaults to draft/latex/audit/section_<stem>_audit.json.",
    )
    section_release_gate_parser.add_argument(
        "--gate-output-json",
        default="",
        help="Output section gate JSON. Defaults to draft/gates/section_<stem>_gate.json.",
    )
    section_release_gate_parser.add_argument(
        "--gate-fixlist-md",
        default="",
        help="Output section fix list markdown. Defaults to draft/gates/section_<stem>_fixlist.md.",
    )
    section_release_gate_parser.add_argument(
        "--strictness",
        default="soft",
        choices=["soft", "hard"],
        help="Gate strictness policy: soft or hard (default: soft).",
    )
    section_release_gate_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite generated outputs. Default false to preserve manual edits.",
    )
    section_release_gate_parser.set_defaults(func=section_release_gate)

    cross_section_bridge_parser = subparsers.add_parser(
        "generate-cross-section-bridges",
        help="Generate conservative inter-section bridge plan and bridge tex fragment.",
    )
    cross_section_bridge_parser.add_argument(
        "--latex-sections-dir",
        default="",
        help="Directory containing section tex files. Defaults to draft/latex/sections.",
    )
    cross_section_bridge_parser.add_argument(
        "--argument-graph-json",
        default="",
        help="Path to argument graph JSON. Defaults to references/index/argument_graph.json.",
    )
    cross_section_bridge_parser.add_argument(
        "--bridge-plan-json",
        default="",
        help="Output bridge plan JSON. Defaults to draft/bridges/bridge_plan.json.",
    )
    cross_section_bridge_parser.add_argument(
        "--bridges-tex",
        default="",
        help="Output bridges tex fragment. Defaults to draft/latex/sections/bridges.tex.",
    )
    cross_section_bridge_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite generated outputs. Default false to preserve manual edits.",
    )
    cross_section_bridge_parser.set_defaults(func=generate_cross_section_bridges)

    claim_trace_matrix_parser = subparsers.add_parser(
        "export-claim-trace-matrix",
        help="Export claim-to-evidence traceability matrix across plans, packets, and citations.",
    )
    claim_trace_matrix_parser.add_argument(
        "--claims-jsonl",
        default="",
        help="Path to claims JSONL. Defaults to references/index/claims.jsonl.",
    )
    claim_trace_matrix_parser.add_argument(
        "--paragraph-plans-dir",
        default="",
        help="Directory of paragraph plans. Defaults to draft/paragraph_plans.",
    )
    claim_trace_matrix_parser.add_argument(
        "--evidence-packets-dir",
        default="",
        help="Directory of evidence packets. Defaults to draft/evidence_packets.",
    )
    claim_trace_matrix_parser.add_argument(
        "--bib-path",
        default="",
        help="Path to bibliography file. Defaults to draft/latex/references.bib.",
    )
    claim_trace_matrix_parser.add_argument(
        "--claim-trace-matrix-csv",
        default="",
        help="Output claim trace matrix CSV. Defaults to draft/audit/claim_trace_matrix.csv.",
    )
    claim_trace_matrix_parser.add_argument(
        "--claim-trace-matrix-json",
        default="",
        help="Output claim trace matrix JSON. Defaults to draft/audit/claim_trace_matrix.json.",
    )
    claim_trace_matrix_parser.add_argument(
        "--strictness",
        default="soft",
        choices=["soft", "hard"],
        help="Trace strictness policy: soft or hard (default: soft).",
    )
    claim_trace_matrix_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite generated outputs. Default false to preserve manual edits.",
    )
    claim_trace_matrix_parser.set_defaults(func=export_claim_trace_matrix)

    figure_table_grounding_parser = subparsers.add_parser(
        "ground-figure-table-links",
        help="Audit figure/table grounding against section claims and references.",
    )
    figure_table_grounding_parser.add_argument(
        "--latex-sections-dir",
        default="",
        help="Directory containing section tex files. Defaults to draft/latex/sections.",
    )
    figure_table_grounding_parser.add_argument(
        "--figures-dir",
        default="",
        help="Figure assets directory. Defaults to figures/.",
    )
    figure_table_grounding_parser.add_argument(
        "--tables-dir",
        default="",
        help="Table CSV directory. Defaults to tables/.",
    )
    figure_table_grounding_parser.add_argument(
        "--evidence-packets-dir",
        default="",
        help="Directory of evidence packets. Defaults to draft/evidence_packets.",
    )
    figure_table_grounding_parser.add_argument(
        "--figure-table-grounding-md",
        default="",
        help="Output grounding report markdown. Defaults to draft/reports/figure_table_grounding.md.",
    )
    figure_table_grounding_parser.add_argument(
        "--figure-table-manifest-json",
        default="",
        help="Output grounding manifest JSON. Defaults to draft/reports/figure_table_manifest.json.",
    )
    figure_table_grounding_parser.add_argument(
        "--strictness",
        default="soft",
        choices=["soft", "hard"],
        help="Grounding strictness policy: soft or hard (default: soft).",
    )
    figure_table_grounding_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite generated outputs. Default false to preserve manual edits.",
    )
    figure_table_grounding_parser.set_defaults(func=ground_figure_table_links)

    latex_build_qa_parser = subparsers.add_parser(
        "latex-build-qa",
        help="Run static/optional compile QA checks for manuscript tex outputs.",
    )
    latex_build_qa_parser.add_argument(
        "--target",
        default="full",
        choices=["main", "full"],
        help="Target tex for QA: main or full (default: full).",
    )
    latex_build_qa_parser.add_argument(
        "--main-tex",
        default="",
        help="Path to main tex. Defaults to draft/main.tex.",
    )
    latex_build_qa_parser.add_argument(
        "--full-draft-tex",
        default="",
        help="Path to full draft tex. Defaults to draft/full_draft_v1.tex.",
    )
    latex_build_qa_parser.add_argument(
        "--bib-path",
        default="",
        help="Path to bibliography file. Defaults to draft/latex/references.bib.",
    )
    latex_build_qa_parser.add_argument(
        "--latex-build-report-md",
        default="",
        help="Output build QA markdown report. Defaults to draft/reports/latex_build_report.md.",
    )
    latex_build_qa_parser.add_argument(
        "--latex-build-log-txt",
        default="",
        help="Output build log text. Defaults to draft/reports/latex_build_log.txt.",
    )
    latex_build_qa_parser.add_argument(
        "--run-compiler",
        action="store_true",
        help="Attempt pdflatex compile if tool is available.",
    )
    latex_build_qa_parser.add_argument(
        "--strictness",
        default="soft",
        choices=["soft", "hard"],
        help="Build QA strictness policy: soft or hard (default: soft).",
    )
    latex_build_qa_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite generated outputs. Default false to preserve manual edits.",
    )
    latex_build_qa_parser.set_defaults(func=latex_build_qa)

    run_parser = subparsers.add_parser(
        "run",
        help="Run the pipeline through selected stages with a single beginner-friendly command.",
    )
    run_parser.add_argument(
        "--topic-frame-json",
        default="",
        help="Path to topic frame JSON/YAML. Required when selected stages include search-candidates or outline-from-evidence.",
    )
    run_parser.add_argument(
        "--from-stage",
        default="search-candidates",
        choices=PIPELINE_STAGE_CHOICES,
        help="Start stage (default: search-candidates).",
    )
    run_parser.add_argument(
        "--to-stage",
        default="full-draft",
        choices=PIPELINE_STAGE_CHOICES,
        help="End stage (default: full-draft, alias of assemble-full-draft).",
    )
    run_parser.add_argument(
        "--with-fulltext",
        action="store_true",
        help="Include fetch-fulltext stage in the run chain. Default false.",
    )
    run_parser.add_argument(
        "--strictness",
        default="soft",
        choices=["soft", "hard"],
        help="Strictness policy for supporting stages (default: soft).",
    )
    run_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwrite for supporting stages that honor overwrite semantics.",
    )
    run_parser.add_argument(
        "--run-compiler",
        action="store_true",
        help="Enable compiler execution when latex-build-qa stage is selected.",
    )
    run_parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue executing next stage even if one stage fails. Default false (fail-fast).",
    )
    run_parser.set_defaults(func=run_pipeline)

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
    if args.command == "fetch-fulltext":
        if args.max_retries < 0:
            parser.error("--max-retries must be >= 0")
        if args.timeout_seconds <= 0:
            parser.error("--timeout-seconds must be > 0")

    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
