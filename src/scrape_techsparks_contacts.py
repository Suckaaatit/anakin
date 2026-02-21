"""
scrape_techsparks_contacts.py -- Build real contact list from public TechSparks pages.

This script scrapes agenda item pages from TechSparks 2025 and extracts speaker
name/title/company/session context into the seed CSV schema used by the pipeline.
"""

from __future__ import annotations

import argparse
import html
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests

BASE_URL = "https://techsparks.yourstory.com"
LANDING_PATH = "/2025"
LEGACY_SPEAKERS_PATH = "/2024"
DEFAULT_TARGET = 180
MAX_WORKERS = 12

REQUIRED_COLUMNS = [
    "id",
    "name",
    "title",
    "company",
    "event_role",
    "session_topic",
    "source_url",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Accept-Encoding": "identity",
}


@dataclass(frozen=True)
class AgendaItem:
    href: str
    event_role: str
    session_topic: str


def _clean_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _slug_to_topic(href: str) -> str:
    slug = href.rsplit("/", 1)[-1]
    words = [w for w in slug.split("-") if w]
    return " ".join(words).title()


def _split_title_company(raw: str) -> Tuple[str, str]:
    text = _clean_text(raw)
    if not text:
        return "Unknown Title", "Unknown Company"

    if "," in text:
        left, right = text.split(",", 1)
        title = left.strip() or "Unknown Title"
        company = right.strip() or "Unknown Company"
        return title, company

    lower = text.lower()
    if " at " in lower:
        idx = lower.rfind(" at ")
        title = text[:idx].strip() or "Unknown Title"
        company = text[idx + 4:].strip() or "Unknown Company"
        return title, company

    return text, "Unknown Company"


def _normalize_person_key(name: str, title: str, company: str) -> str:
    norm = lambda s: re.sub(r"\s+", " ", s).strip().lower()
    return f"{norm(name)}|{norm(title)}|{norm(company)}"


def fetch(url: str, timeout: int = 20) -> str:
    response = requests.get(url, timeout=timeout, headers=HEADERS)
    response.raise_for_status()
    return response.text


def extract_agenda_items(landing_html: str) -> List[AgendaItem]:
    """
    Parse agenda item links + session metadata from the landing page.
    """
    detailed_pattern = re.compile(
        r'<a fs-list-element="item-link" href="(?P<href>/blr-25-agenda-items/[^"]+)"'
        r'.*?<div class="ts_agenda-badge"><div>(?P<role>.*?)</div>'
        r'.*?<div class="ts_agenda-topic-title">(?P<topic>.*?)</div>',
        re.IGNORECASE | re.DOTALL,
    )

    items: Dict[str, AgendaItem] = {}
    for match in detailed_pattern.finditer(landing_html):
        href = match.group("href").strip()
        role = _clean_text(match.group("role")) or "Speaker"
        topic = _clean_text(match.group("topic")) or _slug_to_topic(href)
        if href not in items:
            items[href] = AgendaItem(href=href, event_role=role, session_topic=topic)

    # Fallback if website markup changes.
    if not items:
        href_pattern = re.compile(r'href="(/blr-25-agenda-items/[^"]+)"', re.IGNORECASE)
        for href in href_pattern.findall(landing_html):
            href = href.strip()
            if href not in items:
                items[href] = AgendaItem(
                    href=href,
                    event_role="Speaker",
                    session_topic=_slug_to_topic(href),
                )

    return list(items.values())


def extract_speakers(item_html: str) -> List[Tuple[str, str, str]]:
    """
    Parse speaker blocks from a single agenda item page.
    Returns list of (name, title, company).
    """
    pattern = re.compile(
        r'<div class="ts_agenda-speaker-content">'
        r'\s*<div class="ts_agenda-speaker-title">(.*?)</div>'
        r'\s*<div>(.*?)</div>',
        re.IGNORECASE | re.DOTALL,
    )

    speakers: List[Tuple[str, str, str]] = []
    seen = set()
    for raw_name, raw_title_company in pattern.findall(item_html):
        name = _clean_text(raw_name)
        if len(name.split()) < 2:
            continue
        title, company = _split_title_company(raw_title_company)
        key = _normalize_person_key(name, title, company)
        if key in seen:
            continue
        seen.add(key)
        speakers.append((name, title, company))

    return speakers


def scrape_item(item: AgendaItem) -> List[Dict[str, str]]:
    url = urljoin(BASE_URL, item.href)
    try:
        html_text = fetch(url, timeout=20)
    except Exception:
        return []

    rows: List[Dict[str, str]] = []
    for name, title, company in extract_speakers(html_text):
        rows.append(
            {
                "name": name,
                "title": title,
                "company": company,
                "event_role": item.event_role,
                "session_topic": item.session_topic,
                "source_url": url,
            }
        )
    return rows


def extract_legacy_2024_speakers(legacy_html: str) -> List[Dict[str, str]]:
    """
    Parse public speaker cards from TechSparks 2024 landing page.
    """
    pattern = re.compile(
        r'<a href="(?P<href>/speakers[^"]+)" class="link-block-21[^"]*">'
        r'.*?<div class="agenda__speaker-name">(?P<name>.*?)</div>'
        r'\s*<div class="agenda__speaker-desc">(?P<desc>.*?)</div>',
        re.IGNORECASE | re.DOTALL,
    )

    rows: List[Dict[str, str]] = []
    seen = set()
    for match in pattern.finditer(legacy_html):
        name = _clean_text(match.group("name"))
        if len(name.split()) < 2:
            continue

        title, company = _split_title_company(match.group("desc"))
        key = _normalize_person_key(name, title, company)
        if key in seen:
            continue
        seen.add(key)

        href = match.group("href").strip()
        rows.append(
            {
                "name": name,
                "title": title,
                "company": company,
                "event_role": "Speaker",
                "session_topic": "TechSparks 2024 Speaker Program",
                "source_url": urljoin(BASE_URL, href),
            }
        )

    return rows


def aggregate_contacts(rows: Iterable[Dict[str, str]], target: int) -> pd.DataFrame:
    """
    Build contact list, prioritizing unique contacts and preserving session context.
    """
    contacts: Dict[str, Dict[str, str]] = {}
    session_counts: Dict[str, int] = {}

    for row in rows:
        key = _normalize_person_key(row["name"], row["title"], row["company"])
        session_counts[key] = session_counts.get(key, 0) + 1
        if key not in contacts:
            contacts[key] = dict(row)

    ordered_keys = sorted(
        contacts.keys(),
        key=lambda k: (-session_counts.get(k, 0), contacts[k]["name"].lower()),
    )

    selected_rows = [contacts[k] for k in ordered_keys[:target]]

    # If unique contacts are below target, allow additional session rows for coverage.
    if len(selected_rows) < target:
        seen_contact = set(ordered_keys[: len(selected_rows)])
        for row in rows:
            key = _normalize_person_key(row["name"], row["title"], row["company"])
            session_key = f"{key}|{row['session_topic'].lower()}"
            if session_key in seen_contact:
                continue
            seen_contact.add(session_key)
            selected_rows.append(dict(row))
            if len(selected_rows) >= target:
                break

    final_rows = []
    for idx, row in enumerate(selected_rows, start=1):
        out = {col: row.get(col, "") for col in REQUIRED_COLUMNS if col != "id"}
        out["id"] = idx
        final_rows.append(out)

    return pd.DataFrame(final_rows, columns=REQUIRED_COLUMNS)


def scrape_contacts(target: int = DEFAULT_TARGET) -> pd.DataFrame:
    landing_url = urljoin(BASE_URL, LANDING_PATH)
    landing_html = fetch(landing_url, timeout=30)
    agenda_items = extract_agenda_items(landing_html)

    scraped_rows: List[Dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(scrape_item, item) for item in agenda_items]
        for fut in as_completed(futures):
            scraped_rows.extend(fut.result())

    # Add additional public speaker cards from 2024 page to avoid synthetic expansion.
    try:
        legacy_html = fetch(urljoin(BASE_URL, LEGACY_SPEAKERS_PATH), timeout=30)
        scraped_rows.extend(extract_legacy_2024_speakers(legacy_html))
    except Exception:
        # Legacy source is optional.
        pass

    if not scraped_rows:
        raise RuntimeError("No contacts scraped from TechSparks public pages.")

    return aggregate_contacts(scraped_rows, target=target)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape TechSparks contacts to CSV")
    parser.add_argument(
        "--target",
        type=int,
        default=DEFAULT_TARGET,
        help=f"Target rows in output (default: {DEFAULT_TARGET})",
    )
    parser.add_argument(
        "--output",
        default="data/speakers_raw.csv",
        help="Output CSV path (default: data/speakers_raw.csv)",
    )
    args = parser.parse_args()

    df = scrape_contacts(target=args.target)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False, encoding="utf-8-sig")
    print(f"Scraped {len(df)} contacts into {output}")


if __name__ == "__main__":
    main()
