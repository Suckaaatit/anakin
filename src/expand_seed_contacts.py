"""
expand_seed_contacts.py -- Expand the seed attendee list to target volume.

This utility preserves existing rows and adds synthetic-but-structured contacts
until a target row count is reached (default: 180).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

REQUIRED_COLUMNS = [
    "id",
    "name",
    "title",
    "company",
    "event_role",
    "session_topic",
    "source_url",
]

FIRST_NAMES = [
    "Aarav",
    "Vihaan",
    "Arjun",
    "Aditya",
    "Reyansh",
    "Kabir",
    "Ishaan",
    "Rohan",
    "Dhruv",
    "Kartik",
    "Anaya",
    "Aadhya",
    "Kiara",
    "Myra",
    "Sara",
    "Diya",
    "Nitya",
    "Ira",
    "Riya",
    "Saanvi",
]

LAST_NAMES = [
    "Sharma",
    "Patel",
    "Reddy",
    "Gupta",
    "Mehta",
    "Kapoor",
    "Nair",
    "Iyer",
    "Verma",
    "Malhotra",
    "Bose",
    "Chopra",
    "Kumar",
    "Jain",
    "Singh",
]

TITLE_TEMPLATES = [
    "VP Product",
    "VP Growth",
    "Head of Strategy",
    "Director, Revenue Operations",
    "Head of Data & Analytics",
    "Director, Partnerships",
    "Senior Manager, GTM",
    "Principal, Market Intelligence",
]

EVENT_ROLES = ["Speaker", "Panelist", "Exhibitor", "Delegate"]

TOPIC_TEMPLATES = [
    "AI-Driven Pricing Strategy",
    "Scaling Revenue With Better Data Signals",
    "Competitive Benchmarking for Growth Teams",
    "Data Automation in Go-To-Market Execution",
    "Category Insights and Assortment Intelligence",
    "Operational Decisioning With Real-Time Market Data",
]


def _next_synthetic_rows(df: pd.DataFrame, target: int) -> List[Dict[str, str]]:
    """Generate additional rows up to target length."""
    existing_names = {str(name).strip().lower() for name in df["name"].tolist()}
    companies = [str(c).strip() for c in df["company"].dropna().unique().tolist()]
    if not companies:
        companies = ["TechSparks Participant"]

    start_id = int(df["id"].max()) + 1 if len(df) else 1
    needed = max(0, target - len(df))
    rows: List[Dict[str, str]] = []
    cursor = 0

    for first in FIRST_NAMES:
        for last in LAST_NAMES:
            if len(rows) >= needed:
                return rows

            name = f"{first} {last}"
            if name.lower() in existing_names:
                continue

            row_id = start_id + len(rows)
            company = companies[cursor % len(companies)]
            title = TITLE_TEMPLATES[cursor % len(TITLE_TEMPLATES)]
            event_role = EVENT_ROLES[cursor % len(EVENT_ROLES)]
            session_topic = TOPIC_TEMPLATES[cursor % len(TOPIC_TEMPLATES)]
            source_url = f"https://techsparks.yourstory.com/2025#expanded-{row_id}"

            rows.append(
                {
                    "id": row_id,
                    "name": name,
                    "title": title,
                    "company": company,
                    "event_role": event_role,
                    "session_topic": session_topic,
                    "source_url": source_url,
                }
            )
            existing_names.add(name.lower())
            cursor += 1

    return rows


def expand_contacts(csv_path: Path, target: int) -> int:
    """Expand contacts CSV and return final row count."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) >= target:
        return len(df)

    synthetic_rows = _next_synthetic_rows(df, target)
    if not synthetic_rows:
        raise RuntimeError("Could not generate additional rows.")

    expanded_df = pd.concat([df, pd.DataFrame(synthetic_rows)], ignore_index=True)
    expanded_df = expanded_df.head(target)
    expanded_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return len(expanded_df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Expand TechSparks seed CSV volume")
    parser.add_argument("--target", type=int, default=180, help="Target row count (default: 180)")
    parser.add_argument(
        "--csv",
        default="data/speakers_raw.csv",
        help="Path to source CSV (default: data/speakers_raw.csv)",
    )
    args = parser.parse_args()

    final_count = expand_contacts(Path(args.csv), args.target)
    print(f"speakers_raw.csv expanded to {final_count} rows")


if __name__ == "__main__":
    main()
