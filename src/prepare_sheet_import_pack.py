"""
prepare_sheet_import_pack.py

Create a Google Sheets import pack so Sheets can be used as the visible
system-of-record for assignment demo videos.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")
    return pd.read_csv(path, encoding="utf-8-sig")


def _safe_to_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _ordered_columns(df: pd.DataFrame, preferred: Iterable[str]) -> pd.DataFrame:
    preferred_list = [col for col in preferred if col in df.columns]
    extra = [col for col in df.columns if col not in preferred_list]
    return df[preferred_list + extra]


def build_pack(
    raw_csv: Path,
    enriched_csv: Path,
    personas_csv: Path,
    routed_csv: Path,
    outreach_csv: Path,
    out_dir: Path,
) -> None:
    raw_df = _read_csv(raw_csv)
    enriched_df = _read_csv(enriched_csv)
    personas_df = _read_csv(personas_csv)
    routed_df = _read_csv(routed_csv)
    outreach_df = _read_csv(outreach_csv)

    raw_out = _ordered_columns(
        raw_df,
        [
            "id",
            "name",
            "title",
            "company",
            "event_role",
            "session_topic",
            "source_url",
            "created_at",
        ],
    )
    enriched_out = _ordered_columns(
        enriched_df,
        [
            "id",
            "name",
            "title",
            "company",
            "seniority",
            "industry",
            "industry_relevance_score",
            "linkedin_url",
            "linkedin_confidence",
            "linkedin_source",
            "linkedin_lookup_attempted",
            "job_history",
            "signals",
            "enrichment_confidence_score",
            "evidence_score",
            "enrichment_status",
        ],
    )
    personas_out = _ordered_columns(
        personas_df,
        [
            "id",
            "name",
            "company",
            "persona_archetype",
            "persona_summary",
            "context_summary",
            "personalization_themes",
            "relevance_score",
            "recommended_hook",
            "assign_to",
            "persona_reason",
            "persona_status",
            "llm_error",
        ],
    )

    routed_df = routed_df.copy()
    outreach_df = outreach_df.copy()
    if "id" in routed_df.columns:
        routed_df["id"] = routed_df["id"].astype(str)
    if "id" in outreach_df.columns:
        outreach_df["id"] = outreach_df["id"].astype(str)

    # Merge routed + outreach drafts for a single demo queue tab.
    queue_base_cols = [
        "id",
        "name",
        "title",
        "company",
        "seniority",
        "industry",
        "relevance_score",
        "final_route",
        "outreach_sequence",
        "route_reason",
        "icp_match",
        "segment_cluster",
        "account_priority_score",
        "outreach_score",
        "outreach_approved",
        "outreach_status",
    ]
    queue_msg_cols = [
        "email_subject_a",
        "email_subject_b",
        "email_body_pre_event",
        "email_body_during_event",
        "email_body_post_event",
        "linkedin_note",
    ]

    queue_df = routed_df.copy()
    if "id" in queue_df.columns and "id" in outreach_df.columns:
        message_slice = outreach_df[[col for col in ["id"] + queue_msg_cols if col in outreach_df.columns]].copy()
        queue_df = queue_df.merge(message_slice, on="id", how="left")

    queue_out = _ordered_columns(queue_df, queue_base_cols + queue_msg_cols)

    lead_assignment_seed = pd.DataFrame(
        [
            {"rep_name": "Sarah Chen", "rep_type": "Senior AE", "tier": "Tier A"},
            {"rep_name": "Raj Patel", "rep_type": "AE", "tier": "Tier B"},
            {"rep_name": "Priya Kumar", "rep_type": "SDR", "tier": "Tier C"},
            {"rep_name": "Partnership Lead", "rep_type": "Senior AE", "tier": "VC Track"},
        ]
    )

    suppression_seed = pd.DataFrame(columns=["name", "company", "reason", "added_at"])

    _safe_to_csv(raw_out, out_dir / "Raw_Contacts.csv")
    _safe_to_csv(enriched_out, out_dir / "Enriched_Contacts.csv")
    _safe_to_csv(personas_out, out_dir / "AI_Personas.csv")
    _safe_to_csv(queue_out, out_dir / "Outreach_Queue.csv")
    _safe_to_csv(lead_assignment_seed, out_dir / "Lead_Assignment.csv")
    _safe_to_csv(suppression_seed, out_dir / "Suppression_List.csv")

    instructions = [
        "# Google Sheets Import Pack",
        "",
        "Import files in this order:",
        "1. Raw_Contacts.csv -> Raw_Contacts tab",
        "2. Enriched_Contacts.csv -> Enriched_Contacts tab",
        "3. AI_Personas.csv -> AI_Personas tab",
        "4. Outreach_Queue.csv -> Outreach_Queue tab",
        "5. Lead_Assignment.csv -> Lead_Assignment tab",
        "6. Suppression_List.csv -> Suppression_List tab",
        "",
        "Reference schema and formulas: docs/sheet_schema.md",
    ]
    (out_dir / "README_IMPORT.md").write_text("\n".join(instructions), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare Google Sheets import pack from pipeline outputs.")
    parser.add_argument("--raw-csv", default="data/speakers_raw.csv")
    parser.add_argument("--enriched-csv", default="data/speakers_enriched.csv")
    parser.add_argument("--personas-csv", default="data/speakers_personas.csv")
    parser.add_argument("--routed-csv", default="data/speakers_routed.csv")
    parser.add_argument("--outreach-csv", default="output/outreach_drafts.csv")
    parser.add_argument("--out-dir", default="output/sheets")
    args = parser.parse_args()

    build_pack(
        raw_csv=Path(args.raw_csv),
        enriched_csv=Path(args.enriched_csv),
        personas_csv=Path(args.personas_csv),
        routed_csv=Path(args.routed_csv),
        outreach_csv=Path(args.outreach_csv),
        out_dir=Path(args.out_dir),
    )
    print(f"Google Sheets import pack written to: {args.out_dir}")


if __name__ == "__main__":
    main()
