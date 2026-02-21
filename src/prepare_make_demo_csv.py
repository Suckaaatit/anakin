"""
prepare_make_demo_csv.py

Builds a recording-friendly input CSV for Make:
- prefilled GTM input fields
- blank AI output fields to be written back by automation
"""

from pathlib import Path

import pandas as pd


def main() -> None:
    handoff_path = Path("output/make_handoff.csv")
    routed_path = Path("data/speakers_routed.csv")
    output_path = Path("output/make_demo_input.csv")

    if not handoff_path.exists():
        raise SystemExit(f"Missing required file: {handoff_path}")
    if not routed_path.exists():
        raise SystemExit(f"Missing required file: {routed_path}")

    handoff_df = pd.read_csv(handoff_path, encoding="utf-8-sig")
    routed_df = pd.read_csv(routed_path, encoding="utf-8-sig")

    routed_cols = ["id", "session_topic"]
    routed_subset = routed_df[routed_cols].copy()
    routed_subset["id"] = routed_subset["id"].astype(str)

    base = handoff_df.copy()
    base["id"] = base["id"].astype(str)
    base = base.merge(routed_subset, on="id", how="left")
    base["session_topic"] = base["session_topic"].fillna("TechSparks discussion")

    # Keep demo size small so your live recording run is quick and reliable.
    base = base.head(25).copy()

    base["persona_summary"] = ""
    base["context_summary"] = ""
    base["personalization_themes"] = ""
    base["recommended_hook"] = ""
    base["make_status"] = "PENDING"
    base["last_run_at"] = ""
    base["error_message"] = ""

    ordered_columns = [
        "id",
        "name",
        "title",
        "company",
        "seniority",
        "industry",
        "session_topic",
        "relevance_score",
        "final_route",
        "outreach_sequence",
        "account_priority_score",
        "persona_summary",
        "context_summary",
        "personalization_themes",
        "recommended_hook",
        "make_status",
        "last_run_at",
        "error_message",
    ]
    base = base[ordered_columns]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    base.to_csv(output_path, encoding="utf-8-sig", index=False)

    print(f"Demo CSV written: {output_path} (rows={len(base)})")


if __name__ == "__main__":
    main()
