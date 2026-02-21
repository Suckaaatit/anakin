"""
audit_assignment.py

Rule-by-rule assignment compliance and error audit for TechSparks GTM prototype.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUTPUT = ROOT / "output"
DOCS = ROOT / "docs"


@dataclass
class RuleResult:
    rule: str
    status: str  # MET | PARTIAL | NOT_MET
    evidence: str
    notes: str


def read_csv(path: Path) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    if not path.exists():
        return None, f"Missing file: {path}"
    try:
        return pd.read_csv(path, encoding="utf-8-sig"), None
    except Exception as exc:
        return None, f"Failed to read {path}: {exc}"


def contains_term(paths: List[Path], term: str) -> bool:
    term = term.lower()
    for path in paths:
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="ignore").lower()
            if term in content:
                return True
        except Exception:
            continue
    return False


def count_linkedin_found(df: pd.DataFrame) -> int:
    if "linkedin_url" not in df.columns:
        return 0
    bad = {"", "NOT_FOUND", "ERROR", "SKIP_INVALID_NAME", "NOT_ATTEMPTED_FAST_MODE"}
    return int((~df["linkedin_url"].fillna("").astype(str).isin(bad)).sum())


def non_empty_count(df: pd.DataFrame, col: str) -> int:
    if col not in df.columns:
        return 0
    return int(df[col].fillna("").astype(str).str.strip().ne("").sum())


def assess() -> Dict[str, object]:
    raw_path = DATA / "speakers_raw.csv"
    enriched_path = DATA / "speakers_enriched.csv"
    personas_path = DATA / "speakers_personas.csv"
    routed_path = DATA / "speakers_routed.csv"
    outreach_path = OUTPUT / "outreach_drafts.csv"

    raw_df, raw_err = read_csv(raw_path)
    enriched_df, enriched_err = read_csv(enriched_path)
    personas_df, personas_err = read_csv(personas_path)
    routed_df, routed_err = read_csv(routed_path)
    outreach_df, outreach_err = read_csv(outreach_path)

    errors: List[str] = []
    for maybe_err in (raw_err, enriched_err, personas_err, routed_err, outreach_err):
        if maybe_err:
            errors.append(maybe_err)

    rules: List[RuleResult] = []

    # Rule: 150-200 contacts sourced.
    if raw_df is None:
        rules.append(
            RuleResult(
                rule="Public list/scraper with 150-200 contacts",
                status="NOT_MET",
                evidence=str(raw_path),
                notes="Raw file missing or unreadable.",
            )
        )
    else:
        rows = len(raw_df)
        if 150 <= rows <= 200:
            rules.append(
                RuleResult(
                    rule="Public list/scraper with 150-200 contacts",
                    status="MET",
                    evidence=f"{raw_path} ({rows} rows)",
                    notes="Contact count is within assignment target.",
                )
            )
        else:
            rules.append(
                RuleResult(
                    rule="Public list/scraper with 150-200 contacts",
                    status="PARTIAL",
                    evidence=f"{raw_path} ({rows} rows)",
                    notes="Count is outside target range 150-200.",
                )
            )

    # Rule: enrichment fields.
    enrichment_required = {
        "linkedin_url",
        "job_history",
        "seniority",
        "industry_relevance_score",
        "signals",
    }
    if enriched_df is None:
        rules.append(
            RuleResult(
                rule="Enrichment includes LinkedIn, job history, seniority, industry relevance, signals",
                status="NOT_MET",
                evidence=str(enriched_path),
                notes="Enriched file missing or unreadable.",
            )
        )
    else:
        missing = sorted(enrichment_required - set(enriched_df.columns))
        if missing:
            rules.append(
                RuleResult(
                    rule="Enrichment includes LinkedIn, job history, seniority, industry relevance, signals",
                    status="NOT_MET",
                    evidence=str(enriched_path),
                    notes=f"Missing columns: {', '.join(missing)}",
                )
            )
        else:
            linkedin_found = count_linkedin_found(enriched_df)
            status = "MET" if linkedin_found > 0 else "PARTIAL"
            note = (
                f"All required columns present. LinkedIn found count={linkedin_found}."
                if linkedin_found > 0
                else "Columns present, but latest run has zero LinkedIn matches."
            )
            rules.append(
                RuleResult(
                    rule="Enrichment includes LinkedIn, job history, seniority, industry relevance, signals",
                    status=status,
                    evidence=str(enriched_path),
                    notes=note,
                )
            )

    # Rule: persona context + themes.
    persona_required = {
        "persona_archetype",
        "persona_summary",
        "context_summary",
        "personalization_themes",
    }
    if personas_df is None:
        rules.append(
            RuleResult(
                rule="AI persona/context generation with personalization themes",
                status="NOT_MET",
                evidence=str(personas_path),
                notes="Persona file missing or unreadable.",
            )
        )
    else:
        missing = sorted(persona_required - set(personas_df.columns))
        if missing:
            rules.append(
                RuleResult(
                    rule="AI persona/context generation with personalization themes",
                    status="NOT_MET",
                    evidence=str(personas_path),
                    notes=f"Missing columns: {', '.join(missing)}",
                )
            )
        else:
            empty_personas = len(personas_df) - non_empty_count(personas_df, "persona_summary")
            status = "MET" if empty_personas == 0 else "PARTIAL"
            rules.append(
                RuleResult(
                    rule="AI persona/context generation with personalization themes",
                    status=status,
                    evidence=str(personas_path),
                    notes=f"Empty persona summaries: {empty_personas}",
                )
            )

    # Rule: outreach pre/during/post + LinkedIn.
    outreach_required = {
        "persona_archetype",
        "relevance_score",
        "final_route",
        "outreach_sequence",
        "email_subject_a",
        "email_body_pre_event",
        "email_body_during_event",
        "email_body_post_event",
        "linkedin_note",
    }
    if outreach_df is None:
        rules.append(
            RuleResult(
                rule="Outreach workflow includes email + LinkedIn and pre/during/post stages",
                status="NOT_MET",
                evidence=str(outreach_path),
                notes="Outreach file missing or unreadable.",
            )
        )
    else:
        missing = sorted(outreach_required - set(outreach_df.columns))
        if missing:
            rules.append(
                RuleResult(
                    rule="Outreach workflow includes email + LinkedIn and pre/during/post stages",
                    status="NOT_MET",
                    evidence=str(outreach_path),
                    notes=f"Missing columns: {', '.join(missing)}",
                )
            )
        else:
            rules.append(
                RuleResult(
                    rule="Outreach workflow includes email + LinkedIn and pre/during/post stages",
                    status="MET",
                    evidence=str(outreach_path),
                    notes="All required multi-stage outreach columns are present.",
                )
            )

    # Rule: routing and dedup.
    if routed_df is None:
        rules.append(
            RuleResult(
                rule="Lead assignment logic and duplicate prevention",
                status="NOT_MET",
                evidence=str(routed_path),
                notes="Routed file missing or unreadable.",
            )
        )
    else:
        has_cols = {"final_route", "is_duplicate"}.issubset(set(routed_df.columns))
        if not has_cols:
            rules.append(
                RuleResult(
                    rule="Lead assignment logic and duplicate prevention",
                    status="NOT_MET",
                    evidence=str(routed_path),
                    notes="Missing final_route or is_duplicate columns.",
                )
            )
        else:
            dupes = int(routed_df["is_duplicate"].astype(str).str.lower().isin({"true", "1", "yes"}).sum())
            routes = sorted(set(routed_df["final_route"].dropna().astype(str).tolist()))
            rules.append(
                RuleResult(
                    rule="Lead assignment logic and duplicate prevention",
                    status="MET",
                    evidence=str(routed_path),
                    notes=f"Routes={', '.join(routes)}; duplicates flagged={dupes}.",
                )
            )

    # Hard rule: no "Anakin" in outputs.
    output_paths = [enriched_path, personas_path, routed_path, outreach_path]
    if contains_term(output_paths, "anakin"):
        rules.append(
            RuleResult(
                rule='No mention of "Anakin" in outreach/output',
                status="NOT_MET",
                evidence="data/ + output/",
                notes='Found "Anakin" in generated output files.',
            )
        )
    else:
        rules.append(
            RuleResult(
                rule='No mention of "Anakin" in outreach/output',
                status="MET",
                evidence="data/ + output/",
                notes='No "Anakin" term found in generated output files.',
            )
        )

    # Documentation and prototype artifacts.
    docs_checks = {
        "Workflow diagram present": DOCS / "workflow_diagram.md",
        "No-code scenario spec present": DOCS / "make_scenario_spec.md",
        "Not-automated rationale present": DOCS / "WHAT_WAS_NOT_AUTOMATED.md",
        "TypeScript UI present": ROOT / "ui" / "src" / "main.ts",
    }
    for rule_name, path in docs_checks.items():
        rules.append(
            RuleResult(
                rule=rule_name,
                status="MET" if path.exists() else "NOT_MET",
                evidence=str(path),
                notes="Found." if path.exists() else "Missing.",
            )
        )

    # KPI rule (deliverability + acceptance + customization insight).
    readme = ROOT / "README.md"
    readme_text = readme.read_text(encoding="utf-8", errors="ignore").lower() if readme.exists() else ""
    has_kpi_section = "key performance observations" in readme_text
    mentions_deliverability = "deliverability" in readme_text
    mentions_acceptance = "acceptance" in readme_text
    if has_kpi_section and mentions_deliverability and mentions_acceptance:
        kpi_status = "MET"
        kpi_note = "KPI section includes deliverability and acceptance discussion."
    elif has_kpi_section:
        kpi_status = "PARTIAL"
        kpi_note = "KPI section present, but deliverability/acceptance coverage is incomplete."
    else:
        kpi_status = "NOT_MET"
        kpi_note = "KPI section missing."
    rules.append(
        RuleResult(
            rule="KPI observations included (deliverability, acceptance, customization)",
            status=kpi_status,
            evidence=str(readme),
            notes=kpi_note,
        )
    )

    # Additional error probes.
    if enriched_df is not None:
        if "email_pattern" in enriched_df.columns:
            bad_emails = int(
                enriched_df["email_pattern"]
                .fillna("")
                .astype(str)
                .str.contains(r"\.com\.com$", regex=True, case=False)
                .sum()
            )
            if bad_emails > 0:
                errors.append(f"Found {bad_emails} malformed email patterns ending with .com.com")

        if raw_df is not None and len(enriched_df) != len(raw_df):
            errors.append(
                f"Row mismatch: raw={len(raw_df)}, enriched={len(enriched_df)}. "
                "Re-run full pipeline to align outputs."
            )

    status_counts = {
        "MET": sum(1 for r in rules if r.status == "MET"),
        "PARTIAL": sum(1 for r in rules if r.status == "PARTIAL"),
        "NOT_MET": sum(1 for r in rules if r.status == "NOT_MET"),
    }

    return {
        "counts": status_counts,
        "rules": [asdict(r) for r in rules],
        "errors": errors,
    }


def print_report(report: Dict[str, object]) -> None:
    counts = report["counts"]
    print("Assignment Compliance Summary")
    print("=============================")
    print(f"Met: {counts['MET']}")
    print(f"Partial: {counts['PARTIAL']}")
    print(f"Not met: {counts['NOT_MET']}")
    print("")

    print("Rule Results")
    print("------------")
    for item in report["rules"]:
        print(f"[{item['status']}] {item['rule']}")
        print(f"  Evidence: {item['evidence']}")
        print(f"  Notes: {item['notes']}")
    print("")

    print("Error Audit")
    print("-----------")
    errors: List[str] = report["errors"]
    if not errors:
        print("No blocking errors detected.")
    else:
        for err in errors:
            print(f"- {err}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit assignment compliance and errors.")
    parser.add_argument("--json", action="store_true", help="Print report as JSON.")
    args = parser.parse_args()

    report = assess()
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print_report(report)


if __name__ == "__main__":
    main()
