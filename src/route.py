"""
route.py — Lead routing and deduplication module for TechSparks GTM Automation.

Handles contact deduplication, ICP matching, segment clustering, and route assignment.
"""

import logging
import os
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
from thefuzz import fuzz

from enrich import normalize_company_name

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data/errors.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# R3 fix — Company alias normalization
COMPANY_ALIASES: Dict[str, str] = {
    "zerodha broking": "zerodha",
    "zerodha pvt": "zerodha",
    "infosys bpm": "infosys",
    "ola electric": "ola",
    "meta": "meta"
}

# R6 fix — Expanded ICP
ICP_CONFIG: Dict[str, Any] = {
    "seniority": {"C-Suite / Founder", "VP", "Director / Head"},
    "industries": {
        "fintech", "ecommerce_d2c", "saas_b2b", "payments",
        "ai_tech", "marketplace", "logistics"
    },
    "min_relevance": 60
}

SEGMENT_CLUSTER_MAP: Dict[tuple, str] = {
    ("C-Suite / Founder", "fintech"): "Fintech Founder",
    ("C-Suite / Founder", "ecommerce_d2c"): "D2C Founder",
    ("C-Suite / Founder", "saas_b2b"): "SaaS Founder",
    ("VP", "fintech"): "Fintech Revenue Leader",
    ("VP", "saas_b2b"): "SaaS Revenue Leader",
}


def normalize_for_dedup(name: str, company: str) -> str:
    """
    Normalize name and company for deduplication.
    
    Args:
        name: Person's name.
        company: Company name.
        
    Returns:
        Normalized dedup key.
    """
    canonical = normalize_company_name(company)
    
    # Check aliases
    for alias, canonical_name in COMPANY_ALIASES.items():
        if alias in canonical:
            canonical = canonical_name
            break
    
    return f"{name.lower().strip()}|{canonical}"


def _fuzzy_block_key(name: str, company: str) -> str:
    """
    Build a small blocking key for fuzzy dedup.

    This reduces candidate comparisons from O(m²) to near-linear for large lists.
    """
    name_norm = name.lower().strip()
    company_norm = normalize_company_name(company)
    first_char = name_norm[:1] if name_norm else "_"
    return f"{company_norm[:12]}|{first_char}"


def deduplicate(df: pd.DataFrame, fuzzy_threshold: int = 85) -> pd.DataFrame:
    """
    Deduplicate contacts using 2-pass approach.
    
    Pass 1: Exact match on LinkedIn URL
    Pass 2: Fuzzy match on normalized name+company
    
    Args:
        df: Input DataFrame.
        fuzzy_threshold: Fuzzy matching threshold.
        
    Returns:
        DataFrame with is_duplicate column added.
    """
    df = df.copy()
    df["is_duplicate"] = False
    
    # Pass 1: Exact LinkedIn URL match
    linkedin_urls = df["linkedin_url"].dropna()
    valid_urls = linkedin_urls[
        ~linkedin_urls.isin(
            [
                "",
                "NOT_FOUND",
                "ERROR",
                "SKIP_INVALID_NAME",
                "NOT_ATTEMPTED_FAST_MODE",
                "NOT_AVAILABLE",
                "NOT_PUBLICLY_AVAILABLE",
            ]
        )
    ]
    url_counts = valid_urls.value_counts()
    duplicate_urls = url_counts[url_counts > 1].index.tolist()
    
    for url in duplicate_urls:
        indices = df[df["linkedin_url"] == url].index.tolist()
        # Mark all but first as duplicate
        for idx in indices[1:]:
            df.at[idx, "is_duplicate"] = True
    
    # Pass 2: Fuzzy match on rows without valid LinkedIn
    no_linkedin_mask = (
        df["linkedin_url"].isna() | 
        df["linkedin_url"].isin(
            [
                "",
                "NOT_FOUND",
                "ERROR",
                "SKIP_INVALID_NAME",
                "NOT_ATTEMPTED_FAST_MODE",
                "NOT_AVAILABLE",
                "NOT_PUBLICLY_AVAILABLE",
            ]
        )
    )
    no_linkedin_df = df[no_linkedin_mask & ~df["is_duplicate"]]
    
    dedup_keys = []
    for idx, row in no_linkedin_df.iterrows():
        key = normalize_for_dedup(row["name"], row["company"])
        block_key = _fuzzy_block_key(row["name"], row["company"])
        dedup_keys.append((idx, key, block_key))

    # Exact key pass first for no-linkedin rows.
    seen_exact: Dict[str, int] = {}
    fuzzy_blocks: Dict[str, list] = defaultdict(list)
    for idx, key, block_key in dedup_keys:
        if key in seen_exact:
            df.at[idx, "is_duplicate"] = True
            continue
        seen_exact[key] = idx
        fuzzy_blocks[block_key].append((idx, key))

    # Fuzzy compare within blocks only.
    marked_indices = set()
    block_max = int(os.getenv("FUZZY_BLOCK_MAX", "180"))
    for block_key, entries in fuzzy_blocks.items():
        if len(entries) <= 1:
            continue
        if len(entries) > block_max:
            logger.warning(
                f"Skipping fuzzy comparisons for oversized block '{block_key}' with {len(entries)} rows"
            )
            continue

        for i, (idx1, key1) in enumerate(entries):
            if idx1 in marked_indices:
                continue
            for idx2, key2 in entries[i + 1 :]:
                if idx2 in marked_indices:
                    continue
                if fuzz.ratio(key1, key2) >= fuzzy_threshold:
                    df.at[idx2, "is_duplicate"] = True
                    marked_indices.add(idx2)
    
    duplicate_count = df["is_duplicate"].sum()
    print(f"Deduplication: Found {duplicate_count} duplicates")
    
    return df


def get_send_window() -> str:
    """
    Determine outreach send window based on event date.
    
    Returns:
        Send window: PRE_EVENT | DURING_EVENT | POST_EVENT
    """
    # Keep default aligned to the TechSparks 2025 assignment event date.
    event_date_str = os.getenv("EVENT_DATE", "2025-11-07")
    assignment_event_date = date(2025, 11, 7)
    
    try:
        event_date = date.fromisoformat(event_date_str)
    except ValueError:
        logger.warning(f"Invalid EVENT_DATE format: {event_date_str}, defaulting to POST_EVENT")
        return "POST_EVENT"
    
    today = date.today()
    # Assignment guardrail: after TechSparks 2025 date has passed, always use post-event window.
    if today > assignment_event_date:
        if event_date != assignment_event_date:
            logger.warning(
                "Overriding EVENT_DATE=%s to POST_EVENT because assignment event date (%s) is in the past.",
                event_date.isoformat(),
                assignment_event_date.isoformat(),
            )
        return "POST_EVENT"

    if event_date < today:
        logger.warning(
            f"EVENT_DATE ({event_date.isoformat()}) is in the past relative to today ({today.isoformat()}). "
            "Routing defaults to POST_EVENT."
        )
    delta = (event_date - today).days
    
    if delta > 7:
        return "PRE_EVENT"
    elif -2 <= delta <= 7:
        return "DURING_EVENT"
    else:
        return "POST_EVENT"


def assign_route(
    seniority: str,
    relevance_score: float,
    event_role: str,
    persona_archetype: str,
    industry: str,
    confidence_score: int = 0,
    evidence_score: int = 0,
    send_window: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Assign routing based on contact attributes.
    
    Args:
        seniority: Normalized seniority level.
        relevance_score: Persona relevance score (0-100).
        event_role: Event role (Judge, Speaker, etc.).
        persona_archetype: Persona archetype.
        industry: Industry classification.
        confidence_score: Enrichment confidence score.
        evidence_score: Deterministic evidence score (0-100).
        
    Returns:
        Dictionary with routing decisions.
    """
    result: Dict[str, Any] = {}
    min_route_confidence = int(os.getenv("MIN_ROUTE_CONFIDENCE", "2"))
    min_route_evidence = int(os.getenv("MIN_ROUTE_EVIDENCE_SCORE", "35"))
    if send_window is None:
        send_window = get_send_window()
    
    # Check 1: Low relevance or failed persona
    if relevance_score < 30 or persona_archetype in ("INSUFFICIENT_DATA", "ERROR", ""):
        result["final_route"] = "Not Relevant"
        result["outreach_sequence"] = "SKIP"
        result["outreach_priority"] = 0
        result["outreach_approved"] = "NO"
        result["route_reason"] = "Relevance score below threshold or persona generation failed"
        result["icp_match"] = False
        result["segment_cluster"] = "General Tech Leader"
        result["account_priority_score"] = 0
        result["outreach_score"] = 0
        return result

    # Confidence gate: avoid auto-queueing low-confidence enrichment in live runs.
    if (confidence_score < min_route_confidence or evidence_score < min_route_evidence) and relevance_score < 80:
        result["final_route"] = "SDR"
        result["outreach_sequence"] = "MANUAL_REVIEW_LOW_CONFIDENCE"
        result["outreach_priority"] = min(int(relevance_score), 100)
        result["outreach_approved"] = "NO"
        result["route_reason"] = (
            f"Low enrichment confidence/evidence (confidence={confidence_score}, evidence={evidence_score}) "
            "requires manual review before outreach."
        )
        result["icp_match"] = False
        result["segment_cluster"] = "Manual Review"
        seniority_weight = 2
        evidence_component = min(max(evidence_score, 0), 100) * 0.1
        result["account_priority_score"] = round((relevance_score * 0.4) + (confidence_score * 8) + seniority_weight + evidence_component)
        result["outreach_score"] = result["account_priority_score"]
        return result
    
    # Check 2: Judge/Mentor or Board-Level
    if event_role in ("Judge", "Mentor") or persona_archetype == "Board-Level Strategist":
        result["final_route"] = "Senior AE"
        result["outreach_sequence"] = f"VIP_SEQUENCE_{send_window}"
        result["outreach_priority"] = 95
        result["outreach_approved"] = "PENDING_REVIEW"
        result["route_reason"] = "Judge/Mentor role or Board-level archetype → always Senior AE"
        result["icp_match"] = True
        result["segment_cluster"] = "Board-Level Strategist"
        seniority_weight = 20
        evidence_component = min(max(evidence_score, 0), 100) * 0.1
        result["account_priority_score"] = round(
            (relevance_score * 0.4) + (confidence_score * 8) + seniority_weight + 10 + evidence_component
        )
        result["outreach_score"] = result["account_priority_score"]
        return result
    
    # Check 3: VC investor (R7 fix)
    if industry == "venture_capital":
        result["final_route"] = "Senior AE"
        result["outreach_sequence"] = "PARTNERSHIP_SEQUENCE"
        result["outreach_priority"] = min(int(relevance_score), 85)
        result["outreach_approved"] = "PENDING_REVIEW"
        result["route_reason"] = "VC investor → partnership outreach, not sales"
        result["icp_match"] = True
        result["segment_cluster"] = "VC Investor"
        seniority_weight = 20
        evidence_component = min(max(evidence_score, 0), 100) * 0.1
        result["account_priority_score"] = round(
            (relevance_score * 0.4) + (confidence_score * 8) + seniority_weight + 10 + evidence_component
        )
        result["outreach_score"] = result["account_priority_score"]
        return result
    
    # Seniority-based routing
    if seniority == "C-Suite / Founder":
        if relevance_score >= 70:
            final_route = "Senior AE"
        else:
            final_route = "AE"
        seniority_weight = 20
    elif seniority == "VP":
        if relevance_score >= 60:
            final_route = "AE"
        else:
            final_route = "SDR"
        seniority_weight = 12
    elif seniority == "Director / Head":
        final_route = "SDR"
        seniority_weight = 6
    elif seniority == "Senior IC / Manager":
        final_route = "SDR"
        seniority_weight = 2
    elif seniority == "Manager":
        final_route = "SDR"
        seniority_weight = 2
    elif seniority == "IC":
        final_route = "SDR"
        seniority_weight = 2
    elif seniority == "Junior / Intern":
        final_route = "Not Relevant"
        seniority_weight = 0
    elif seniority == "Unclassified → Manual Review":
        final_route = "SDR"
        seniority_weight = 2
    else:
        final_route = "SDR"
        seniority_weight = 2
    
    result["final_route"] = final_route
    result["route_reason"] = f"Seniority={seniority}, score={relevance_score}"
    
    # Set sequence based on send window
    if final_route == "Senior AE":
        result["outreach_sequence"] = f"VIP_SEQUENCE_{send_window}"
    elif final_route == "AE":
        result["outreach_sequence"] = f"STANDARD_ABM_{send_window}"
    elif final_route == "SDR":
        result["outreach_sequence"] = f"HIGH_VOLUME_{send_window}"
    else:
        result["outreach_sequence"] = "SKIP"
    
    result["outreach_priority"] = min(int(relevance_score), 100) if final_route != "Not Relevant" else 0
    result["outreach_approved"] = "PENDING_REVIEW" if final_route != "Not Relevant" else "NO"
    
    # ICP match
    result["icp_match"] = (
        seniority in ICP_CONFIG["seniority"] and
        industry in ICP_CONFIG["industries"] and
        relevance_score >= ICP_CONFIG["min_relevance"]
    )
    
    # Segment cluster
    cluster_key = (seniority, industry)
    if industry == "venture_capital":
        result["segment_cluster"] = "VC Investor"
    elif cluster_key in SEGMENT_CLUSTER_MAP:
        result["segment_cluster"] = SEGMENT_CLUSTER_MAP[cluster_key]
    else:
        result["segment_cluster"] = "General Tech Leader"
    
    # Account priority score
    icp_bonus = 10 if result["icp_match"] else 0
    evidence_component = min(max(evidence_score, 0), 100) * 0.1
    result["account_priority_score"] = round(
        (relevance_score * 0.4) + (confidence_score * 8) + seniority_weight + icp_bonus + evidence_component
    )
    result["outreach_score"] = result["account_priority_score"]
    
    return result


def run_routing(
    input_csv: str = "data/speakers_personas.csv",
    output_csv: str = "data/speakers_routed.csv",
    handoff_csv: str = "output/make_handoff.csv",
) -> pd.DataFrame:
    """
    Run routing pipeline on all personas.
    
    Args:
        input_csv: Path to personas CSV.
        output_csv: Path to output CSV.
        
    Returns:
        Routed DataFrame.
    """
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    
    # Deduplicate
    df = deduplicate(df)
    
    # Get send window
    send_window = get_send_window()
    print(f"Current send window: {send_window}")
    
    # Apply routing to non-duplicates
    routing_results = []
    for idx, row in df.iterrows():
        if row["is_duplicate"]:
            # Duplicates get minimal routing
            routing = {
                "final_route": "DUPLICATE",
                "outreach_sequence": "SKIP",
                "outreach_priority": 0,
                "outreach_approved": "NO",
                "icp_match": False,
                "segment_cluster": "DUPLICATE",
                "account_priority_score": 0,
                "outreach_score": 0,
                "route_reason": "Duplicate contact"
            }
        else:
            confidence = row.get("enrichment_confidence_score", 0)
            if pd.isna(confidence):
                confidence = 0
            evidence = row.get("evidence_score", 0)
            if pd.isna(evidence):
                evidence = 0
            routing = assign_route(
                seniority=row.get("seniority", ""),
                relevance_score=float(row.get("relevance_score", 0)),
                event_role=row.get("event_role", ""),
                persona_archetype=row.get("persona_archetype", ""),
                industry=row.get("industry", ""),
                confidence_score=int(confidence),
                evidence_score=int(evidence),
                send_window=send_window,
            )
        routing_results.append(routing)
    
    # Add routing columns
    routing_df = pd.DataFrame(routing_results)
    df = pd.concat([df.reset_index(drop=True), routing_df], axis=1)
    
    # Print routing distribution
    print("\n=== Routing Distribution ===")
    route_counts = df["final_route"].value_counts()
    for route, count in route_counts.items():
        print(f"  {route}: {count}")
    
    # Print ICP match count
    icp_count = int(df["icp_match"].sum()) if len(df) else 0
    icp_pct = (icp_count / len(df) * 100) if len(df) else 0.0
    print(f"\nICP matches: {icp_count} ({icp_pct:.1f}%)")
    
    # Write output
    df.to_csv(output_csv, encoding="utf-8-sig", index=False)
    print(f"Output written to: {output_csv}")

    # Runtime no-code handoff artifact for Make/Sheets scenarios.
    handoff_mask = (
        df["final_route"].isin(["Senior AE", "AE"])
        & ~df["is_duplicate"].astype(str).str.lower().isin({"true", "1", "yes"})
        & (df["outreach_approved"] == "PENDING_REVIEW")
    )
    handoff_cols = [
        "id",
        "name",
        "title",
        "company",
        "seniority",
        "industry",
        "relevance_score",
        "final_route",
        "outreach_sequence",
        "account_priority_score",
    ]
    handoff_cols = [col for col in handoff_cols if col in df.columns]
    handoff_path = Path(handoff_csv)
    handoff_path.parent.mkdir(parents=True, exist_ok=True)
    df.loc[handoff_mask, handoff_cols].to_csv(handoff_path, index=False, encoding="utf-8-sig")
    print(f"No-code handoff written to: {handoff_path}")
    
    return df


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    run_routing()
