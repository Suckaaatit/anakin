"""
performance_report.py -- Generate assignment KPI observations.

Creates output/performance_observations.json and output/performance_observations.md
from pipeline artifacts. If campaign tracking data exists, observed delivery and
acceptance rates are computed; otherwise proxy readiness metrics are reported.
"""

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

MISSING_VALUES = {
    "",
    "N/A",
    "NOT_AVAILABLE",
    "NOT_FOUND",
    "ERROR",
    "DRAFT_ERROR",
    "nan",
    "None",
    "null",
}
YC_INTRO_LINE = (
    "Once you find this relevant, I can introduce you to a YC-backed company "
    "that specialises in solving this."
)
STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "about",
    "your",
    "this",
    "that",
    "their",
    "event",
    "panel",
    "speaker",
    "session",
}


@dataclass
class ObservedMetric:
    mode: str
    sent: int
    positive: int
    rate_pct: Optional[float]
    notes: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _pct(num: int, den: int) -> Optional[float]:
    if den <= 0:
        return None
    return round((num / den) * 100.0, 2)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _missing(value: Any) -> bool:
    return _safe_text(value) in MISSING_VALUES


def _to_bool(value: Any) -> bool:
    text = _safe_text(value).lower()
    return text in {"1", "true", "yes", "y", "sent", "delivered", "accepted"}


def _normalize(value: Any) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", _safe_text(value).lower()).strip()


def _word_count(text: Any) -> int:
    return len(re.findall(r"[A-Za-z0-9']+", _safe_text(text)))


def _subject_valid(subject: Any) -> bool:
    text = _safe_text(subject)
    return bool(text) and "?" not in text and _word_count(text) <= 8


def _topic_personalized(topic: Any, corpus: str) -> bool:
    topic_norm = _normalize(topic)
    corpus_norm = _normalize(corpus)
    if not topic_norm or not corpus_norm:
        return False
    if topic_norm in corpus_norm:
        return True
    words = [w for w in topic_norm.split() if len(w) >= 4 and w not in STOPWORDS]
    if not words:
        words = [w for w in topic_norm.split() if len(w) >= 3]
    if not words:
        return False
    hits = sum(1 for w in words[:6] if w in corpus_norm)
    threshold = 2 if len(words) >= 2 else 1
    return hits >= threshold


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig")


def _combine_text(row: pd.Series) -> str:
    parts = [
        row.get("email_subject_a", ""),
        row.get("email_subject_b", ""),
        row.get("email_body_pre_event", ""),
        row.get("email_body_during_event", ""),
        row.get("email_body_post_event", ""),
        row.get("linkedin_note", ""),
    ]
    return " ".join(_safe_text(p) for p in parts)


def _observed_from_tracking(tracking_df: Optional[pd.DataFrame]) -> Tuple[ObservedMetric, ObservedMetric]:
    if tracking_df is None:
        return (
            ObservedMetric("no_sample", 0, 0, None, "No tracking CSV found."),
            ObservedMetric("no_sample", 0, 0, None, "No tracking CSV found."),
        )
    if tracking_df.empty:
        return (
            ObservedMetric("no_sample", 0, 0, None, "Tracking CSV has zero rows."),
            ObservedMetric("no_sample", 0, 0, None, "Tracking CSV has zero rows."),
        )

    cols = {c.lower(): c for c in tracking_df.columns}
    email_sent_col = cols.get("email_sent")
    email_delivered_col = cols.get("email_delivered")
    email_bounced_col = cols.get("email_bounced")
    linkedin_sent_col = cols.get("linkedin_sent")
    linkedin_accepted_col = cols.get("linkedin_accepted")

    email_sent = 0
    email_delivered = 0
    linkedin_sent = 0
    linkedin_accepted = 0

    for _, row in tracking_df.iterrows():
        if email_sent_col and _to_bool(row.get(email_sent_col)):
            email_sent += 1
            if email_delivered_col and _to_bool(row.get(email_delivered_col)):
                email_delivered += 1
            elif email_bounced_col and not _to_bool(row.get(email_bounced_col)):
                email_delivered += 1

        if linkedin_sent_col and _to_bool(row.get(linkedin_sent_col)):
            linkedin_sent += 1
            if linkedin_accepted_col and _to_bool(row.get(linkedin_accepted_col)):
                linkedin_accepted += 1

    if email_sent <= 0:
        email_metric = ObservedMetric(
            "no_sample",
            0,
            0,
            None,
            "Tracking CSV present but email_sent=0.",
        )
    else:
        email_metric = ObservedMetric(
            "observed",
            email_sent,
            email_delivered,
            _pct(email_delivered, email_sent),
            "Observed from campaign tracking CSV.",
        )

    if linkedin_sent <= 0:
        linkedin_metric = ObservedMetric(
            "no_sample",
            0,
            0,
            None,
            "Tracking CSV present but linkedin_sent=0.",
        )
    else:
        linkedin_metric = ObservedMetric(
            "observed",
            linkedin_sent,
            linkedin_accepted,
            _pct(linkedin_accepted, linkedin_sent),
            "Observed from campaign tracking CSV.",
        )

    return email_metric, linkedin_metric


def build_report(
    enriched_df: pd.DataFrame,
    personas_df: pd.DataFrame,
    routed_df: pd.DataFrame,
    outreach_df: pd.DataFrame,
    tracking_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    draft_mask = outreach_df.get("outreach_status", pd.Series([""] * len(outreach_df))).astype(str) == "DRAFT_GENERATED"
    drafts = outreach_df[draft_mask].copy()
    if drafts.empty:
        drafts = outreach_df.copy()

    draft_count = len(drafts)
    contact_count = len(enriched_df)

    name_mention = 0
    company_mention = 0
    session_mention = 0
    linkedin_under_300 = 0
    subject_rules = 0
    yc_line_pass = 0
    forbidden_hits = 0

    for _, row in drafts.iterrows():
        combined = _combine_text(row)
        combined_norm = _normalize(combined)
        first_name = _normalize(_safe_text(row.get("name")).split(" ")[0])
        company = _normalize(row.get("company"))
        session_topic = row.get("session_topic")

        if first_name and first_name in combined_norm:
            name_mention += 1
        if company and company in combined_norm:
            company_mention += 1
        if _topic_personalized(session_topic, combined):
            session_mention += 1

        note = _safe_text(row.get("linkedin_note"))
        if note and len(note) < 300:
            linkedin_under_300 += 1

        if _subject_valid(row.get("email_subject_a")) and _subject_valid(row.get("email_subject_b")):
            subject_rules += 1

        bodies = [
            _safe_text(row.get("email_body_pre_event")),
            _safe_text(row.get("email_body_during_event")),
            _safe_text(row.get("email_body_post_event")),
        ]
        if all(YC_INTRO_LINE in body for body in bodies if body):
            yc_line_pass += 1

        if "anakin" in combined.lower():
            forbidden_hits += 1

    subjects = drafts.get("email_subject_a", pd.Series(dtype=str)).astype(str).str.strip().str.lower()
    subject_unique_rate = _pct(subjects.nunique(dropna=True), len(subjects)) if len(subjects) else None

    personalization_nonempty = 0
    if "personalization_themes" in personas_df.columns and len(personas_df):
        personalization_nonempty = int(
            personas_df["personalization_themes"].fillna("").astype(str).str.strip().ne("").sum()
        )

    email_pattern_available = 0
    if "email_pattern" in enriched_df.columns and len(enriched_df):
        email_pattern_available = int(
            (~enriched_df["email_pattern"].fillna("").astype(str).isin(MISSING_VALUES)).sum()
        )

    observed_email, observed_linkedin = _observed_from_tracking(tracking_df)

    email_pattern_available_pct = _pct(email_pattern_available, contact_count)
    subject_rules_pass_pct = _pct(subject_rules, draft_count)
    yc_intro_line_pass_pct = _pct(yc_line_pass, draft_count)
    linkedin_under_300_pct = _pct(linkedin_under_300, draft_count)
    session_topic_mention_pct = _pct(session_mention, draft_count)
    persona_theme_nonempty_pct = _pct(personalization_nonempty, len(personas_df))

    email_deliverability_rate = observed_email.rate_pct if observed_email.mode == "observed" else None
    linkedin_acceptance_rate = observed_linkedin.rate_pct if observed_linkedin.mode == "observed" else None

    send_readiness_score_pct = round(
        ((email_pattern_available_pct or 0.0) * 0.2)
        + ((subject_rules_pass_pct or 0.0) * 0.2)
        + ((yc_intro_line_pass_pct or 0.0) * 0.2)
        + ((session_topic_mention_pct or 0.0) * 0.2)
        + ((persona_theme_nonempty_pct or 0.0) * 0.2),
        2,
    )
    forbidden_violation_pct = _pct(forbidden_hits, draft_count) or 0.0
    spam_risk_score_pct = round(
        min(
            100.0,
            max(
                0.0,
                ((100.0 - (subject_rules_pass_pct or 0.0)) * 0.45)
                + ((100.0 - (yc_intro_line_pass_pct or 0.0)) * 0.25)
                + ((100.0 - (session_topic_mention_pct or 0.0)) * 0.20)
                + (forbidden_violation_pct * 0.10),
            ),
        ),
        2,
    )

    customization_components = [
        _pct(name_mention, draft_count) or 0.0,
        _pct(company_mention, draft_count) or 0.0,
        _pct(session_mention, draft_count) or 0.0,
        subject_unique_rate or 0.0,
        _pct(personalization_nonempty, len(personas_df)) or 0.0,
    ]
    customization_depth = round(sum(customization_components) / len(customization_components), 2)

    route_distribution = {}
    if "final_route" in routed_df.columns and len(routed_df):
        route_distribution = {str(k): int(v) for k, v in routed_df["final_route"].value_counts().to_dict().items()}

    report = {
        "generated_at": _now_iso(),
        "sample_size": {
            "enriched_contacts": contact_count,
            "persona_rows": len(personas_df),
            "routed_rows": len(routed_df),
            "outreach_rows": len(outreach_df),
            "drafts_generated": draft_count,
        },
        "routing": {
            "distribution": route_distribution,
            "duplicates_flagged": int(route_distribution.get("DUPLICATE", 0)),
        },
        "email_deliverability": {
            "measurement_mode": observed_email.mode,
            "observed": {
                "sent": observed_email.sent,
                "delivered": observed_email.positive,
                "deliverability_rate_pct": observed_email.rate_pct,
                "notes": observed_email.notes,
            },
            "readiness_signals": {
                "email_pattern_available_pct": email_pattern_available_pct,
                "subject_rules_pass_pct": subject_rules_pass_pct,
                "yc_intro_line_pass_pct": yc_intro_line_pass_pct,
                "forbidden_term_violations": forbidden_hits,
            },
            "assignment_rate_pct": email_deliverability_rate,
        },
        "linkedin_acceptance": {
            "measurement_mode": observed_linkedin.mode,
            "observed": {
                "requests_sent": observed_linkedin.sent,
                "accepted": observed_linkedin.positive,
                "acceptance_rate_pct": observed_linkedin.rate_pct,
                "notes": observed_linkedin.notes,
            },
            "readiness_signals": {
                "notes_under_300_chars_pct": linkedin_under_300_pct,
                "topic_personalization_pct": session_topic_mention_pct,
                "forbidden_term_violations": forbidden_hits,
            },
            "assignment_rate_pct": linkedin_acceptance_rate,
        },
        "message_customization": {
            "measurement_mode": "draft_content_analysis",
            "name_mention_pct": _pct(name_mention, draft_count),
            "company_mention_pct": _pct(company_mention, draft_count),
            "session_topic_mention_pct": session_topic_mention_pct,
            "subject_unique_rate_pct": subject_unique_rate,
            "persona_theme_nonempty_pct": persona_theme_nonempty_pct,
            "customization_depth_score_pct": customization_depth,
        },
        "assignment_metrics": {
            "email_deliverability_rate_pct": email_deliverability_rate,
            "email_deliverability_measurement_mode": observed_email.mode,
            "linkedin_acceptance_rate_pct": linkedin_acceptance_rate,
            "linkedin_acceptance_measurement_mode": observed_linkedin.mode,
            "expected_acceptance_rate_pct": linkedin_acceptance_rate,
            "send_readiness_score_pct": send_readiness_score_pct,
            "spam_risk_score_pct": spam_risk_score_pct,
        },
        "notes": [
            "Observed deliverability and acceptance require campaign_tracking.csv with real send outcomes.",
            "Send outcomes are not reported until email_sent/linkedin_sent are logged in campaign_tracking.csv.",
        ],
    }
    return report


def render_markdown(report: Dict[str, Any]) -> str:
    email = report["email_deliverability"]
    linkedin = report["linkedin_acceptance"]
    custom = report["message_customization"]
    assignment = report.get("assignment_metrics", {})
    sample = report["sample_size"]

    lines = [
        "# Key Performance Observations",
        "",
        f"Generated at: {report['generated_at']}",
        "",
        "## Sample Size",
        "",
        f"- Enriched contacts: {sample['enriched_contacts']}",
        f"- Persona rows: {sample['persona_rows']}",
        f"- Routed rows: {sample['routed_rows']}",
        f"- Outreach rows: {sample['outreach_rows']}",
        f"- Drafts generated: {sample['drafts_generated']}",
        "",
        "## Email Deliverability",
        "",
        f"- Measurement mode: {email['measurement_mode']}",
        f"- Observed sent: {email['observed']['sent']}",
        f"- Observed delivered: {email['observed']['delivered']}",
        f"- Observed deliverability rate: {email['observed']['deliverability_rate_pct']}",
        f"- Readiness `email_pattern_available_pct`: {email['readiness_signals']['email_pattern_available_pct']}",
        f"- Readiness `subject_rules_pass_pct`: {email['readiness_signals']['subject_rules_pass_pct']}",
        f"- Readiness `yc_intro_line_pass_pct`: {email['readiness_signals']['yc_intro_line_pass_pct']}",
        f"- Forbidden term violations: {email['readiness_signals']['forbidden_term_violations']}",
        f"- Assignment deliverability rate: {email.get('assignment_rate_pct')}",
        "",
        "## LinkedIn Acceptance",
        "",
        f"- Measurement mode: {linkedin['measurement_mode']}",
        f"- Observed requests sent: {linkedin['observed']['requests_sent']}",
        f"- Observed accepted: {linkedin['observed']['accepted']}",
        f"- Observed acceptance rate: {linkedin['observed']['acceptance_rate_pct']}",
        f"- Readiness `notes_under_300_chars_pct`: {linkedin['readiness_signals']['notes_under_300_chars_pct']}",
        f"- Readiness `topic_personalization_pct`: {linkedin['readiness_signals']['topic_personalization_pct']}",
        f"- Forbidden term violations: {linkedin['readiness_signals']['forbidden_term_violations']}",
        f"- Assignment acceptance rate: {linkedin.get('assignment_rate_pct')}",
        "",
        "## Assignment Metrics",
        "",
        f"- Email deliverability rate (assignment): {assignment.get('email_deliverability_rate_pct')}",
        f"- Email measurement mode: {assignment.get('email_deliverability_measurement_mode')}",
        f"- LinkedIn acceptance rate (assignment): {assignment.get('linkedin_acceptance_rate_pct')}",
        f"- LinkedIn measurement mode: {assignment.get('linkedin_acceptance_measurement_mode')}",
        f"- Expected acceptance rate: {assignment.get('expected_acceptance_rate_pct')}",
        f"- Send readiness score: {assignment.get('send_readiness_score_pct')}",
        f"- Spam risk score: {assignment.get('spam_risk_score_pct')}",
        "",
        "## Message Customization",
        "",
        f"- Name mention rate: {custom['name_mention_pct']}",
        f"- Company mention rate: {custom['company_mention_pct']}",
        f"- Session-topic mention rate: {custom['session_topic_mention_pct']}",
        f"- Subject uniqueness rate: {custom['subject_unique_rate_pct']}",
        f"- Persona themes non-empty rate: {custom['persona_theme_nonempty_pct']}",
        f"- Customization depth score: {custom['customization_depth_score_pct']}",
        "",
        "## Notes",
        "",
    ]
    for note in report.get("notes", []):
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate KPI observations for assignment submission.")
    parser.add_argument("--enriched-csv", default="data/speakers_enriched.csv")
    parser.add_argument("--persona-csv", default="data/speakers_personas.csv")
    parser.add_argument("--routed-csv", default="data/speakers_routed.csv")
    parser.add_argument("--outreach-csv", default="output/outreach_drafts.csv")
    parser.add_argument("--tracking-csv", default="data/campaign_tracking.csv")
    parser.add_argument("--json-out", default="output/performance_observations.json")
    parser.add_argument("--md-out", default="output/performance_observations.md")
    args = parser.parse_args()

    enriched_df = _read_csv(Path(args.enriched_csv))
    personas_df = _read_csv(Path(args.persona_csv))
    routed_df = _read_csv(Path(args.routed_csv))
    outreach_df = _read_csv(Path(args.outreach_csv))

    tracking_path = Path(args.tracking_csv)
    tracking_df = _read_csv(tracking_path) if tracking_path.exists() else None

    report = build_report(enriched_df, personas_df, routed_df, outreach_df, tracking_df=tracking_df)

    json_out = Path(args.json_out)
    md_out = Path(args.md_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md_out.write_text(render_markdown(report), encoding="utf-8")

    print(f"Performance observations JSON written to: {json_out}")
    print(f"Performance observations Markdown written to: {md_out}")


if __name__ == "__main__":
    main()
