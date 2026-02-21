"""
web_app.py -- Browser API for TechSparks GTM Automation.

Provides:
- Pipeline run control (start/stop)
- Live status + logs
- Output summary + table previews
- Optional static hosting for built TypeScript UI
"""

from __future__ import annotations

import io
import json
import os
import sqlite3
import subprocess
import sys
import threading
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from flask import Flask, jsonify, request, send_file, send_from_directory

PROJECT_ROOT = Path(__file__).resolve().parents[1]
UI_DIST = PROJECT_ROOT / "ui" / "dist"
RUN_HISTORY_PATH = PROJECT_ROOT / "data" / "run_history.json"
APP_DB_PATH = PROJECT_ROOT / "data" / "app_state.db"
MAX_LOG_LINES = 4000
LOG_TAIL_LINES = 300
VALID_STAGES = ("enrich", "persona", "route", "outreach")
CONTROL_TOKEN = os.getenv("WEB_APP_TOKEN", "").strip()
HIDDEN_LOG_SNIPPETS = (
    "http request: post https://api.mistral.ai/v1/chat/completions",
    "http request: post http://127.0.0.1:11434/v1/chat/completions",
    "retrying request to /chat/completions",
    "persona llm call failed after retries",
    "outreach llm call failed after retries",
)

MISSING_LINKEDIN_VALUES = {
    "NOT_FOUND",
    "ERROR",
    "",
    "SKIP_INVALID_NAME",
    "NOT_ATTEMPTED_FAST_MODE",
    "NOT_AVAILABLE",
    "NOT_PUBLICLY_AVAILABLE",
}
MISSING_GENERIC_VALUES = {
    "",
    "NOT_FOUND",
    "ERROR",
    "NOT_AVAILABLE",
    "NOT_PUBLICLY_AVAILABLE",
    "DRAFT_ERROR",
    "nan",
    "none",
    "null",
}

YC_INTRO_LINE = (
    "Once you find this relevant, I can introduce you to a YC-backed company "
    "that specialises in solving this."
)

DATASET_PATHS: Dict[str, Path] = {
    "raw": PROJECT_ROOT / "data" / "speakers_raw.csv",
    "enriched": PROJECT_ROOT / "data" / "speakers_enriched.csv",
    "personas": PROJECT_ROOT / "data" / "speakers_personas.csv",
    "routed": PROJECT_ROOT / "data" / "speakers_routed.csv",
    "outreach": PROJECT_ROOT / "output" / "outreach_drafts.csv",
}
CAMPAIGN_TRACKING_PATH = PROJECT_ROOT / "data" / "campaign_tracking.csv"

STAGE_FILE_CONFIG: Dict[str, Dict[str, Any]] = {
    "enrich": {
        "dataset": "enriched",
        "status_col": "enrichment_status",
        "pass_values": {"ENRICHED"},
        "id_cols": ["id", "name", "company", "linkedin_url", "seniority", "industry", "enrichment_status"],
        "log_tokens": ("enrich", "linkedin", "industry", "enrichment"),
        "config": {
            "input": "data/speakers_raw.csv",
            "output": "data/speakers_enriched.csv",
            "rule": "Status ENRICHED means stage passed.",
        },
    },
    "persona": {
        "dataset": "personas",
        "status_col": "persona_status",
        "pass_values": {"GENERATED"},
        "id_cols": ["id", "name", "company", "persona_archetype", "relevance_score", "persona_status"],
        "log_tokens": ("persona", "token", "llm"),
        "config": {
            "input": "data/speakers_enriched.csv",
            "output": "data/speakers_personas.csv",
            "rule": "Status GENERATED means stage passed.",
        },
    },
    "route": {
        "dataset": "routed",
        "status_col": "final_route",
        "pass_values": {"AE", "SDR", "Senior AE", "PARTNERSHIP"},
        "id_cols": ["id", "name", "company", "final_route", "outreach_sequence", "route_reason"],
        "log_tokens": ("route", "dedup", "icp"),
        "config": {
            "input": "data/speakers_personas.csv",
            "output": "data/speakers_routed.csv",
            "rule": "Any non-empty non-DUPLICATE route is treated as pass.",
        },
    },
    "outreach": {
        "dataset": "outreach",
        "status_col": "outreach_status",
        "pass_values": {"DRAFT_GENERATED", "APPROVED", "SKIPPED", "REJECTED"},
        "id_cols": ["id", "name", "company", "final_route", "outreach_status", "email_subject_a"],
        "log_tokens": ("outreach", "draft", "linkedin note"),
        "config": {
            "input": "data/speakers_routed.csv",
            "output": "output/outreach_drafts.csv",
            "rule": "DRAFT_GENERATED, APPROVED, SKIPPED, or REJECTED means stage processed.",
        },
    },
}

RUN_HISTORY: List[Dict[str, Any]] = []


def now_utc_iso() -> str:
    """Return UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sanitize_log_line(line: str) -> Optional[str]:
    """Hide low-level transport noise from UI run logs."""
    clean_line = line.rstrip("\r\n")
    if not clean_line:
        return None
    low = clean_line.lower()
    if "warning" in low or "debug" in low:
        return None
    if any(snippet in low for snippet in HIDDEN_LOG_SNIPPETS):
        return None
    return clean_line


def _write_run_history() -> None:
    RUN_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUN_HISTORY_PATH.write_text(json.dumps(RUN_HISTORY[-50:], indent=2), encoding="utf-8")


def _load_run_history() -> None:
    global RUN_HISTORY
    if not RUN_HISTORY_PATH.exists():
        RUN_HISTORY = []
        return
    try:
        parsed = json.loads(RUN_HISTORY_PATH.read_text(encoding="utf-8"))
        if isinstance(parsed, list):
            RUN_HISTORY = [entry for entry in parsed if isinstance(entry, dict)][-50:]
        else:
            RUN_HISTORY = []
    except Exception:
        RUN_HISTORY = []


def _safe_numeric_series(df: Optional[pd.DataFrame], col: str) -> pd.Series:
    if df is None or col not in df.columns:
        return pd.Series([], dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _safe_pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _subject_valid(subject: Any) -> bool:
    text = str(subject or "").strip()
    if not text:
        return False
    if "?" in text:
        return False
    return len(text.split()) <= 8


def _to_bool_metric(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "sent", "delivered", "accepted"}


def _observed_campaign_rates() -> Dict[str, Any]:
    tracking_df = _safe_dataframe_read(CAMPAIGN_TRACKING_PATH)
    default_payload = {
        "email_measurement_mode": "none",
        "email_sent": 0,
        "email_delivered": 0,
        "email_deliverability_rate_pct": None,
        "linkedin_measurement_mode": "none",
        "linkedin_sent": 0,
        "linkedin_accepted": 0,
        "linkedin_acceptance_rate_pct": None,
    }
    if tracking_df is None or tracking_df.empty:
        return default_payload

    cols = {str(c).strip().lower(): c for c in tracking_df.columns}
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
        if email_sent_col and _to_bool_metric(row.get(email_sent_col)):
            email_sent += 1
            if email_delivered_col and _to_bool_metric(row.get(email_delivered_col)):
                email_delivered += 1
            elif email_bounced_col and not _to_bool_metric(row.get(email_bounced_col)):
                email_delivered += 1

        if linkedin_sent_col and _to_bool_metric(row.get(linkedin_sent_col)):
            linkedin_sent += 1
            if linkedin_accepted_col and _to_bool_metric(row.get(linkedin_accepted_col)):
                linkedin_accepted += 1

    payload = dict(default_payload)
    if email_sent > 0:
        payload["email_measurement_mode"] = "observed"
        payload["email_sent"] = email_sent
        payload["email_delivered"] = email_delivered
        payload["email_deliverability_rate_pct"] = _safe_pct(email_delivered, email_sent)

    if linkedin_sent > 0:
        payload["linkedin_measurement_mode"] = "observed"
        payload["linkedin_sent"] = linkedin_sent
        payload["linkedin_accepted"] = linkedin_accepted
        payload["linkedin_acceptance_rate_pct"] = _safe_pct(linkedin_accepted, linkedin_sent)

    return payload


def _seniority_weight(seniority: str) -> int:
    weights = {
        "C-Suite / Founder": 20,
        "VP": 12,
        "Director / Head": 6,
        "Senior IC / Manager": 2,
        "Manager": 2,
        "IC": 2,
        "Junior / Intern": 0,
    }
    return int(weights.get(str(seniority), 2))


def _compute_explainability(row: Dict[str, Any]) -> Dict[str, Any]:
    relevance_score = float(pd.to_numeric(row.get("relevance_score", 0), errors="coerce") or 0)
    confidence_score = float(pd.to_numeric(row.get("enrichment_confidence_score", 0), errors="coerce") or 0)
    evidence_score = float(pd.to_numeric(row.get("evidence_score", 0), errors="coerce") or 0)
    seniority = str(row.get("seniority", "")).strip()
    icp_match = str(row.get("icp_match", "")).strip().lower() in {"true", "1", "yes"}

    relevance_component = round(relevance_score * 0.4, 2)
    confidence_component = round(confidence_score * 8.0, 2)
    evidence_component = round(min(max(evidence_score, 0.0), 100.0) * 0.1, 2)
    seniority_component = float(_seniority_weight(seniority))
    icp_component = 10.0 if icp_match else 0.0

    computed_total = round(
        relevance_component + confidence_component + evidence_component + seniority_component + icp_component,
        2,
    )
    stored_total = pd.to_numeric(row.get("account_priority_score", row.get("outreach_score", computed_total)), errors="coerce")
    if pd.isna(stored_total):
        stored_total = computed_total

    return {
        "priority_score": float(round(float(stored_total), 2)),
        "breakdown": [
            {"label": f"Seniority: {seniority or 'Unknown'}", "value": seniority_component},
            {"label": f"Relevance Score: {int(relevance_score)}", "value": relevance_component},
            {"label": f"Confidence Score: {int(confidence_score)}", "value": confidence_component},
            {"label": f"Evidence Score: {int(evidence_score)}", "value": evidence_component},
            {"label": f"ICP Match: {'Yes' if icp_match else 'No'}", "value": icp_component},
        ],
        "segment_cluster": str(row.get("segment_cluster", "")),
        "final_route": str(row.get("final_route", "")),
        "sequence": str(row.get("outreach_sequence", "")),
        "route_reason": str(row.get("route_reason", "")),
    }


def _current_quality_snapshot() -> Dict[str, Any]:
    enriched_df = _safe_dataframe_read(DATASET_PATHS["enriched"])
    personas_df = _safe_dataframe_read(DATASET_PATHS["personas"])
    routed_df = _safe_dataframe_read(DATASET_PATHS["routed"])
    outreach_df = _safe_dataframe_read(DATASET_PATHS["outreach"])

    enriched_total = len(enriched_df) if enriched_df is not None else 0
    enriched_ok = (
        int((enriched_df["enrichment_status"] == "ENRICHED").sum())
        if enriched_df is not None and "enrichment_status" in enriched_df.columns
        else 0
    )
    confidence_avg_score = 0.0
    low_confidence_pct = 0.0
    if enriched_df is not None and "enrichment_confidence_score" in enriched_df.columns and len(enriched_df) > 0:
        confidence_series = _safe_numeric_series(enriched_df, "enrichment_confidence_score").clip(0, 5)
        confidence_avg_score = round(float(confidence_series.mean()), 2) if len(confidence_series) else 0.0
        low_confidence_pct = _safe_pct(int((confidence_series < 2).sum()), len(confidence_series))

    linkedin_missing_pct = 0.0
    linkedin_not_attempted_pct = 0.0
    if enriched_df is not None and "linkedin_url" in enriched_df.columns and len(enriched_df) > 0:
        missing_mask = enriched_df["linkedin_url"].astype(str).str.strip().isin(MISSING_LINKEDIN_VALUES)
        linkedin_missing_pct = round(float(missing_mask.mean() * 100), 2)
        not_attempted_mask = enriched_df["linkedin_url"].astype(str).eq("NOT_ATTEMPTED_FAST_MODE")
        linkedin_not_attempted_pct = round(float(not_attempted_mask.mean() * 100), 2)
    linkedin_match_coverage_pct = round(max(0.0, 100.0 - linkedin_missing_pct), 2)
    linkedin_lookup_coverage_pct = round(max(0.0, 100.0 - linkedin_not_attempted_pct), 2)

    icp_rate = 0.0
    if routed_df is not None and "icp_match" in routed_df.columns and len(routed_df) > 0:
        icp_rate = round(float(routed_df["icp_match"].astype(str).str.lower().isin({"true", "1", "yes"}).mean() * 100), 2)

    outreach_total = len(outreach_df) if outreach_df is not None else 0
    generated_like_status = {"DRAFT_GENERATED", "APPROVED", "REJECTED"}
    drafts_generated = (
        int(outreach_df["outreach_status"].astype(str).isin(generated_like_status).sum())
        if outreach_df is not None and "outreach_status" in outreach_df.columns
        else 0
    )
    outreach_generation_rate = round((drafts_generated / outreach_total * 100), 2) if outreach_total else 0.0

    email_pattern_available_pct = 0.0
    if enriched_df is not None and "email_pattern" in enriched_df.columns and len(enriched_df) > 0:
        patterns = enriched_df["email_pattern"].fillna("").astype(str).str.strip().str.lower()
        email_pattern_available_pct = _safe_pct(int((~patterns.isin(MISSING_GENERIC_VALUES)).sum()), len(enriched_df))

    subject_rules_pass_pct = 0.0
    linkedin_note_under_300_pct = 0.0
    note_contextual_pct = 0.0
    yc_intro_line_pass_pct = 0.0
    forbidden_term_violation_pct = 0.0
    forbidden_term_violations = 0
    if outreach_df is not None and len(outreach_df) > 0:
        draft_like_df = outreach_df.copy()
        if "outreach_status" in draft_like_df.columns:
            filtered = draft_like_df[draft_like_df["outreach_status"].astype(str).isin(generated_like_status)]
            if not filtered.empty:
                draft_like_df = filtered

        draft_len = len(draft_like_df)
        if draft_len > 0:
            if {"email_subject_a", "email_subject_b"}.issubset(draft_like_df.columns):
                valid_subjects = sum(
                    1
                    for _, row in draft_like_df.iterrows()
                    if _subject_valid(row.get("email_subject_a")) and _subject_valid(row.get("email_subject_b"))
                )
                subject_rules_pass_pct = _safe_pct(valid_subjects, draft_len)

            if "linkedin_note" in draft_like_df.columns:
                notes = draft_like_df["linkedin_note"].fillna("").astype(str).str.strip()
                under_300 = int(((notes.str.len() > 0) & (notes.str.len() < 300)).sum())
                linkedin_note_under_300_pct = _safe_pct(under_300, draft_len)

                contextual_hits = 0
                for _, row in draft_like_df.iterrows():
                    note_low = str(row.get("linkedin_note", "") or "").lower()
                    name_first = str(row.get("name", "") or "").split(" ")[0].strip().lower()
                    company = str(row.get("company", "") or "").strip().lower()
                    if (name_first and name_first in note_low) or (company and company in note_low):
                        contextual_hits += 1
                note_contextual_pct = _safe_pct(contextual_hits, draft_len)

            yc_hits = 0
            forbidden_hits = 0
            for _, row in draft_like_df.iterrows():
                body_candidates = [
                    str(row.get("email_body_pre_event", "") or "").strip(),
                    str(row.get("email_body_during_event", "") or "").strip(),
                    str(row.get("email_body_post_event", "") or "").strip(),
                ]
                nonempty_bodies = [body for body in body_candidates if body]
                if nonempty_bodies and all(YC_INTRO_LINE.lower() in body.lower() for body in nonempty_bodies):
                    yc_hits += 1

                combined = " ".join(
                    [
                        str(row.get("email_subject_a", "") or ""),
                        str(row.get("email_subject_b", "") or ""),
                        str(row.get("email_body_pre_event", "") or ""),
                        str(row.get("email_body_during_event", "") or ""),
                        str(row.get("email_body_post_event", "") or ""),
                        str(row.get("linkedin_note", "") or ""),
                    ]
                ).lower()
                if "anakin" in combined:
                    forbidden_hits += 1

            yc_intro_line_pass_pct = _safe_pct(yc_hits, draft_len)
            forbidden_term_violation_pct = _safe_pct(forbidden_hits, draft_len)
            forbidden_term_violations = forbidden_hits

    persona_theme_nonempty_pct = 0.0
    if personas_df is not None and "personalization_themes" in personas_df.columns and len(personas_df) > 0:
        nonempty = int(personas_df["personalization_themes"].fillna("").astype(str).str.strip().ne("").sum())
        persona_theme_nonempty_pct = _safe_pct(nonempty, len(personas_df))

    email_deliverability_proxy_pct = round(
        (email_pattern_available_pct * 0.5) + (subject_rules_pass_pct * 0.5),
        2,
    )
    linkedin_acceptance_proxy_pct = round(
        (linkedin_note_under_300_pct * 0.6) + (note_contextual_pct * 0.4),
        2,
    )
    send_readiness_score_pct = round(
        (email_pattern_available_pct * 0.2)
        + (subject_rules_pass_pct * 0.2)
        + (yc_intro_line_pass_pct * 0.2)
        + (note_contextual_pct * 0.2)
        + (persona_theme_nonempty_pct * 0.2),
        2,
    )
    spam_risk_score_pct = round(
        min(
            100.0,
            max(
                0.0,
                ((100.0 - subject_rules_pass_pct) * 0.45)
                + ((100.0 - yc_intro_line_pass_pct) * 0.25)
                + ((100.0 - note_contextual_pct) * 0.20)
                + (forbidden_term_violation_pct * 0.10),
            ),
        ),
        2,
    )

    observed_rates = _observed_campaign_rates()
    if observed_rates["email_measurement_mode"] == "observed":
        email_deliverability_rate_pct: Optional[float] = float(observed_rates["email_deliverability_rate_pct"] or 0.0)
    else:
        email_deliverability_rate_pct = None

    if observed_rates["linkedin_measurement_mode"] == "observed":
        linkedin_acceptance_rate_pct: Optional[float] = float(observed_rates["linkedin_acceptance_rate_pct"] or 0.0)
    else:
        linkedin_acceptance_rate_pct = None

    return {
        "timestamp": now_utc_iso(),
        "enrichment_success_rate": round((enriched_ok / enriched_total * 100), 2) if enriched_total else 0.0,
        "linkedin_missing_pct": linkedin_missing_pct,
        "linkedin_not_attempted_pct": linkedin_not_attempted_pct,
        "linkedin_match_coverage_pct": linkedin_match_coverage_pct,
        "linkedin_lookup_coverage_pct": linkedin_lookup_coverage_pct,
        "icp_match_rate": icp_rate,
        "outreach_generation_rate": outreach_generation_rate,
        "confidence_avg_score": confidence_avg_score,
        "low_confidence_pct": low_confidence_pct,
        "email_deliverability_proxy_pct": email_deliverability_proxy_pct,
        "linkedin_acceptance_proxy_pct": linkedin_acceptance_proxy_pct,
        "email_deliverability_rate_pct": email_deliverability_rate_pct,
        "email_deliverability_measurement_mode": observed_rates["email_measurement_mode"],
        "linkedin_acceptance_rate_pct": linkedin_acceptance_rate_pct,
        "linkedin_acceptance_measurement_mode": observed_rates["linkedin_measurement_mode"],
        "expected_linkedin_acceptance_rate_pct": linkedin_acceptance_rate_pct,
        "send_readiness_score_pct": send_readiness_score_pct,
        "spam_risk_score_pct": spam_risk_score_pct,
        "subject_rules_pass_pct": subject_rules_pass_pct,
        "yc_intro_line_pass_pct": yc_intro_line_pass_pct,
        "forbidden_term_violation_pct": forbidden_term_violation_pct,
        "forbidden_term_violations": forbidden_term_violations,
        "observed_email_sent": int(observed_rates["email_sent"]),
        "observed_linkedin_sent": int(observed_rates["linkedin_sent"]),
        "persona_theme_nonempty_pct": persona_theme_nonempty_pct,
    }


def _append_run_history_snapshot() -> None:
    snapshot = _current_quality_snapshot()
    RUN_HISTORY.append(snapshot)
    del RUN_HISTORY[:-50]
    _write_run_history()


@dataclass
class PipelineRunState:
    """State holder for the currently running (or last) pipeline job."""

    status: str = "idle"
    mode: str = "full"
    from_stage: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    return_code: Optional[int] = None
    command: Optional[List[str]] = None
    logs: List[str] = field(default_factory=list)
    process: Optional[subprocess.Popen] = None

    def append_log(self, line: str) -> None:
        clean_line = _sanitize_log_line(line)
        if clean_line is None:
            return
        self.logs.append(clean_line)
        if len(self.logs) > MAX_LOG_LINES:
            overflow = len(self.logs) - MAX_LOG_LINES
            del self.logs[:overflow]

    def snapshot(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "from_stage": self.from_stage,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "return_code": self.return_code,
            "command": self.command or [],
            "can_start": self.status != "running",
            "log_count": len(self.logs),
            "logs": self.logs[-LOG_TAIL_LINES:],
        }


STATE_LOCK = threading.Lock()
RUN_STATE = PipelineRunState()
APPROVAL_WRITE_LOCK = threading.Lock()


def _is_control_authorized() -> bool:
    """Authorize write/control endpoints using optional API token."""
    if not CONTROL_TOKEN:
        remote = str(request.remote_addr or "").strip()
        return remote in {"127.0.0.1", "::1", "localhost"}
    provided = request.headers.get("X-API-Token", "").strip()
    if not provided:
        provided = str(request.args.get("token", "")).strip()
    return bool(provided) and provided == CONTROL_TOKEN


def _atomic_csv_write(df: pd.DataFrame, path: Path) -> None:
    """Write CSV via temp file and atomic replace to reduce partial-write risk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8-sig",
        newline="",
        delete=False,
        dir=str(path.parent),
        suffix=".tmp",
    ) as tmp:
        temp_name = tmp.name
        df.to_csv(tmp, index=False)
    retries = 6
    for attempt in range(retries):
        try:
            os.replace(temp_name, path)
            return
        except PermissionError:
            if attempt >= retries - 1:
                raise
            # Windows can transiently lock CSV targets when concurrent reads occur.
            time.sleep(0.12 * (attempt + 1))


def _init_app_db() -> None:
    APP_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(APP_DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS approval_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contact_id TEXT NOT NULL,
                decision TEXT NOT NULL,
                decided_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _record_approval_audit(contact_id: str, decision: str) -> None:
    conn = sqlite3.connect(APP_DB_PATH)
    try:
        conn.execute(
            "INSERT INTO approval_audit (contact_id, decision, decided_at) VALUES (?, ?, ?)",
            (str(contact_id), str(decision).upper(), now_utc_iso()),
        )
        conn.commit()
    finally:
        conn.close()


def _start_process(test_mode: bool, from_stage: Optional[str], fast_mode: Optional[bool] = None) -> Tuple[bool, str]:
    """
    Start pipeline as a subprocess and attach log streaming thread.

    Returns:
        (success, message)
    """
    with STATE_LOCK:
        if RUN_STATE.status == "running":
            return False, "Pipeline is already running."

        RUN_STATE.status = "running"
        mode_label = "test" if test_mode else "full"
        if fast_mode is True:
            mode_label += "-cached"
        elif fast_mode is False:
            mode_label += "-live"
        RUN_STATE.mode = mode_label
        RUN_STATE.from_stage = from_stage
        RUN_STATE.started_at = now_utc_iso()
        RUN_STATE.finished_at = None
        RUN_STATE.return_code = None
        RUN_STATE.logs = []
        RUN_STATE.process = None

    command = [sys.executable, str(PROJECT_ROOT / "src" / "pipeline.py")]
    display_command = ["python", "src/pipeline.py"]
    if test_mode:
        command.append("--test")
        display_command.append("--test")
    if from_stage:
        command.extend(["--from", from_stage])
        display_command.extend(["--from", from_stage])
    if fast_mode is True:
        command.append("--fast")
        display_command.append("--fast")
    elif fast_mode is False:
        command.append("--live")
        display_command.append("--live")

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env.setdefault("FAST_MODE", "1")
    env.setdefault("DISABLE_INDUSTRY_LLM", "1")
    env.setdefault("PERSONA_MAX_WORKERS", "1")
    env.setdefault("PERSONA_RETRY_ATTEMPTS", "0")
    env.setdefault("PERSONA_FAIL_FAST_ON_429", "1")
    env.setdefault("PERSONA_DISABLE_LLM_ON_RATE_LIMIT", "1")
    env.setdefault("PERSONA_RATE_LIMIT_SWITCH_THRESHOLD", "1")
    env.setdefault("PERSONA_ROW_DELAY_SEC", "0")
    env.setdefault("OUTREACH_MAX_WORKERS", "1")
    env.setdefault("OUTREACH_RETRY_ATTEMPTS", "0")
    env.setdefault("OUTREACH_FAIL_FAST_ON_429", "1")
    env.setdefault("OUTREACH_DISABLE_LLM_ON_RATE_LIMIT", "1")
    env.setdefault("OUTREACH_RATE_LIMIT_SWITCH_THRESHOLD", "1")
    env.setdefault("OUTREACH_ROW_DELAY_SEC", "0")

    try:
        process = subprocess.Popen(
            command,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
    except Exception as exc:
        with STATE_LOCK:
            RUN_STATE.status = "failed"
            RUN_STATE.finished_at = now_utc_iso()
            RUN_STATE.return_code = -1
            RUN_STATE.command = display_command
            RUN_STATE.append_log(f"Failed to start pipeline: {exc}")
        return False, "Failed to start pipeline process."

    with STATE_LOCK:
        RUN_STATE.process = process
        RUN_STATE.command = display_command
        RUN_STATE.append_log(
            f"Started at {RUN_STATE.started_at} UTC | Mode={RUN_STATE.mode} | From={from_stage or 'start'}"
        )
        RUN_STATE.append_log("Command: " + " ".join(display_command))

    def _stream_output() -> None:
        if process.stdout is None:
            return

        for line in process.stdout:
            with STATE_LOCK:
                RUN_STATE.append_log(line)

        return_code = process.wait()
        with STATE_LOCK:
            RUN_STATE.process = None
            RUN_STATE.return_code = return_code
            RUN_STATE.finished_at = now_utc_iso()
            RUN_STATE.status = "completed" if return_code == 0 else "failed"
            RUN_STATE.append_log(f"Process exited with code {return_code}")
        if return_code == 0:
            _append_run_history_snapshot()

    threading.Thread(target=_stream_output, daemon=True).start()
    return True, "Pipeline started."


def _stop_process() -> Tuple[bool, str]:
    """Terminate currently running pipeline subprocess."""
    with STATE_LOCK:
        process = RUN_STATE.process
        if process is None or RUN_STATE.status != "running":
            return False, "No running pipeline process."
        RUN_STATE.append_log("Stop requested by user.")

    try:
        process.terminate()
    except Exception as exc:
        return False, f"Failed to stop process: {exc}"
    return True, "Stop signal sent."


def _safe_dataframe_read(path: Path) -> Optional[pd.DataFrame]:
    """Read a CSV using project defaults, returning None on failure."""
    if not path.exists():
        return None
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return None


def _file_summary(path: Path) -> Dict[str, Any]:
    """Return metadata for a pipeline output file."""
    if not path.exists():
        return {
            "exists": False,
            "rows": 0,
            "columns": 0,
            "updated_at": None,
        }

    df = _safe_dataframe_read(path)
    if df is None:
        return {
            "exists": True,
            "rows": 0,
            "columns": 0,
            "updated_at": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(
                timespec="seconds"
            ),
            "read_error": True,
        }

    return {
        "exists": True,
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "updated_at": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(
            timespec="seconds"
        ),
        "read_error": False,
    }


def _counts(df: Optional[pd.DataFrame], column: str) -> Dict[str, int]:
    """Count values for a column if available."""
    if df is None or column not in df.columns:
        return {}
    values = df[column].fillna("EMPTY").astype(str).value_counts().to_dict()
    return {str(key): int(value) for key, value in values.items()}


def _contains_term(paths: List[Path], term: str) -> bool:
    """Check whether any file contains a term (case-insensitive)."""
    term_lower = term.lower()
    for path in paths:
        if not path.exists():
            continue
        try:
            if term_lower in path.read_text(encoding="utf-8", errors="ignore").lower():
                return True
        except Exception:
            continue
    return False


def _build_summary() -> Dict[str, Any]:
    """Build dataset summary payload for UI cards/charts."""
    files = {name: _file_summary(path) for name, path in DATASET_PATHS.items()}

    enriched_df = _safe_dataframe_read(DATASET_PATHS["enriched"])
    personas_df = _safe_dataframe_read(DATASET_PATHS["personas"])
    routed_df = _safe_dataframe_read(DATASET_PATHS["routed"])
    outreach_df = _safe_dataframe_read(DATASET_PATHS["outreach"])

    warnings: List[str] = []
    quality_snapshot = _current_quality_snapshot()

    if enriched_df is not None and "linkedin_url" in enriched_df.columns:
        linkedin_found = (
            ~enriched_df["linkedin_url"].astype(str).str.strip().isin(MISSING_LINKEDIN_VALUES)
        ).sum()
        if int(linkedin_found) == 0:
            warnings.append("No LinkedIn profiles were found in the latest enrichment run.")

    if personas_df is not None and "persona_status" in personas_df.columns:
        skipped = int((personas_df["persona_status"] == "SKIPPED_LOW_CONFIDENCE").sum())
        if skipped > 0:
            warnings.append(f"{skipped} contacts were skipped in persona generation due to low confidence.")

    if outreach_df is not None and "outreach_status" in outreach_df.columns:
        generated = int((outreach_df["outreach_status"] == "DRAFT_GENERATED").sum())
        if generated == 0:
            warnings.append("No outreach drafts were generated in the latest run.")

    email_mode = str(quality_snapshot.get("email_deliverability_measurement_mode", "none")).strip().lower()
    linkedin_mode = str(quality_snapshot.get("linkedin_acceptance_measurement_mode", "none")).strip().lower()
    if email_mode != "observed" or linkedin_mode != "observed":
        warnings.append(
            "Email deliverability and LinkedIn acceptance require logged sample outcomes in data/campaign_tracking.csv."
        )

    compliance = {
        "anakin_found": _contains_term(
            [DATASET_PATHS["enriched"], DATASET_PATHS["personas"], DATASET_PATHS["routed"], DATASET_PATHS["outreach"]],
            "anakin",
        ),
        "all_outputs_exist": all(meta["exists"] for name, meta in files.items() if name != "raw"),
    }

    return {
        "files": files,
        "distributions": {
            "persona_status": _counts(personas_df, "persona_status"),
            "final_route": _counts(routed_df, "final_route"),
            "outreach_status": _counts(outreach_df, "outreach_status"),
        },
        "warnings": warnings,
        "compliance": compliance,
    }


def _preview_dataset(dataset: str, limit: int) -> Tuple[int, Dict[str, Any]]:
    """Return preview rows for a selected dataset."""
    if dataset not in DATASET_PATHS:
        return 404, {"error": f"Unknown dataset '{dataset}'."}

    path = DATASET_PATHS[dataset]
    df = _safe_dataframe_read(path)
    if df is None:
        return 404, {"error": f"Dataset file not found or unreadable: {path}"}

    limit = max(1, min(limit, 100))
    preview_df = df.head(limit).where(pd.notnull(df), None)

    return 200, {
        "dataset": dataset,
        "file": str(path.relative_to(PROJECT_ROOT)),
        "rows_total": int(len(df)),
        "rows_returned": int(len(preview_df)),
        "columns": [str(col) for col in preview_df.columns.tolist()],
        "data": preview_df.to_dict(orient="records"),
    }


def _stage_status_mask(df: pd.DataFrame, stage: str) -> Tuple[pd.Series, pd.Series]:
    cfg = STAGE_FILE_CONFIG[stage]
    col = cfg["status_col"]

    if col not in df.columns:
        fail = pd.Series([True] * len(df), index=df.index)
        passed = ~fail
        return passed, fail

    values = df[col].fillna("").astype(str).str.strip()
    if stage == "route":
        passed = values.ne("") & ~values.isin(["DUPLICATE"])
        failed = ~passed
        return passed, failed

    pass_values = cfg["pass_values"]
    passed = values.isin(pass_values)
    failed = ~passed
    return passed, failed


def _trim_records(df: pd.DataFrame, columns: List[str], limit: int) -> List[Dict[str, Any]]:
    existing_cols = [col for col in columns if col in df.columns]
    if not existing_cols:
        existing_cols = [str(col) for col in df.columns[:8]]
    view = df.head(limit)[existing_cols].where(pd.notnull(df), None)
    return view.to_dict(orient="records")


def _stage_logs(stage: str, limit: int = 200) -> List[str]:
    cfg = STAGE_FILE_CONFIG[stage]
    tokens = tuple(token.lower() for token in cfg.get("log_tokens", (stage,)))

    with STATE_LOCK:
        run_logs = list(RUN_STATE.logs)

    scoped: List[str] = []
    current_stage: Optional[str] = None
    for line in run_logs:
        low = line.lower()
        for s in VALID_STAGES:
            marker = f"running {s}"
            if marker in low:
                current_stage = s
                break
        if current_stage == stage:
            scoped.append(line)

    if not scoped:
        scoped = [line for line in run_logs if any(token in line.lower() for token in tokens)]

    if not scoped:
        errors_path = PROJECT_ROOT / "data" / "errors.log"
        if errors_path.exists():
            try:
                lines = errors_path.read_text(encoding="utf-8", errors="ignore").splitlines()
                scoped = []
                for line in lines:
                    if not any(token in line.lower() for token in tokens):
                        continue
                    clean = _sanitize_log_line(line)
                    if clean is not None:
                        scoped.append(clean)
            except Exception:
                scoped = []

    return scoped[-limit:]


def _stage_details(stage: str, limit: int = 20) -> Tuple[int, Dict[str, Any]]:
    if stage not in STAGE_FILE_CONFIG:
        return 404, {"error": f"Unknown stage '{stage}'."}

    cfg = STAGE_FILE_CONFIG[stage]
    dataset = cfg["dataset"]
    path = DATASET_PATHS[dataset]
    df = _safe_dataframe_read(path)
    if df is None:
        return 404, {"error": f"Stage dataset file unavailable: {path}"}

    passed_mask, failed_mask = _stage_status_mask(df, stage)
    passed_df = df[passed_mask].copy()
    failed_df = df[failed_mask].copy()

    payload = {
        "stage": stage,
        "dataset": dataset,
        "file": str(path.relative_to(PROJECT_ROOT)),
        "counts": {
            "total": int(len(df)),
            "passed": int(len(passed_df)),
            "failed": int(len(failed_df)),
        },
        "samples": {
            "passed": _trim_records(passed_df, cfg["id_cols"], limit),
            "failed": _trim_records(failed_df, cfg["id_cols"], limit),
        },
        "logs": _stage_logs(stage, limit=300),
        "config": cfg["config"],
    }
    return 200, payload


def _quality_payload() -> Dict[str, Any]:
    enriched_df = _safe_dataframe_read(DATASET_PATHS["enriched"])
    routed_df = _safe_dataframe_read(DATASET_PATHS["routed"])
    outreach_df = _safe_dataframe_read(DATASET_PATHS["outreach"])

    route_distribution = _counts(routed_df, "final_route")
    seniority_distribution = _counts(enriched_df, "seniority")

    confidence_histogram = {str(i): 0 for i in range(0, 6)}
    if enriched_df is not None and "enrichment_confidence_score" in enriched_df.columns:
        confidence_series = _safe_numeric_series(enriched_df, "enrichment_confidence_score").clip(0, 5).round().astype(int)
        confidence_histogram = {str(i): int((confidence_series == i).sum()) for i in range(0, 6)}

    history = RUN_HISTORY[-20:]
    if not history:
        history = [_current_quality_snapshot()]

    return {
        "route_distribution": route_distribution,
        "seniority_distribution": seniority_distribution,
        "confidence_histogram": confidence_histogram,
        "history": history,
        "kpis": _current_quality_snapshot(),
    }


def _queue_payload(limit: int = 100, status: str = "pending") -> Tuple[int, Dict[str, Any]]:
    outreach_df = _safe_dataframe_read(DATASET_PATHS["outreach"])
    if outreach_df is None:
        return 404, {"error": "Outreach dataset unavailable."}

    work_df = outreach_df.copy()
    if "approval_decision" not in work_df.columns:
        work_df["approval_decision"] = ""

    pending_mask = work_df["outreach_approved"].astype(str).str.upper().eq("PENDING_REVIEW")
    approved_mask = work_df["outreach_approved"].astype(str).str.upper().eq("YES")
    rejected_mask = work_df["approval_decision"].astype(str).str.upper().eq("REJECTED")
    non_queueable_mask = ~(pending_mask | approved_mask | rejected_mask)

    status_lower = status.strip().lower()
    if status_lower == "pending":
        mask = pending_mask
    elif status_lower == "approved":
        mask = approved_mask
    elif status_lower == "rejected":
        mask = rejected_mask
    elif status_lower == "non_queueable":
        mask = non_queueable_mask
    else:
        mask = pd.Series([True] * len(work_df), index=work_df.index)

    filtered = work_df[mask].copy()
    cols = [
        "id",
        "name",
        "company",
        "persona_archetype",
        "relevance_score",
        "enrichment_confidence_score",
        "evidence_score",
        "final_route",
        "outreach_sequence",
        "outreach_approved",
        "approval_decision",
        "outreach_status",
    ]
    preview_cols = [col for col in cols if col in filtered.columns]
    rows = filtered.head(limit)[preview_cols].where(pd.notnull(filtered), None).to_dict(orient="records")

    counts = {
        "total": int(len(work_df)),
        "pending": int(pending_mask.sum()),
        "approved": int(approved_mask.sum()),
        "rejected": int(rejected_mask.sum()),
        "non_queueable": int(non_queueable_mask.sum()),
    }
    return 200, {"status": status_lower, "counts": counts, "rows": rows}


def _update_approval(contact_id: str, decision: str) -> Tuple[int, Dict[str, Any]]:
    decision_norm = decision.strip().lower()
    if decision_norm not in {"approve", "reject"}:
        return 400, {"error": "Decision must be 'approve' or 'reject'."}

    with APPROVAL_WRITE_LOCK:
        outreach_path = DATASET_PATHS["outreach"]
        routed_path = DATASET_PATHS["routed"]
        outreach_df = _safe_dataframe_read(outreach_path)
        routed_df = _safe_dataframe_read(routed_path)
        if outreach_df is None or routed_df is None:
            return 404, {"error": "Required datasets unavailable."}

        if "id" not in outreach_df.columns or "id" not in routed_df.columns:
            return 400, {"error": "Missing id column in routed/outreach datasets."}

        if "approval_decision" not in outreach_df.columns:
            outreach_df["approval_decision"] = ""

        target = str(contact_id).strip()
        out_mask = outreach_df["id"].astype(str).eq(target)
        route_mask = routed_df["id"].astype(str).eq(target)
        if int(out_mask.sum()) == 0:
            return 404, {"error": f"Contact id {contact_id} not found in outreach dataset."}

        if decision_norm == "approve":
            outreach_df.loc[out_mask, "outreach_approved"] = "YES"
            outreach_df.loc[out_mask, "approval_decision"] = "APPROVED"
            outreach_df.loc[out_mask, "outreach_status"] = "APPROVED"
            routed_df.loc[route_mask, "outreach_approved"] = "YES"
        else:
            outreach_df.loc[out_mask, "outreach_approved"] = "NO"
            outreach_df.loc[out_mask, "approval_decision"] = "REJECTED"
            outreach_df.loc[out_mask, "outreach_status"] = "REJECTED"
            routed_df.loc[route_mask, "outreach_approved"] = "NO"

        _atomic_csv_write(outreach_df, outreach_path)
        _atomic_csv_write(routed_df, routed_path)
        _record_approval_audit(target, decision_norm)

    msg = "Approved contact for outreach send." if decision_norm == "approve" else "Rejected contact from outreach queue."
    return 200, {"ok": True, "message": msg}


def _contact_payload(contact_id: str) -> Tuple[int, Dict[str, Any]]:
    outreach_df = _safe_dataframe_read(DATASET_PATHS["outreach"])
    if outreach_df is None:
        return 404, {"error": "Outreach dataset unavailable."}
    if "id" not in outreach_df.columns:
        return 400, {"error": "Outreach dataset missing id column."}

    target = str(contact_id).strip()
    mask = outreach_df["id"].astype(str).eq(target)
    if int(mask.sum()) == 0:
        return 404, {"error": f"Contact id {contact_id} not found."}

    row = outreach_df[mask].iloc[0].where(pd.notnull(outreach_df[mask].iloc[0]), None).to_dict()
    return 200, {
        "contact": row,
        "explainability": _compute_explainability(row),
    }


_load_run_history()
_init_app_db()
if not RUN_HISTORY:
    RUN_HISTORY.append(_current_quality_snapshot())


app = Flask(__name__, static_folder=str(UI_DIST), static_url_path="/")


@app.get("/api/health")
def api_health() -> Any:
    return jsonify({"ok": True, "timestamp": now_utc_iso()})


@app.get("/api/status")
def api_status() -> Any:
    with STATE_LOCK:
        payload = RUN_STATE.snapshot()
    return jsonify(payload)


@app.post("/api/run")
def api_run() -> Any:
    if not _is_control_authorized():
        return jsonify({"ok": False, "error": "Unauthorized control request."}), 401
    payload = request.get_json(silent=True) or {}
    test_mode = bool(payload.get("test_mode", False))
    from_stage = payload.get("from_stage")
    fast_mode_raw = payload.get("fast_mode")

    if from_stage in ("", None):
        from_stage = None
    elif from_stage not in VALID_STAGES:
        return jsonify({"ok": False, "error": "Invalid from_stage value."}), 400

    fast_mode: Optional[bool]
    if fast_mode_raw is None:
        fast_mode = None
    elif isinstance(fast_mode_raw, bool):
        fast_mode = fast_mode_raw
    else:
        return jsonify({"ok": False, "error": "fast_mode must be boolean when provided."}), 400

    ok, message = _start_process(test_mode=test_mode, from_stage=from_stage, fast_mode=fast_mode)
    status_code = 200 if ok else 409
    return jsonify({"ok": ok, "message": message}), status_code


@app.post("/api/stop")
def api_stop() -> Any:
    if not _is_control_authorized():
        return jsonify({"ok": False, "error": "Unauthorized control request."}), 401
    ok, message = _stop_process()
    status_code = 200 if ok else 409
    return jsonify({"ok": ok, "message": message}), status_code


@app.get("/api/summary")
def api_summary() -> Any:
    return jsonify(_build_summary())


@app.get("/api/quality")
def api_quality() -> Any:
    return jsonify(_quality_payload())


@app.get("/api/preview/<dataset>")
def api_preview(dataset: str) -> Any:
    try:
        limit = int(request.args.get("limit", 20))
    except ValueError:
        return jsonify({"error": "limit must be an integer"}), 400

    status_code, payload = _preview_dataset(dataset, limit)
    return jsonify(payload), status_code


@app.get("/api/stage/<stage>/details")
def api_stage_details(stage: str) -> Any:
    try:
        limit = int(request.args.get("limit", 20))
    except ValueError:
        return jsonify({"error": "limit must be an integer"}), 400
    status_code, payload = _stage_details(stage, max(1, min(limit, 50)))
    return jsonify(payload), status_code


@app.get("/api/stage/<stage>/samples")
def api_stage_samples(stage: str) -> Any:
    # Compatibility endpoint for requested pattern.
    return api_stage_details(stage)


@app.get("/api/queue")
def api_queue() -> Any:
    status = str(request.args.get("status", "pending"))
    try:
        limit = int(request.args.get("limit", 100))
    except ValueError:
        return jsonify({"error": "limit must be an integer"}), 400
    status_code, payload = _queue_payload(limit=max(1, min(limit, 300)), status=status)
    return jsonify(payload), status_code


@app.post("/api/queue/action")
def api_queue_action() -> Any:
    if not _is_control_authorized():
        return jsonify({"error": "Unauthorized control request."}), 401
    payload = request.get_json(silent=True) or {}
    if not payload:
        payload = request.args.to_dict()
    contact_id = str(payload.get("id", "")).strip()
    decision = str(payload.get("decision", "")).strip()
    if not contact_id:
        return jsonify({"error": "Missing id in payload."}), 400
    status_code, response = _update_approval(contact_id=contact_id, decision=decision)
    return jsonify(response), status_code


@app.get("/api/contact/<contact_id>")
def api_contact(contact_id: str) -> Any:
    status_code, payload = _contact_payload(contact_id)
    return jsonify(payload), status_code


@app.get("/api/download/<dataset>")
def api_download(dataset: str) -> Any:
    """Download dataset as CSV (default), JSON, or XLSX."""
    if dataset not in DATASET_PATHS:
        return jsonify({"error": f"Unknown dataset '{dataset}'."}), 404

    path = DATASET_PATHS[dataset]
    if not path.exists():
        return jsonify({"error": f"Dataset file not found: {path}"}), 404

    fmt = str(request.args.get("format", "csv")).strip().lower()
    if fmt == "csv":
        return send_file(
            path,
            mimetype="text/csv",
            as_attachment=True,
            download_name=path.name,
        )

    df = _safe_dataframe_read(path)
    if df is None:
        return jsonify({"error": f"Dataset file unreadable: {path}"}), 500

    if fmt == "json":
        payload = json.dumps(df.where(pd.notnull(df), None).to_dict(orient="records"), ensure_ascii=False, indent=2)
        buffer = io.BytesIO(payload.encode("utf-8"))
        return send_file(
            buffer,
            mimetype="application/json",
            as_attachment=True,
            download_name=f"{path.stem}.json",
        )

    if fmt == "xlsx":
        try:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="data")
            buffer.seek(0)
            return send_file(
                buffer,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name=f"{path.stem}.xlsx",
            )
        except Exception as exc:
            return jsonify(
                {
                    "error": "XLSX export requires openpyxl.",
                    "detail": str(exc),
                }
            ), 500

    return jsonify({"error": "Unsupported format. Use csv, json, or xlsx."}), 400


@app.get("/")
def serve_root() -> Any:
    index_path = UI_DIST / "index.html"
    if index_path.exists():
        return send_from_directory(str(UI_DIST), "index.html")
    return jsonify(
        {
            "message": "UI build not found.",
            "next_step": "Run 'npm install && npm run build' inside the ui directory.",
        }
    )


@app.get("/<path:path>")
def serve_spa(path: str) -> Any:
    if path.startswith("api/"):
        return jsonify({"error": "Not found"}), 404

    asset_path = UI_DIST / path
    if asset_path.exists() and asset_path.is_file():
        return send_from_directory(str(UI_DIST), path)

    index_path = UI_DIST / "index.html"
    if index_path.exists():
        return send_from_directory(str(UI_DIST), "index.html")
    return jsonify({"error": "UI build not found"}), 404


if __name__ == "__main__":
    host = os.getenv("WEB_APP_HOST", "127.0.0.1")
    port = int(os.getenv("WEB_APP_PORT", "8000"))
    app.run(host=host, port=port, debug=False)
