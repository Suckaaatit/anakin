"""
outreach.py — Outreach message generation module for TechSparks GTM Automation.

Generates personalized email and LinkedIn outreach drafts using LLM.
"""

import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional, Tuple

import json5
import pandas as pd
from openai import OpenAI
from llm_runtime import (
    backoff_delay,
    create_llm_client,
    get_llm_model,
    get_llm_rate_limiter,
    is_rate_limited_error,
)

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
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("openai._base_client").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

LINKEDIN_NOTE_MAX = 299

OUTREACH_SYSTEM_PROMPT = """You are a senior SDR writing personalised ABM outreach for a data intelligence company.

RULES:
1. NEVER mention "Anakin" or any specific company name on our side.
2. Frame the value as: data intelligence, pricing transparency, competitive benchmarking, assortment insights, or market data automation.
3. End ALL email bodies with exactly this sentence: "Once you find this relevant, I can introduce you to a YC-backed company that specialises in solving this."
4. No filler: no "I hope this finds you well", no "synergy", no "leverage", no "reach out".
5. Email subject line: under 8 words. No question marks in subject.
6. LinkedIn note: count characters carefully, MUST be strictly under 300 characters.
7. VC contacts (outreach_sequence=PARTNERSHIP_SEQUENCE): frame as partnership/ecosystem opportunity, not a sales pitch.
8. Return ONLY valid JSON. No markdown, no preamble, no trailing text.

JSON schema — return ALL fields:
{
  "email_subject_a": "version A subject line (problem-focused)",
  "email_subject_b": "version B subject line (outcome-focused, different angle)",
  "email_body_pre_event": "3-4 short paragraphs. Reference upcoming TechSparks attendance.",
  "email_body_during_event": "2-3 short paragraphs. Reference being at TechSparks right now. Short. Invite to brief conversation.",
  "email_body_post_event": "3-4 short paragraphs. Reference shared TechSparks experience. Reference their specific session topic.",
  "linkedin_note": "connection request note — strictly under 300 characters, references their session topic",
  "sequence_timing": "recommended timing e.g. 7 days pre-event or 2 days after event",
  "message_variant": "A or B (randomly assign)",
  "experiment_group": "CONTROL or TREATMENT (randomly assign)"
}"""


def _parse_json_payload(content: str) -> Dict[str, Any]:
    """
    Parse model JSON output robustly.

    Supports:
    - strict JSON
    - json5
    - responses with leading/trailing prose where the JSON object is embedded
    """
    text = str(content).strip()
    candidates = [text]

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        extracted = text[start : end + 1].strip()
        if extracted and extracted not in candidates:
            candidates.append(extracted)

    last_error: Optional[Exception] = None
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except Exception as exc:
            last_error = exc
        try:
            return json5.loads(candidate)
        except Exception as exc:
            last_error = exc

    raise ValueError(f"Could not parse JSON payload: {last_error}")


def truncate_at_sentence_boundary(text: str, max_chars: int) -> str:
    """
    Truncate text at last sentence boundary before max_chars (R8 fix).
    
    Args:
        text: Input text.
        max_chars: Maximum character limit.
        
    Returns:
        Truncated text.
    """
    if len(text) <= max_chars:
        return text
    
    # Split by sentence boundaries
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    result = ""
    for sentence in sentences:
        if len(result) + len(sentence) + 1 <= max_chars:
            result += sentence + " "
        else:
            break
    
    result = result.strip()
    
    # If single sentence exceeds limit, hard truncate
    if not result:
        return text[:max_chars-3] + "..."
    
    return result


def sanitise_drafts(drafts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize outreach drafts.
    
    - Replace all case variants of "anakin" with "[COMPANY]"
    - Apply truncate_at_sentence_boundary to linkedin_note
    
    Args:
        drafts: Draft dictionary from LLM.
        
    Returns:
        Sanitized drafts.
    """
    result = dict(drafts)
    anakin_found = False
    
    # Replace anakin in all string fields
    for key, value in result.items():
        if isinstance(value, str):
            if re.search(r'anakin', value, re.IGNORECASE):
                anakin_found = True
                result[key] = re.sub(r'anakin', '[COMPANY]', value, flags=re.IGNORECASE)
    
    # Truncate LinkedIn note
    if "linkedin_note" in result:
        original = result["linkedin_note"]
        truncated = truncate_at_sentence_boundary(original, LINKEDIN_NOTE_MAX)
        if len(truncated) < len(original):
            logger.warning(f"LinkedIn note truncated from {len(original)} to {len(truncated)} chars")
        result["linkedin_note"] = truncated
    
    if anakin_found:
        logger.warning("'Anakin' replaced with [COMPANY] in outreach drafts")
    
    return result


def build_outreach_prompt(row: Dict[str, Any]) -> str:
    """
    Build user prompt for outreach generation.
    
    Args:
        row: Routed contact row.
        
    Returns:
        Formatted user prompt.
    """
    fields = [
        f"Name: {row.get('name', 'N/A')}",
        f"Title: {row.get('title', 'N/A')}",
        f"Company: {row.get('company', 'N/A')}",
        f"Persona Archetype: {row.get('persona_archetype', 'N/A')}",
        f"Persona Summary: {row.get('persona_summary', 'N/A')}",
        f"Context Summary: {row.get('context_summary', 'N/A')}",
        f"Personalization Themes: {row.get('personalization_themes', 'N/A')}",
        f"Recommended Hook: {row.get('recommended_hook', 'N/A')}",
        f"Final Route: {row.get('final_route', 'N/A')}",
        f"Outreach Sequence: {row.get('outreach_sequence', 'N/A')}",
        f"Session Topic: {row.get('session_topic', 'N/A')}",
        f"Previous Role (Inferred): {row.get('previous_role_inferred', 'N/A')}",
        f"Segment Cluster: {row.get('segment_cluster', 'N/A')}",
    ]
    
    return "\n".join(fields) + "\n\nGenerate the outreach drafts now. LinkedIn note must be under 300 characters."


def build_outreach_fast(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build outreach drafts without external LLM calls.

    This mode is cached/template-based and token-free for fast local execution.
    """
    name = str(row.get("name", "there")).strip()
    first_name = name.split()[0] if name else "there"
    company = str(row.get("company", "your company")).strip()
    session_topic = str(row.get("session_topic", "your TechSparks session")).strip()
    sequence = str(row.get("outreach_sequence", "POST_EVENT")).strip()
    cluster = str(row.get("segment_cluster", "Tech Leader")).strip()

    value_line = (
        "We help teams automate competitive benchmarking, pricing visibility, and market signals without manual reporting overhead."
    )
    ending_line = (
        "Once you find this relevant, I can introduce you to a YC-backed company that specialises in solving this."
    )

    pre_event_body = (
        f"Hi {first_name},\n\n"
        f"I am reaching out ahead of TechSparks because your session on {session_topic} is directly relevant to market decision velocity.\n\n"
        f"{value_line}\n\n"
        f"{ending_line}"
    )

    during_event_body = (
        f"Hi {first_name},\n\n"
        f"I am at TechSparks today and wanted to connect around your session on {session_topic}.\n\n"
        f"{value_line}\n\n"
        f"{ending_line}"
    )

    post_event_body = (
        f"Hi {first_name},\n\n"
        f"Your TechSparks session on {session_topic} made the need for faster market intelligence very clear.\n\n"
        f"{value_line}\n\n"
        f"{ending_line}"
    )

    linkedin_note = (
        f"Hi {first_name}, your TechSparks session on {session_topic} was sharp. "
        f"Would love to connect and share a data-intelligence angle relevant to {company}."
    )
    linkedin_note = truncate_at_sentence_boundary(linkedin_note, LINKEDIN_NOTE_MAX)

    if "PRE_EVENT" in sequence:
        sequence_timing = "7 days pre-event"
    elif "DURING_EVENT" in sequence:
        sequence_timing = "same day at event"
    else:
        sequence_timing = "2 days post-event"

    # Stable experiment assignment from row id if available.
    row_id = str(row.get("id", "0"))
    variant_seed = sum(ord(ch) for ch in row_id + company)
    message_variant = "A" if (variant_seed % 2 == 0) else "B"
    experiment_group = "CONTROL" if (variant_seed % 3 == 0) else "TREATMENT"

    return {
        "email_subject_a": f"{cluster} market blind spots"[:60],
        "email_subject_b": f"Faster signals for {company}"[:60],
        "email_body_pre_event": pre_event_body,
        "email_body_during_event": during_event_body,
        "email_body_post_event": post_event_body,
        "linkedin_note": linkedin_note,
        "sequence_timing": sequence_timing,
        "message_variant": message_variant,
        "experiment_group": experiment_group,
    }


def call_llm_outreach(
    user_message: str,
    client: OpenAI,
    model: str,
    retries: int = 1
) -> Dict[str, Any]:
    """
    Call LLM for outreach generation with retry logic.
    
    Args:
        user_message: User prompt.
        client: OpenAI client instance.
        model: LLM model name.
        retries: Number of retry attempts.
        
    Returns:
        Parsed outreach dict or error dict.
    """
    effective_retries = max(0, int(os.getenv("OUTREACH_RETRY_ATTEMPTS", str(retries))))
    fail_fast_on_rate_limit = os.getenv("OUTREACH_FAIL_FAST_ON_429", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    rate_limited_fail_fast = False
    limiter = get_llm_rate_limiter()
    for attempt in range(effective_retries + 1):
        try:
            with limiter.slot():
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": OUTREACH_SYSTEM_PROMPT},
                        {"role": "user", "content": user_message}
                    ],
                    temperature=0.4,
                    max_tokens=800
                )
            
            content = response.choices[0].message.content.strip()
            
            # Strip markdown fences
            content = content.replace("```json", "").replace("```", "").strip()
            
            try:
                parsed = _parse_json_payload(content)
            except Exception:
                logger.warning("Outreach JSON parsing failed for this attempt; retrying.")
                if attempt < effective_retries:
                    continue
                raise
            
            return parsed
            
        except Exception as e:
            rate_limited = is_rate_limited_error(e)
            if rate_limited and fail_fast_on_rate_limit:
                logger.warning(
                    "Outreach LLM rate-limited on attempt %s; fail-fast enabled for this row.",
                    attempt + 1,
                )
                rate_limited_fail_fast = True
                break
            if attempt < effective_retries:
                delay = backoff_delay(attempt, e)
                if rate_limited:
                    logger.warning(
                        "Outreach LLM rate-limited on attempt %s; retrying in %.2fs",
                        attempt + 1,
                        delay,
                    )
                else:
                    logger.warning(
                        "Outreach LLM call failed on attempt %s; retrying in %.2fs (%s)",
                        attempt + 1,
                        delay,
                        e,
                    )
                time.sleep(delay)
            else:
                logger.error(f"Outreach LLM call failed after retries: {e}")
                return {
                    "email_subject_a": "DRAFT_ERROR",
                    "email_subject_b": "DRAFT_ERROR",
                    "email_body_pre_event": "DRAFT_ERROR",
                    "email_body_during_event": "DRAFT_ERROR",
                    "email_body_post_event": "DRAFT_ERROR",
                    "linkedin_note": "DRAFT_ERROR",
                    "sequence_timing": "DRAFT_ERROR",
                    "message_variant": "A",
                    "experiment_group": "CONTROL",
                    "error": "OUTREACH_GENERATION_FAILED"
                }

    if rate_limited_fail_fast:
        return {
            "email_subject_a": "DRAFT_ERROR",
            "email_subject_b": "DRAFT_ERROR",
            "email_body_pre_event": "DRAFT_ERROR",
            "email_body_during_event": "DRAFT_ERROR",
            "email_body_post_event": "DRAFT_ERROR",
            "linkedin_note": "DRAFT_ERROR",
            "sequence_timing": "DRAFT_ERROR",
            "message_variant": "A",
            "experiment_group": "CONTROL",
            "error": "OUTREACH_RATE_LIMITED_FAIL_FAST",
        }
    
    return {
        "email_subject_a": "DRAFT_ERROR",
        "email_subject_b": "DRAFT_ERROR",
        "email_body_pre_event": "DRAFT_ERROR",
        "email_body_during_event": "DRAFT_ERROR",
        "email_body_post_event": "DRAFT_ERROR",
        "linkedin_note": "DRAFT_ERROR",
        "sequence_timing": "DRAFT_ERROR",
        "message_variant": "A",
        "experiment_group": "CONTROL",
        "error": "OUTREACH_GENERATION_FAILED"
    }


def generate_outreach_drafts(
    input_csv: str = "data/speakers_routed.csv",
    output_csv: str = "output/outreach_drafts.csv",
    delay: float = 0.0,
    fast_mode: bool = False
) -> pd.DataFrame:
    """
    Generate outreach drafts for all approved contacts.
    
    Args:
        input_csv: Path to routed CSV.
        output_csv: Path to output CSV.
        delay: Delay between LLM calls.
        
    Returns:
        DataFrame with outreach drafts.
    """
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    
    # Split active vs skipped
    active_mask = df["outreach_approved"] == "PENDING_REVIEW"
    active_df = df[active_mask].copy()
    skipped_df = df[~active_mask].copy()
    
    print(f"Generating outreach for {len(active_df)} active contacts ({len(skipped_df)} skipped)")
    
    if fast_mode:
        print("FAST MODE: generating outreach drafts without LLM calls.")

    row_delay = max(0.0, float(os.getenv("OUTREACH_ROW_DELAY_SEC", str(delay))))

    # Initialize client only in live mode.
    client: Optional[OpenAI]
    if fast_mode:
        client = None
    else:
        client = create_llm_client()
    model = get_llm_model()
    
    outreach_workers = 1 if fast_mode else max(1, int(os.getenv("OUTREACH_MAX_WORKERS", "1")))
    if outreach_workers > 1:
        print(f"LIVE MODE: outreach parallelism enabled ({outreach_workers} workers).")

    # Process active rows
    results = []
    fallback_count = 0
    llm_disabled_due_rate_limit = False
    consecutive_rate_limit_fallbacks = 0
    disable_llm_on_rate_limit = os.getenv("OUTREACH_DISABLE_LLM_ON_RATE_LIMIT", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    rate_limit_switch_threshold = max(1, int(os.getenv("OUTREACH_RATE_LIMIT_SWITCH_THRESHOLD", "1")))
    indexed_rows = [(idx, row.to_dict()) for idx, row in active_df.iterrows()]

    def _draft_error_row(row_dict: Dict[str, Any]) -> Dict[str, Any]:
        result_row = dict(row_dict)
        result_row["outreach_status"] = "DRAFT_ERROR"
        result_row["email_subject_a"] = "DRAFT_ERROR"
        result_row["email_subject_b"] = "DRAFT_ERROR"
        result_row["email_body_pre_event"] = "DRAFT_ERROR"
        result_row["email_body_during_event"] = "DRAFT_ERROR"
        result_row["email_body_post_event"] = "DRAFT_ERROR"
        result_row["linkedin_note"] = "DRAFT_ERROR"
        result_row["sequence_timing"] = "DRAFT_ERROR"
        result_row["message_variant"] = "A"
        result_row["experiment_group"] = "CONTROL"
        return result_row

    def _process_one(
        idx: int,
        row_dict: Dict[str, Any],
        force_rule_based: bool = False,
    ) -> tuple[int, Dict[str, Any], bool, bool]:
        try:
            if fast_mode:
                drafts = build_outreach_fast(row_dict)
                draft_source = "CACHED_RULE_BASED"
                fallback_reason = ""
                rate_limited_fallback = False
            elif force_rule_based:
                drafts = build_outreach_fast(row_dict)
                draft_source = "FALLBACK_RULE_BASED"
                fallback_reason = "GLOBAL_RATE_LIMIT_SWITCH"
                rate_limited_fallback = False
            else:
                user_prompt = build_outreach_prompt(row_dict)
                drafts = call_llm_outreach(user_prompt, client, model)
                if drafts.get("error"):
                    error_code = str(drafts.get("error", "")).strip().upper()
                    logger.warning(
                        "Outreach LLM hard-failed for row %s; using rule-based fallback.", idx
                    )
                    drafts = build_outreach_fast(row_dict)
                    draft_source = "FALLBACK_RULE_BASED"
                    fallback_reason = error_code or "LLM_ERROR_RATE_LIMIT_OR_PARSE"
                    rate_limited_fallback = "RATE_LIMIT" in error_code
                else:
                    draft_source = "LIVE_LLM"
                    fallback_reason = ""
                    rate_limited_fallback = False

            drafts = sanitise_drafts(drafts)
            result_row = dict(row_dict)
            for key in [
                "email_subject_a",
                "email_subject_b",
                "email_body_pre_event",
                "email_body_during_event",
                "email_body_post_event",
                "linkedin_note",
                "sequence_timing",
                "message_variant",
                "experiment_group",
            ]:
                result_row[key] = drafts.get(key, "DRAFT_ERROR")
            result_row["outreach_status"] = "DRAFT_GENERATED"
            result_row["outreach_source"] = draft_source
            if fallback_reason:
                result_row["llm_error"] = fallback_reason
            return idx, result_row, bool(fallback_reason), rate_limited_fallback
        except Exception as exc:
            logger.error(f"Outreach generation failed for row {idx}: {exc}")
            error_row = _draft_error_row(row_dict)
            error_row["outreach_source"] = "ERROR"
            error_row["llm_error"] = str(exc)
            return idx, error_row, False, False

    if outreach_workers == 1:
        for idx, row_dict in indexed_rows:
            force_rule_based = (not fast_mode) and llm_disabled_due_rate_limit
            _idx, result_row, used_fallback, rate_limited_fallback = _process_one(
                idx,
                row_dict,
                force_rule_based=force_rule_based,
            )
            results.append(result_row)
            if used_fallback:
                fallback_count += 1
            if (not fast_mode) and disable_llm_on_rate_limit and not llm_disabled_due_rate_limit:
                if rate_limited_fallback:
                    consecutive_rate_limit_fallbacks += 1
                else:
                    consecutive_rate_limit_fallbacks = 0
                if consecutive_rate_limit_fallbacks >= rate_limit_switch_threshold:
                    llm_disabled_due_rate_limit = True
                    logger.warning(
                        "Outreach LLM rate-limited for %s consecutive rows. "
                        "Switching remaining rows to rule-based fallback.",
                        consecutive_rate_limit_fallbacks,
                    )
            if (len(results)) % 5 == 0:
                print(f"Generated {len(results)} outreach drafts...")
            if (not fast_mode) and row_delay > 0:
                time.sleep(row_delay)
    else:
        ordered_results: Dict[int, Tuple[Dict[str, Any], bool, bool]] = {}
        completed = 0
        with ThreadPoolExecutor(max_workers=outreach_workers) as executor:
            futures = {executor.submit(_process_one, idx, row_dict): idx for idx, row_dict in indexed_rows}
            for future in as_completed(futures):
                idx, result_row, used_fallback, rate_limited_fallback = future.result()
                ordered_results[idx] = (result_row, used_fallback, rate_limited_fallback)
                completed += 1
                if completed % 5 == 0:
                    print(f"Generated {completed}/{len(indexed_rows)} outreach drafts...")
        for idx, _row_dict in indexed_rows:
            result_row, used_fallback, _rate_limited_fallback = ordered_results[idx]
            results.append(result_row)
            if used_fallback:
                fallback_count += 1
    
    # Add placeholder fields for skipped rows
    skipped_results = []
    for idx, row in skipped_df.iterrows():
        result_row = dict(row)
        for key in ["email_subject_a", "email_subject_b", "email_body_pre_event",
                   "email_body_during_event", "email_body_post_event", "linkedin_note",
                    "sequence_timing", "message_variant", "experiment_group"]:
            result_row[key] = ""
        result_row["outreach_status"] = "SKIPPED"
        result_row["outreach_source"] = "SKIPPED"
        skipped_results.append(result_row)
    
    # Combine results
    all_results = results + skipped_results
    result_df = pd.DataFrame(all_results)
    
    # Print summary
    print(f"\n=== Outreach Generation Summary ===")
    print(f"Drafts generated: {len(results)}")
    print(f"Fallback used: {fallback_count}")
    if llm_disabled_due_rate_limit:
        print("Live LLM disabled mid-run due to repeated rate limits; switched to rule-based fallback.")
    print(f"Skipped: {len(skipped_results)}")
    
    # Write output
    result_df.to_csv(output_csv, encoding="utf-8-sig", index=False)
    print(f"Output written to: {output_csv}")
    
    return result_df


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    generate_outreach_drafts(fast_mode=os.getenv("FAST_MODE", "0") == "1")
