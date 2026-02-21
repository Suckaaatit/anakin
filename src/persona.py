"""
persona.py — AI persona generation module for TechSparks GTM Automation.

Generates contact personas using LLM with strict validation and safeguards.
"""

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

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

VALID_ARCHETYPES = [
    "Visionary Founder",
    "Technical Decision Maker",
    "Revenue Leader",
    "Operator / Scaler",
    "Board-Level Strategist",
    "Data-Driven Builder",
    "INSUFFICIENT_DATA",
]

VALID_ASSIGNMENTS = ["Senior AE", "AE", "SDR", "Not Relevant"]

PERSONA_SYSTEM_PROMPT = """You are a senior GTM analyst building contact intelligence for ABM outreach.

STRICT RULES:
1. Use ONLY the data I provide. Never invent facts, past employers, or interests.
2. If any field is "NOT_FOUND", "NOT_AVAILABLE", or empty — omit it from reasoning entirely.
3. NEVER mention any company named "Anakin" in any field of your output.
4. Return ONLY valid JSON. No preamble, no markdown, no backticks, no trailing text.
5. If data is too sparse, set persona_archetype to "INSUFFICIENT_DATA".
6. No assumptions about gender, ethnicity, age, or personal life.

RELEVANCE SCORE RUBRIC (apply strictly — do not inflate):
90–100: C-Suite/Founder in fintech, saas_b2b, ecommerce, payments where pricing intelligence or data automation is a direct operational need.
70–89:  VP-level in relevant industry, or Founder in adjacent industry.
50–69:  Director/Head, or mid-relevance industry.
30–49:  Manager/IC, or low-relevance but adjacent.
0–29:   Student, media, government, nonprofit, or no relevance to data intelligence.

Output exactly this JSON schema, nothing else:
{
  "persona_archetype": "Visionary Founder | Technical Decision Maker | Revenue Leader | Operator / Scaler | Board-Level Strategist | Data-Driven Builder | INSUFFICIENT_DATA",
  "persona_summary": "2-3 sentences. What they do, what they care about, what pressure they are under. Only from provided data.",
  "context_summary": "1-2 sentences. Why they are relevant to pricing intelligence, competitive benchmarking, assortment insights, or data automation. Be specific to their role.",
  "personalization_themes": ["theme tied to their role", "theme tied to their company challenge", "theme tied to data intelligence value"],
  "relevance_score": 0,
  "recommended_hook": "One specific opening line referencing their actual session topic or company. Max 20 words. Not generic.",
  "assign_to": "Senior AE | AE | SDR | Not Relevant",
  "persona_reason": "One sentence: why this archetype was chosen. Reference title and industry."
}"""


def contains_anakin(data: Dict[str, Any]) -> bool:
    """
    Check if any string value in dict contains 'anakin' (case-insensitive).
    
    Args:
        data: Dictionary to check.
        
    Returns:
        True if 'anakin' found, False otherwise.
    """
    def search_value(obj: Any) -> bool:
        if isinstance(obj, str):
            return "anakin" in obj.lower()
        elif isinstance(obj, dict):
            return any(search_value(v) for v in obj.values())
        elif isinstance(obj, list):
            return any(search_value(item) for item in obj)
        return False
    
    return search_value(data)


def build_user_prompt(row: Dict[str, Any]) -> str:
    """
    Format all enriched fields into the user message.
    
    Args:
        row: Enriched contact row.
        
    Returns:
        Formatted user prompt string.
    """
    fields = [
        f"Name: {row.get('name', 'N/A')}",
        f"Title: {row.get('title', 'N/A')}",
        f"Company: {row.get('company', 'N/A')}",
        f"Seniority: {row.get('seniority', 'N/A')}",
        f"Industry: {row.get('industry', 'N/A')}",
        f"Industry Source: {row.get('industry_source', 'N/A')}",
        f"Industry Relevance Score: {row.get('industry_relevance_score', 'N/A')}",
        f"Event Role: {row.get('event_role', 'N/A')}",
        f"Session Topic: {row.get('session_topic', 'N/A')}",
        f"Job History: {row.get('job_history', 'N/A')}",
        f"Previous Role (Inferred): {row.get('previous_role_inferred', 'N/A')}",
        f"News Signal: {row.get('news_signal', 'N/A')}",
        f"Signals: {row.get('signals', 'N/A')}",
        f"LinkedIn URL: {row.get('linkedin_url', 'N/A')}",
        f"LinkedIn Source: {row.get('linkedin_source', 'N/A')}",
        f"Evidence Score: {row.get('evidence_score', 'N/A')}",
        f"Enrichment Confidence Score: {row.get('enrichment_confidence_score', 'N/A')}",
    ]
    
    return "\n".join(fields) + "\n\nGenerate the JSON persona now."


def normalize_persona_payload(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort normalization for imperfect model JSON outputs."""
    normalized = dict(parsed)

    archetype = str(normalized.get("persona_archetype", "")).strip()
    if "|" in archetype:
        archetype = archetype.split("|", 1)[0].strip()
    normalized["persona_archetype"] = archetype

    assign_to = str(normalized.get("assign_to", "")).strip()
    if "|" in assign_to:
        assign_to = assign_to.split("|", 1)[0].strip()
    normalized["assign_to"] = assign_to

    themes = normalized.get("personalization_themes", [])
    if isinstance(themes, str):
        themes = [chunk.strip() for chunk in themes.replace("\n", "|").replace(";", "|").split("|") if chunk.strip()]
    elif isinstance(themes, list):
        themes = [str(item).strip() for item in themes if str(item).strip()]
    else:
        themes = []
    if not themes:
        themes = ["role priorities", "competitive pressure", "data automation value"]
    normalized["personalization_themes"] = themes[:3]

    relevance_raw = normalized.get("relevance_score")
    if relevance_raw in (None, ""):
        relevance_score = 50
    else:
        try:
            relevance_score = int(round(float(relevance_raw)))
        except (TypeError, ValueError):
            relevance_score = 50
    normalized["relevance_score"] = max(0, min(100, relevance_score))

    for field in ["persona_summary", "context_summary", "persona_reason"]:
        value = str(normalized.get(field, "")).strip()
        normalized[field] = value or "Not Publicly Available"

    hook = str(normalized.get("recommended_hook", "")).strip()
    if not hook:
        hook = "Relevant opportunity to discuss pricing intelligence and competitive benchmarking."
    normalized["recommended_hook"] = _truncate_words(hook, max_words=20)

    return normalized


def validate_persona(parsed: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate persona output structure.
    
    Args:
        parsed: Parsed JSON persona.
        
    Returns:
        Tuple of (is_valid, error_message).
    """
    required_fields = [
        "persona_archetype", "persona_summary", "context_summary",
        "personalization_themes", "relevance_score", "recommended_hook",
        "assign_to", "persona_reason"
    ]
    
    # Check required fields
    for field in required_fields:
        if field not in parsed:
            return False, f"Missing field: {field}"
        value = parsed.get(field)
        if field == "relevance_score":
            continue
        if field == "personalization_themes":
            if not isinstance(value, list) or len(value) == 0:
                return False, "Missing or empty field: personalization_themes"
            continue
        if str(value).strip() == "":
            return False, f"Missing or empty field: {field}"
    
    # Check archetype validity
    if parsed["persona_archetype"] not in VALID_ARCHETYPES:
        return False, f"Invalid archetype: {parsed['persona_archetype']}"
    
    # Check relevance score
    score = parsed.get("relevance_score")
    if not isinstance(score, (int, float)) or score < 0 or score > 100:
        return False, f"Invalid relevance_score: {score}"
    
    # Check assign_to validity
    if parsed["assign_to"] not in VALID_ASSIGNMENTS:
        return False, f"Invalid assign_to: {parsed['assign_to']}"
    
    return True, ""


def _truncate_words(text: str, max_words: int = 20) -> str:
    """Truncate text by word count."""
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words])


def _score_relevance_fast(row: Dict[str, Any]) -> int:
    """Fast rule-based relevance score (0-100) without external LLM calls."""
    seniority = str(row.get("seniority", "")).strip()
    industry = str(row.get("industry", "")).strip()
    event_role = str(row.get("event_role", "")).strip()

    seniority_points = {
        "C-Suite / Founder": 45,
        "VP": 30,
        "Director / Head": 20,
        "Senior IC / Manager": 12,
        "Manager": 10,
        "IC": 8,
        "Junior / Intern": 2,
    }.get(seniority, 8)

    core = {"fintech", "saas_b2b", "ecommerce_d2c", "payments"}
    adjacent = {"ai_tech", "marketplace", "logistics", "mobility_ev", "foodtech", "deep_tech", "healthtech"}

    if industry in core:
        industry_points = 40
    elif industry == "venture_capital":
        industry_points = 34
    elif industry in adjacent:
        industry_points = 24
    else:
        industry_points = 14

    role_bonus = 8 if event_role in {"Judge", "Mentor", "Keynote Speaker"} else 0
    confidence_bonus = int(row.get("enrichment_confidence_score", 0)) * 2
    evidence_bonus = int(min(max(float(row.get("evidence_score", 0) or 0), 0.0), 100.0) / 10)

    score = seniority_points + industry_points + role_bonus + confidence_bonus + evidence_bonus
    return max(0, min(100, int(score)))


def _archetype_fast(row: Dict[str, Any], relevance_score: int) -> str:
    """Pick persona archetype using rule mapping."""
    seniority = str(row.get("seniority", "")).strip()
    industry = str(row.get("industry", "")).strip()
    event_role = str(row.get("event_role", "")).strip()

    if relevance_score < 30:
        return "INSUFFICIENT_DATA"
    if industry == "venture_capital" or event_role in {"Judge", "Mentor"}:
        return "Board-Level Strategist"
    if seniority == "C-Suite / Founder":
        return "Visionary Founder"
    if seniority == "VP":
        return "Revenue Leader"
    if seniority in {"Director / Head", "Senior IC / Manager"}:
        return "Operator / Scaler"
    return "Data-Driven Builder"


def _assign_to_fast(relevance_score: int) -> str:
    """Route owner assignment using score bands."""
    if relevance_score >= 80:
        return "Senior AE"
    if relevance_score >= 60:
        return "AE"
    if relevance_score >= 30:
        return "SDR"
    return "Not Relevant"


def build_persona_fast(row: Dict[str, Any]) -> Dict[str, Any]:
    """Generate persona payload using local rules only (no external API calls)."""
    title = str(row.get("title", ""))
    company = str(row.get("company", ""))
    seniority = str(row.get("seniority", ""))
    industry = str(row.get("industry", "other_tech"))
    session_topic = str(row.get("session_topic", "their session"))

    relevance_score = _score_relevance_fast(row)
    archetype = _archetype_fast(row, relevance_score)
    assign_to = _assign_to_fast(relevance_score)

    persona_summary = (
        f"{title} at {company}. Focused on growth and execution priorities in {industry} "
        "with strong pressure to move on reliable market signals."
    )
    context_summary = (
        "Relevant for data intelligence through pricing benchmarks, competitor tracking, "
        "and faster decision support."
    )
    themes = [
        f"{seniority} operating priorities",
        f"{industry} competitive pressure",
        "market data automation at execution speed",
    ]
    recommended_hook = _truncate_words(
        f"Your session on {session_topic} shows why real-time market intelligence now matters for operating decisions.",
        max_words=20,
    )
    persona_reason = f"Rule-based persona from title={title}, seniority={seniority}, industry={industry}."

    return {
        "persona_archetype": archetype,
        "persona_summary": persona_summary,
        "context_summary": context_summary,
        "personalization_themes": themes,
        "relevance_score": relevance_score,
        "recommended_hook": recommended_hook,
        "assign_to": assign_to,
        "persona_reason": persona_reason,
    }


def call_llm(
    user_message: str,
    client: OpenAI,
    model: str,
    token_tracker: Dict[str, int],
    token_lock: Optional[threading.Lock] = None,
    retries: int = 1
) -> Dict[str, Any]:
    """
    Call LLM with retry logic and validation.
    
    Args:
        user_message: User prompt.
        client: OpenAI client instance.
        model: LLM model name.
        token_tracker: Token usage tracker dict.
        retries: Number of retry attempts.
        
    Returns:
        Parsed persona dict or error dict.
    """
    effective_retries = max(0, int(os.getenv("PERSONA_RETRY_ATTEMPTS", str(retries))))
    fail_fast_on_rate_limit = os.getenv("PERSONA_FAIL_FAST_ON_429", "1").strip().lower() in {
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
                        {"role": "system", "content": PERSONA_SYSTEM_PROMPT},
                        {"role": "user", "content": user_message}
                    ],
                    temperature=0.3,
                    max_tokens=600
                )
            
            # Track token usage
            usage = response.usage
            if usage:
                if token_lock is not None:
                    with token_lock:
                        token_tracker["total"] = token_tracker.get("total", 0) + usage.total_tokens
                else:
                    token_tracker["total"] = token_tracker.get("total", 0) + usage.total_tokens
            
            # Credit warning
            if token_tracker["total"] > 80000:
                logger.warning("High cumulative token usage in current run.")
            
            # Parse response
            content = response.choices[0].message.content.strip()
            
            # Strip markdown fences
            content = content.replace("```json", "").replace("```", "").strip()
            
            # Try JSON parsing
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                try:
                    parsed = json5.loads(content)
                except Exception:
                    logger.info("Persona JSON parsing failed for this attempt; retrying.")
                    if attempt < retries:
                        continue
                    raise

            parsed = normalize_persona_payload(parsed)
            
            # Check for Anakin
            if contains_anakin(parsed):
                logger.info("'Anakin' found in persona output, retrying...")
                if attempt < effective_retries:
                    continue
            
            # Validate persona structure
            is_valid, error = validate_persona(parsed)
            if not is_valid:
                logger.info("Persona validation failed on attempt %s; retrying (%s)", attempt + 1, error)
                if attempt < effective_retries:
                    continue
            
            return parsed
            
        except Exception as e:
            rate_limited = is_rate_limited_error(e)
            if rate_limited and fail_fast_on_rate_limit:
                logger.warning(
                    "Persona LLM rate-limited on attempt %s; fail-fast enabled for this row.",
                    attempt + 1,
                )
                rate_limited_fail_fast = True
                break
            if attempt < effective_retries:
                delay = backoff_delay(attempt, e)
                if rate_limited:
                    logger.warning(
                        "Persona LLM rate-limited on attempt %s; retrying in %.2fs",
                        attempt + 1,
                        delay,
                    )
                else:
                    logger.warning(
                        "Persona LLM call failed on attempt %s; retrying in %.2fs (%s)",
                        attempt + 1,
                        delay,
                        e,
                    )
                time.sleep(delay)
            else:
                logger.error(f"Persona LLM call failed after retries: {e}")
                return {
                    "persona_archetype": "ERROR",
                    "persona_summary": "ERROR",
                    "context_summary": "ERROR",
                    "personalization_themes": ["ERROR"],
                    "relevance_score": 0,
                    "recommended_hook": "ERROR",
                    "assign_to": "Not Relevant",
                    "persona_reason": "ERROR",
                    "error": "PERSONA_GENERATION_FAILED_AFTER_RETRIES"
                }
    
    if rate_limited_fail_fast:
        return {
            "persona_archetype": "ERROR",
            "persona_summary": "ERROR",
            "context_summary": "ERROR",
            "personalization_themes": ["ERROR"],
            "relevance_score": 0,
            "recommended_hook": "ERROR",
            "assign_to": "Not Relevant",
            "persona_reason": "ERROR",
            "error": "PERSONA_RATE_LIMITED_FAIL_FAST",
        }

    return {
        "persona_archetype": "ERROR",
        "persona_summary": "ERROR",
        "context_summary": "ERROR",
        "personalization_themes": ["ERROR"],
        "relevance_score": 0,
        "recommended_hook": "ERROR",
        "assign_to": "Not Relevant",
        "persona_reason": "ERROR",
        "error": "PERSONA_GENERATION_FAILED_AFTER_RETRIES"
    }


def generate_personas(
    input_csv: str = "data/speakers_enriched.csv",
    output_csv: str = "data/speakers_personas.csv",
    delay: float = 0.0,
    fast_mode: bool = False
) -> pd.DataFrame:
    """
    Generate personas for all enriched contacts.
    
    Args:
        input_csv: Path to enriched CSV.
        output_csv: Path to output CSV.
        delay: Delay between LLM calls.
        
    Returns:
        DataFrame with personas.
    """
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    
    # Gate persona generation to rows that actually completed enrichment.
    if "enrichment_status" in df.columns:
        enriched_mask = df["enrichment_status"].fillna("").astype(str).str.upper().eq("ENRICHED")
    else:
        enriched_mask = pd.Series([True] * len(df), index=df.index)

    eligible_df = df[enriched_mask].copy()
    skip_not_enriched_df = df[~enriched_mask].copy()
    if "enrichment_confidence_score" not in eligible_df.columns:
        eligible_df["enrichment_confidence_score"] = 0

    # Split by confidence score for live mode.
    min_confidence = int(os.getenv("MIN_PERSONA_CONFIDENCE", "1"))
    if fast_mode:
        process_df = eligible_df.copy()
        skip_low_conf_df = eligible_df.iloc[0:0].copy()
        print(
            f"CACHED MODE: processing {len(process_df)} contacts "
            f"(skipping {len(skip_not_enriched_df)} not enriched) "
            "using existing enrichment data (no external persona API calls)."
        )
    else:
        process_df = eligible_df[eligible_df["enrichment_confidence_score"] >= min_confidence].copy()
        skip_low_conf_df = eligible_df[eligible_df["enrichment_confidence_score"] < min_confidence].copy()
        print(
            f"Processing {len(process_df)} contacts "
            f"(skipping {len(skip_low_conf_df)} below confidence {min_confidence}, "
            f"{len(skip_not_enriched_df)} not enriched)"
        )
    
    row_delay = max(0.0, float(os.getenv("PERSONA_ROW_DELAY_SEC", str(delay))))

    # Initialize client only in live mode.
    client: Optional[OpenAI]
    if fast_mode:
        client = None
    else:
        client = create_llm_client()
    model = get_llm_model()
    
    token_tracker = {"total": 0}
    token_lock = threading.Lock()
    generated_count = 0
    error_count = 0
    fallback_count = 0
    llm_disabled_due_rate_limit = False
    consecutive_rate_limit_fallbacks = 0
    disable_llm_on_rate_limit = os.getenv("PERSONA_DISABLE_LLM_ON_RATE_LIMIT", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    rate_limit_switch_threshold = max(1, int(os.getenv("PERSONA_RATE_LIMIT_SWITCH_THRESHOLD", "3")))

    persona_workers = 1 if fast_mode else max(1, int(os.getenv("PERSONA_MAX_WORKERS", "1")))
    if persona_workers > 1:
        print(f"LIVE MODE: persona parallelism enabled ({persona_workers} workers).")

    def _build_result_row(row_dict: Dict[str, Any], persona: Dict[str, Any]) -> Dict[str, Any]:
        result_row = dict(row_dict)
        for key in [
            "persona_archetype",
            "persona_summary",
            "context_summary",
            "relevance_score",
            "recommended_hook",
            "assign_to",
            "persona_reason",
        ]:
            result_row[key] = persona.get(key, "ERROR")

        themes = persona.get("personalization_themes", [])
        if isinstance(themes, list):
            result_row["personalization_themes"] = "|".join(str(t) for t in themes)
        else:
            result_row["personalization_themes"] = str(themes)

        result_row["persona_status"] = "GENERATED" if "error" not in persona else "ERROR"
        result_row["persona_source"] = str(
            persona.get("_source", "CACHED_RULE_BASED" if fast_mode else "LIVE_LLM")
        )
        fallback_reason = str(persona.get("_fallback_reason", "")).strip()
        if fallback_reason:
            result_row["llm_error"] = fallback_reason
        elif "error" in persona:
            result_row["llm_error"] = persona["error"]
        return result_row

    def _process_one(
        idx: int,
        row_dict: Dict[str, Any],
        force_rule_based: bool = False,
    ) -> Tuple[int, Dict[str, Any], bool, bool, bool]:
        try:
            if fast_mode:
                persona = build_persona_fast(row_dict)
                persona["_source"] = "CACHED_RULE_BASED"
                used_fallback = False
                rate_limited_fallback = False
            elif force_rule_based:
                persona = build_persona_fast(row_dict)
                persona["_source"] = "FALLBACK_RULE_BASED"
                persona["_fallback_reason"] = "GLOBAL_RATE_LIMIT_SWITCH"
                used_fallback = True
                rate_limited_fallback = False
            else:
                user_prompt = build_user_prompt(row_dict)
                persona = call_llm(user_prompt, client, model, token_tracker, token_lock=token_lock)
                if "error" in persona:
                    error_code = str(persona.get("error", "")).strip().upper()
                    logger.warning(
                        "Persona LLM hard-failed for row %s; using rule-based fallback.", idx
                    )
                    persona = build_persona_fast(row_dict)
                    persona["_source"] = "FALLBACK_RULE_BASED"
                    persona["_fallback_reason"] = error_code or "LLM_ERROR_RATE_LIMIT_OR_PARSE"
                    used_fallback = True
                    rate_limited_fallback = "RATE_LIMIT" in error_code
                else:
                    persona["_source"] = "LIVE_LLM"
                    used_fallback = False
                    rate_limited_fallback = False
            result_row = _build_result_row(row_dict, persona)
            had_error = "error" in persona
            return idx, result_row, had_error, used_fallback, rate_limited_fallback
        except Exception as exc:
            logger.error(f"Persona generation failed for row {idx}: {exc}")
            result_row = dict(row_dict)
            result_row["persona_status"] = "ERROR"
            result_row["llm_error"] = str(exc)
            result_row["persona_source"] = "ERROR"
            return idx, result_row, True, False, False

    # Process each row
    results: List[Dict[str, Any]] = []
    indexed_rows = [(idx, row.to_dict()) for idx, row in process_df.iterrows()]
    if persona_workers == 1:
        for idx, row_dict in indexed_rows:
            force_rule_based = (not fast_mode) and llm_disabled_due_rate_limit
            _idx, result_row, had_error, used_fallback, rate_limited_fallback = _process_one(
                idx,
                row_dict,
                force_rule_based=force_rule_based,
            )
            results.append(result_row)
            if had_error:
                error_count += 1
            else:
                generated_count += 1
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
                        "Persona LLM rate-limited for %s consecutive rows. "
                        "Switching remaining rows to rule-based fallback.",
                        consecutive_rate_limit_fallbacks,
                    )
            if (idx + 1) % 5 == 0:
                print(f"Generated {generated_count} personas...")
            if (not fast_mode) and row_delay > 0:
                time.sleep(row_delay)
    else:
        ordered_results: Dict[int, Tuple[Dict[str, Any], bool, bool, bool]] = {}
        completed = 0
        with ThreadPoolExecutor(max_workers=persona_workers) as executor:
            futures = {executor.submit(_process_one, idx, row_dict): idx for idx, row_dict in indexed_rows}
            for future in as_completed(futures):
                idx, result_row, had_error, used_fallback, rate_limited_fallback = future.result()
                ordered_results[idx] = (result_row, had_error, used_fallback, rate_limited_fallback)
                completed += 1
                if completed % 5 == 0:
                    print(f"Generated {completed}/{len(indexed_rows)} personas...")

        for idx, _row_dict in indexed_rows:
            result_row, had_error, used_fallback, _rate_limited_fallback = ordered_results[idx]
            results.append(result_row)
            if had_error:
                error_count += 1
            else:
                generated_count += 1
            if used_fallback:
                fallback_count += 1
    
    # Mark skipped rows.
    if not fast_mode:
        for _, row in skip_low_conf_df.iterrows():
            result_row = dict(row)
            result_row["persona_status"] = "SKIPPED_LOW_CONFIDENCE"
            result_row["assign_to"] = "Not Relevant"
            result_row["persona_archetype"] = "INSUFFICIENT_DATA"
            result_row["persona_summary"] = ""
            result_row["context_summary"] = ""
            result_row["personalization_themes"] = ""
            result_row["relevance_score"] = 0
            result_row["recommended_hook"] = ""
            result_row["persona_reason"] = "Low enrichment confidence"
            results.append(result_row)

    for _, row in skip_not_enriched_df.iterrows():
        result_row = dict(row)
        result_row["persona_status"] = "SKIPPED_NOT_ENRICHED"
        result_row["assign_to"] = "Not Relevant"
        result_row["persona_archetype"] = "INSUFFICIENT_DATA"
        result_row["persona_summary"] = ""
        result_row["context_summary"] = ""
        result_row["personalization_themes"] = ""
        result_row["relevance_score"] = 0
        result_row["recommended_hook"] = ""
        result_row["persona_reason"] = "Upstream enrichment_status is not ENRICHED"
        result_row["llm_error"] = "UPSTREAM_ENRICHMENT_NOT_COMPLETE"
        result_row["persona_source"] = "SKIPPED_UPSTREAM"
        results.append(result_row)
    
    # Print summary
    print(f"\n=== Persona Generation Summary ===")
    print(f"Generated: {generated_count}")
    print(f"Errors: {error_count}")
    print(f"Fallback used: {fallback_count}")
    if llm_disabled_due_rate_limit:
        print("Live LLM disabled mid-run due to repeated rate limits; switched to rule-based fallback.")
    print(f"Skipped (low confidence): {len(skip_low_conf_df)}")
    print(f"Skipped (not enriched): {len(skip_not_enriched_df)}")
    if fast_mode:
        print("Mode: cached (no external persona API calls).")
    else:
        print("Mode: live LLM persona generation.")
    
    # Write output
    result_df = pd.DataFrame(results)
    result_df.to_csv(output_csv, encoding="utf-8-sig", index=False)
    print(f"Output written to: {output_csv}")
    
    return result_df


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    generate_personas(fast_mode=os.getenv("FAST_MODE", "0") == "1")
