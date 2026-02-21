"""
pipeline.py -- Main orchestration module for TechSparks GTM Automation.

Coordinates the full pipeline: validate -> enrich -> persona -> route -> outreach.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

from validate_env import validate
from enrich import run_enrichment
from persona import generate_personas
from route import run_routing
from outreach import generate_outreach_drafts
from scrape_techsparks_contacts import scrape_contacts

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # Start each pipeline run with a fresh log file so stale historical errors
        # do not appear as current failures in the UI.
        logging.FileHandler("data/errors.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

STAGE_ORDER = ["enrich", "persona", "route", "outreach"]
STATUS_OK = "[OK]"
STATUS_FAIL = "[FAIL]"
RAW_INPUT_CSV = "data/speakers_raw.csv"
DEFAULT_SCRAPE_TARGET = 180

BASE_STAGE_CONFIG = {
    "enrich": {
        "function": run_enrichment,
        "input": RAW_INPUT_CSV,
        "output": "data/speakers_enriched.csv"
    },
    "persona": {
        "function": generate_personas,
        "input": "data/speakers_enriched.csv",
        "output": "data/speakers_personas.csv"
    },
    "route": {
        "function": run_routing,
        "input": "data/speakers_personas.csv",
        "output": "data/speakers_routed.csv",
        "handoff": "output/make_handoff.csv",
    },
    "outreach": {
        "function": generate_outreach_drafts,
        "input": "data/speakers_routed.csv",
        "output": "output/outreach_drafts.csv"
    }
}


def _build_stage_config(test_mode: bool) -> dict:
    """Return stage config for normal vs isolated test runs."""
    if not test_mode:
        return {name: dict(config) for name, config in BASE_STAGE_CONFIG.items()}

    test_data_dir = Path("data/test")
    test_output_dir = Path("output/test")
    test_data_dir.mkdir(parents=True, exist_ok=True)
    test_output_dir.mkdir(parents=True, exist_ok=True)

    enrich_out = (test_data_dir / "speakers_enriched_test.csv").as_posix()
    persona_out = (test_data_dir / "speakers_personas_test.csv").as_posix()
    route_out = (test_data_dir / "speakers_routed_test.csv").as_posix()
    handoff_out = (test_output_dir / "make_handoff_test.csv").as_posix()
    outreach_out = (test_output_dir / "outreach_drafts_test.csv").as_posix()

    return {
        "enrich": {
            "function": run_enrichment,
            "input": RAW_INPUT_CSV,
            "output": enrich_out,
        },
        "persona": {
            "function": generate_personas,
            "input": enrich_out,
            "output": persona_out,
        },
        "route": {
            "function": run_routing,
            "input": persona_out,
            "output": route_out,
            "handoff": handoff_out,
        },
        "outreach": {
            "function": generate_outreach_drafts,
            "input": route_out,
            "output": outreach_out,
        },
    }


def _csv_row_count(path: Path) -> int:
    """Count data rows in CSV (header excluded)."""
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as handle:
        next(handle, None)  # header
        return sum(1 for _ in handle)


def _validate_stage_input(
    stage: str,
    input_path: Path,
    run_started_epoch: float,
    allow_stale_first_stage: bool,
    max_age_minutes: int,
    enforce_age_for_first_stage: bool = True,
) -> tuple[bool, str]:
    """
    Validate that stage input exists, is non-empty, and is fresh enough.

    - For first stage in resume mode, allow older input up to max_age_minutes.
    - For downstream stages in same run, require input file mtime >= run start.
    """
    if not input_path.exists():
        return False, f"{stage}: missing required input file: {input_path}"

    try:
        rows = _csv_row_count(input_path)
    except Exception as exc:
        return False, f"{stage}: could not read input CSV {input_path}: {exc}"

    if rows <= 0:
        return False, f"{stage}: input CSV has zero rows: {input_path}"

    try:
        modified_epoch = input_path.stat().st_mtime
    except Exception as exc:
        return False, f"{stage}: could not read input timestamp for {input_path}: {exc}"

    if allow_stale_first_stage:
        if not enforce_age_for_first_stage:
            return True, f"{stage}: source input validated ({rows} rows)."
        age_minutes = max(0.0, (time.time() - modified_epoch) / 60.0)
        if age_minutes > float(max_age_minutes):
            return (
                False,
                (
                    f"{stage}: input appears stale ({age_minutes:.1f}m old > "
                    f"{max_age_minutes}m). Re-run upstream stage or increase "
                    "PIPELINE_INPUT_MAX_AGE_MINUTES."
                ),
            )
        return True, f"{stage}: resume input validated ({rows} rows, age {age_minutes:.1f}m)."

    if modified_epoch < run_started_epoch:
        modified_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(modified_epoch))
        return (
            False,
            (
                f"{stage}: input file ({input_path}) predates this pipeline run "
                f"(mtime {modified_text}). Aborting to avoid stale downstream processing."
            ),
        )

    return True, f"{stage}: input validated ({rows} rows, fresh for current run)."


def _configure_stdout_encoding() -> None:
    """
    Configure UTF-8 console output when possible.

    This prevents Windows cp1252 terminals from crashing on non-ASCII content
    emitted by dependencies.
    """
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                # Keep default stream settings if reconfigure is unavailable.
                pass


def _safe_print(message: str = "") -> None:
    """Print text without crashing if terminal encoding is restrictive."""
    try:
        print(message)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "utf-8"
        sanitized = message.encode(encoding, errors="replace").decode(encoding, errors="replace")
        print(sanitized)


def _should_scrape_before_run(
    *,
    test_mode: bool,
    from_stage: Optional[str],
    scrape_before_run: bool,
) -> bool:
    """Scrape before full runs that begin at enrich/start."""
    if not scrape_before_run:
        return False
    return from_stage in (None, "enrich")


def _refresh_raw_contacts(target: int, output_csv: str = RAW_INPUT_CSV) -> int:
    """Scrape public TechSparks pages and write raw contact seed CSV."""
    normalized_target = max(150, min(int(target), 200))
    df = scrape_contacts(target=normalized_target)
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, encoding="utf-8-sig", index=False)
    return int(len(df))


def run_pipeline(
    test_mode: bool = False,
    from_stage: Optional[str] = None,
    fast_mode: bool = False,
    scrape_before_run: bool = True,
    scrape_target: int = DEFAULT_SCRAPE_TARGET,
) -> None:
    """
    Run the complete GTM automation pipeline.
    
    Args:
        test_mode: If True, limit enrich stage to 10 rows.
        from_stage: Resume from this stage (skip earlier stages).
    """
    _safe_print("=" * 60)
    _safe_print("TechSparks GTM Automation Pipeline")
    _safe_print(f"Execution mode: {'FAST (no external calls)' if fast_mode else 'LIVE (Brave/DDG + LLM)'}")
    _safe_print("=" * 60)
    if test_mode:
        _safe_print("Test mode artifact isolation: enabled (data/test + output/test).")

    if _should_scrape_before_run(
        test_mode=test_mode,
        from_stage=from_stage,
        scrape_before_run=scrape_before_run,
    ):
        _safe_print("\n[0/5] Scraping public TechSparks contacts...")
        try:
            scraped = _refresh_raw_contacts(target=scrape_target, output_csv=RAW_INPUT_CSV)
            _safe_print(f"  {STATUS_OK} Scraped {scraped} contacts into {RAW_INPUT_CSV}")
        except Exception as exc:
            logger.error(f"Scrape step failed: {exc}")
            _safe_print(f"  {STATUS_FAIL} scrape failed: {exc}")
            _safe_print("  Aborting before enrichment because assignment requires scrape-first ingestion.")
            sys.exit(1)
    elif not scrape_before_run:
        _safe_print("\n[0/5] Scrape step skipped by --skip-scrape.")
    elif from_stage and from_stage != "enrich":
        _safe_print(f"\n[0/5] Resume mode from '{from_stage}': scrape step skipped.")
    
    # Always validate first
    _safe_print("\n[1/5] Validating environment...")
    try:
        validate(fast_mode=fast_mode)
    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)
    
    run_started_epoch = time.time()
    fail_fast = os.getenv("PIPELINE_FAIL_FAST", "1") == "1"
    enforce_stage_input_guard = os.getenv("PIPELINE_STAGE_INPUT_GUARD", "1") == "1"
    max_input_age_minutes = int(os.getenv("PIPELINE_INPUT_MAX_AGE_MINUTES", "180"))
    stage_config = _build_stage_config(test_mode=test_mode)

    # Determine starting stage
    start_idx = 0
    if from_stage:
        if from_stage not in STAGE_ORDER:
            _safe_print(f"ERROR: Invalid stage '{from_stage}'. Valid stages: {', '.join(STAGE_ORDER)}")
            sys.exit(1)
        start_idx = STAGE_ORDER.index(from_stage)
        _safe_print(f"\nResuming from stage: {from_stage}")
    
    stages_to_run = STAGE_ORDER[start_idx:]
    failed_stages: List[str] = []
    
    # Run each stage
    for i, stage in enumerate(stages_to_run, start=start_idx + 1):
        config = stage_config[stage]
        _safe_print(f"\n[{i}/5] Running {stage}...")
        _safe_print(f"  Input: {config['input']}")
        _safe_print(f"  Output: {config['output']}")

        if enforce_stage_input_guard:
            is_first_stage_in_this_run = stage == stages_to_run[0]
            allow_stale_first_stage = bool(from_stage) and is_first_stage_in_this_run
            # For full runs starting at enrich, raw seed data can be pre-existing.
            if not from_stage and stage == "enrich" and is_first_stage_in_this_run:
                allow_stale_first_stage = True
            input_path = Path(config["input"])
            healthy, reason = _validate_stage_input(
                stage=stage,
                input_path=input_path,
                run_started_epoch=run_started_epoch,
                allow_stale_first_stage=allow_stale_first_stage,
                max_age_minutes=max_input_age_minutes,
                enforce_age_for_first_stage=bool(from_stage),
            )
            if healthy:
                _safe_print(f"  {STATUS_OK} {reason}")
            else:
                _safe_print(f"  {STATUS_FAIL} {reason}")
                logger.error(reason)
                failed_stages.append(stage)
                if fail_fast:
                    _safe_print("  Aborting remaining stages (PIPELINE_FAIL_FAST=1).")
                    break
                _safe_print("  Continuing to next stage (PIPELINE_FAIL_FAST=0).")
                continue
        
        try:
            if stage == "enrich":
                config["function"](
                    input_csv=config["input"],
                    output_csv=config["output"],
                    limit=10 if test_mode else None,
                    fast_mode=fast_mode,
                )
            elif stage == "persona":
                config["function"](
                    input_csv=config["input"],
                    output_csv=config["output"],
                    fast_mode=fast_mode,
                )
            elif stage == "route":
                config["function"](
                    input_csv=config["input"],
                    output_csv=config["output"],
                    handoff_csv=config.get("handoff", "output/make_handoff.csv"),
                )
            elif stage == "outreach":
                config["function"](
                    input_csv=config["input"],
                    output_csv=config["output"],
                    fast_mode=fast_mode,
                )
            else:
                config["function"]()
            _safe_print(f"  {STATUS_OK} {stage} completed successfully")
        except Exception as e:
            logger.error(f"Stage '{stage}' failed: {e}")
            _safe_print(f"  {STATUS_FAIL} {stage} failed: {e}")
            failed_stages.append(stage)
            if fail_fast:
                _safe_print("  Aborting remaining stages (PIPELINE_FAIL_FAST=1).")
                break
    
    # Print summary
    _safe_print("\n" + "=" * 60)
    _safe_print("Pipeline Summary")
    _safe_print("=" * 60)

    if failed_stages:
        _safe_print(f"Failed stages: {', '.join(failed_stages)}")
        _safe_print("Check data/errors.log for details")
    else:
        _safe_print("All stages completed successfully!")

    _safe_print(f"\nFinal output: {stage_config['outreach']['output']}")
    
    if failed_stages:
        sys.exit(1)


def main() -> None:
    """Parse CLI arguments and run pipeline."""
    _configure_stdout_encoding()
    parser = argparse.ArgumentParser(
        description="TechSparks GTM Automation Pipeline"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode (limit to 10 contacts)"
    )
    parser.add_argument(
        "--from",
        dest="from_stage",
        choices=STAGE_ORDER,
        help="Resume from this stage (skip earlier stages)"
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Use cached no-token mode (skips external DDG + LLM calls)"
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Use live DDG + LLM calls (slow, token-consuming)"
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip scrape-first ingestion and use existing data/speakers_raw.csv.",
    )
    parser.add_argument(
        "--scrape-target",
        type=int,
        default=DEFAULT_SCRAPE_TARGET,
        help=f"Target contact count for scrape-first ingestion ({DEFAULT_SCRAPE_TARGET} default; clamped to 150-200).",
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()

    fast_mode_default = os.getenv("FAST_MODE", "1") == "1"
    if args.live:
        fast_mode = False
    elif args.fast:
        fast_mode = True
    else:
        fast_mode = fast_mode_default
    
    run_pipeline(
        test_mode=args.test,
        from_stage=args.from_stage,
        fast_mode=fast_mode,
        scrape_before_run=not args.skip_scrape,
        scrape_target=args.scrape_target,
    )


if __name__ == "__main__":
    main()
