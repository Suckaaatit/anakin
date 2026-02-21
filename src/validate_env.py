"""
validate_env.py — Pre-flight checks for TechSparks GTM Automation Pipeline.

Run before any pipeline stage. Raises SystemExit with clear human-readable
message if any prerequisite fails.
"""

import os
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd


def check_required_env_vars(fast_mode: bool = False) -> List[str]:
    """
    CHECK 1 — Required environment variables.
    
    Provider-aware checks:
    - mistral: MISTRAL_API_KEY, MISTRAL_BASE_URL, LLM_MODEL
    - ollama: OLLAMA_BASE_URL + model (OLLAMA_MODEL or LLM_MODEL)
    
    Returns:
        List of missing/invalid variable names.
    """
    if fast_mode:
        return []

    provider = os.getenv("LLM_PROVIDER", "mistral").strip().lower()
    missing: List[str] = []

    if provider not in {"mistral", "ollama"}:
        return [f"LLM_PROVIDER unsupported: {provider} (use 'mistral' or 'ollama')"]

    if provider == "ollama":
        base_url = os.getenv("OLLAMA_BASE_URL")
        if not base_url:
            missing.append("OLLAMA_BASE_URL")
        model = os.getenv("OLLAMA_MODEL") or os.getenv("LLM_MODEL")
        if not model:
            missing.append("OLLAMA_MODEL (or LLM_MODEL)")
        return missing

    # Default: mistral-compatible provider config.
    required = ["MISTRAL_API_KEY", "MISTRAL_BASE_URL", "LLM_MODEL"]
    for var in required:
        value = os.getenv(var)
        if not value:
            missing.append(var)
        elif var == "MISTRAL_API_KEY" and len(value) < 20:
            missing.append(f"{var} (too short — check console.mistral.ai)")
    return missing


def check_speakers_csv_exists() -> bool:
    """
    CHECK 2 — speakers_raw.csv exists.
    
    Path: data/speakers_raw.csv
    
    Returns:
        True if file exists, False otherwise.
    """
    csv_path = Path("data/speakers_raw.csv")
    return csv_path.exists()


def check_required_columns(df: pd.DataFrame) -> List[str]:
    """
    CHECK 3 — Required CSV columns.
    
    Required columns: ["id", "name", "title", "company", "event_role", "session_topic", "source_url"]
    
    Args:
        df: DataFrame read from CSV.
        
    Returns:
        List of missing column names.
    """
    required = ["id", "name", "title", "company", "event_role", "session_topic", "source_url"]
    missing = [col for col in required if col not in df.columns]
    return missing


def check_blank_names(df: pd.DataFrame) -> List[tuple]:
    """
    CHECK 4 — No blank name or company values.
    
    For each row: if name has fewer than 2 words after strip, log a WARNING.
    Do not abort — just collect warnings.
    
    Args:
        df: DataFrame read from CSV.
        
    Returns:
        List of tuples (row_number, name_value) for problematic rows.
    """
    warnings: List[tuple] = []
    for idx, row in df.iterrows():
        name = str(row.get("name", "")).strip()
        word_count = len(name.split())
        if word_count < 2:
            warnings.append((idx + 2, name))  # +2 for 1-based and header
    return warnings


def check_contact_volume(df: pd.DataFrame, minimum: int = 150) -> Optional[str]:
    """Return warning text if contact volume is below assignment minimum."""
    count = len(df)
    if count < minimum:
        return (
            f"Dataset has {count} contacts; assignment expects at least {minimum}. "
            "Run: python src/scrape_techsparks_contacts.py --target 180"
        )
    return None


def create_output_directories() -> None:
    """
    CHECK 5 — Create output directories.
    
    Create output/ and data/cache/ with mkdir exist_ok=True.
    No error if already exists.
    """
    Path("output").mkdir(exist_ok=True)
    Path("data/cache").mkdir(exist_ok=True, parents=True)


def validate(fast_mode: bool = False) -> bool:
    """
    Run all validation checks in order.
    
    Returns:
        True if all checks pass.
        
    Raises:
        SystemExit: If any check fails, with appropriate error message.
    """
    # CHECK 1 — Required environment variables
    missing_vars = check_required_env_vars(fast_mode=fast_mode)
    if missing_vars:
        print("ERROR: Missing or invalid environment variables:")
        for i, var in enumerate(missing_vars, 1):
            print(f"  {i}. {var}")
        print("\nPlease set these variables in your .env file and try again.")
        sys.exit(1)
    
    # CHECK 2 — speakers_raw.csv exists
    if not check_speakers_csv_exists():
        print("ERROR: data/speakers_raw.csv not found.")
        print("Run: python src/scrape_techsparks_contacts.py --target 180")
        sys.exit(1)
    
    # CHECK 3 — Required CSV columns
    try:
        df = pd.read_csv("data/speakers_raw.csv", encoding="utf-8-sig")
    except Exception as e:
        print(f"ERROR: Could not read data/speakers_raw.csv: {e}")
        sys.exit(1)
    
    missing_cols = check_required_columns(df)
    if missing_cols:
        print("ERROR: Missing required columns in speakers_raw.csv:")
        for col in missing_cols:
            print(f"  - {col}")
        sys.exit(1)
    
    # CHECK 4 — No blank name or company values (WARNING only)
    name_warnings = check_blank_names(df)
    if name_warnings:
        print("WARNING: The following rows have single-word names (may cause issues):")
        for row_num, name in name_warnings:
            print(f"  Row {row_num}: '{name}'")
        print("Proceeding with pipeline...\n")

    # CHECK 4b — Assignment-scale minimum volume (WARNING only)
    volume_warning = check_contact_volume(df)
    if volume_warning:
        print(f"WARNING: {volume_warning}\n")
    
    # CHECK 5 — Create output directories
    create_output_directories()
    
    print("[STARTUP OK] All prerequisites validated.")
    return True


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    is_fast_mode = os.getenv("FAST_MODE", "0") == "1"
    validate(fast_mode=is_fast_mode)
