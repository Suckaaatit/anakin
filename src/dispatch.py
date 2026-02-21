"""
dispatch.py -- Optional outreach dispatch bridge.

Purpose:
- Convert approved outreach drafts into a send-ready queue.
- Support free-tier integration via webhook or file export.

Safety:
- No outbound send is executed unless explicitly configured.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

INPUT_CSV = Path("output/outreach_drafts.csv")
OUTPUT_JSONL = Path("output/dispatch_queue.jsonl")


def _approved_rows(df: pd.DataFrame) -> pd.DataFrame:
    approved_mask = df.get("outreach_approved", pd.Series(dtype=str)).astype(str).str.upper().eq("YES")
    route_mask = ~df.get("final_route", pd.Series(dtype=str)).astype(str).isin(["Not Relevant", "DUPLICATE"])
    return df[approved_mask & route_mask].copy()


def _payloads(df: pd.DataFrame) -> List[Dict[str, object]]:
    payloads: List[Dict[str, object]] = []
    for _, row in df.iterrows():
        payloads.append(
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "company": row.get("company"),
                "final_route": row.get("final_route"),
                "outreach_sequence": row.get("outreach_sequence"),
                "email_subject": row.get("email_subject_post_event") or row.get("email_subject_a"),
                "email_body": row.get("email_body_post_event"),
                "linkedin_note": row.get("linkedin_note"),
            }
        )
    return payloads


def _write_jsonl(payloads: List[Dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for item in payloads:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def _dispatch_webhook(payloads: List[Dict[str, object]], url: str) -> Dict[str, int]:
    sent = 0
    failed = 0
    max_retries = max(1, int(os.getenv("DISPATCH_MAX_RETRIES", "3")))
    timeout_sec = max(2.0, float(os.getenv("DISPATCH_TIMEOUT_SEC", "10")))

    for item in payloads:
        ok = False
        for attempt in range(max_retries):
            try:
                resp = requests.post(url, json=item, timeout=timeout_sec)
                if 200 <= resp.status_code < 300:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(min(2.0 * (attempt + 1), 6.0))
        if ok:
            sent += 1
        else:
            failed += 1
    return {"sent": sent, "failed": failed}


def run_dispatch(input_csv: Path = INPUT_CSV) -> int:
    if not input_csv.exists():
        print(f"Missing input: {input_csv}")
        return 1

    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    approved = _approved_rows(df)
    queue = _payloads(approved)

    mode = os.getenv("DISPATCH_MODE", "file").strip().lower()
    if mode == "webhook":
        url = os.getenv("DISPATCH_WEBHOOK_URL", "").strip()
        if not url:
            print("DISPATCH_MODE=webhook but DISPATCH_WEBHOOK_URL is empty.")
            return 2
        result = _dispatch_webhook(queue, url)
        print(f"Dispatch mode: webhook | approved={len(queue)} sent={result['sent']} failed={result['failed']}")
        return 0 if result["failed"] == 0 else 3

    _write_jsonl(queue, OUTPUT_JSONL)
    print(f"Dispatch mode: file | approved={len(queue)} exported={OUTPUT_JSONL}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_dispatch())
