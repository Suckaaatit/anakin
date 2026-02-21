# What Was Not Automated (and Why)

This document satisfies the assignment requirement to clearly state what is intentionally manual.

## 1) Actual Email Sending

Not automated:
- Sending emails to external contacts

Why:
- Consent/compliance risk for unsolicited sends
- Cold-domain deliverability risk without SPF/DKIM/DMARC and warm-up

What is automated instead:
- Draft generation and sequencing
- Manual approval gate before any send
- Optional dispatch bridge (`src/dispatch.py`) to export approved rows to JSONL or webhook for downstream sender tools

## 2) Actual LinkedIn Sending

Not automated:
- Auto-sending LinkedIn connection requests/messages

Why:
- LinkedIn anti-automation and ToS risk
- No free compliant API path for this use case

What is automated instead:
- Personalized LinkedIn notes are generated and length-limited
- Manual send by SDR/AE

## 3) Deep Multi-Role Employment Timelines

Not fully automated:
- Full historical role-by-role timeline extraction for every profile

Why:
- Free compliant data sources do not reliably expose complete professional history
- Profile scraping at scale is fragile and policy-sensitive

What is automated instead:
- `job_history` snapshot generated for each contact
- includes current role plus inferred prior role when confidence is sufficient

## 4) Verified Inbox Ownership for Every Email

Not automated:
- Guaranteed inbox verification at scale for all guessed patterns

Why:
- Free tiers are insufficient for assignment volume
- hard verification APIs are usually paid

What is automated instead:
- rule-based pattern inference (`email_pattern`)
- explicit confidence labeling (`GUESSED` / `NOT_AVAILABLE`)

## Summary

Automated:
- Data enrichment
- Persona/context generation
- Routing and deduplication
- Outreach drafting
- Approved-row dispatch handoff (JSONL/webhook bridge)

Manual by design:
- Final outbound delivery actions and approval
