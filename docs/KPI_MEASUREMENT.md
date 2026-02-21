# KPI Measurement Guide

This document closes the assignment requirement for key observations on:
- email deliverability
- LinkedIn acceptance rate
- message customization depth
- send readiness score
- spam risk score

## 1) Generate KPI Report From Current Drafts

Run:

```bash
python src/performance_report.py
```

Outputs:
- `output/performance_observations.json`
- `output/performance_observations.md`

If no campaign outcome file exists, the report uses **proxy readiness metrics** (clearly labeled).
The generated JSON now includes an `assignment_metrics` section with:
- `email_deliverability_rate_pct`
- `linkedin_acceptance_rate_pct`
- `expected_acceptance_rate_pct`
- `send_readiness_score_pct`
- `spam_risk_score_pct`

## 2) Add Observed Delivery / Acceptance Outcomes

Create a tracking file:

```bash
copy data\campaign_tracking_template.csv data\campaign_tracking.csv
```

Fill `data/campaign_tracking.csv` with one row per attempted outreach:
- `email_sent`: `TRUE/FALSE`
- `email_delivered`: `TRUE/FALSE`
- `email_bounced`: `TRUE/FALSE`
- `linkedin_sent`: `TRUE/FALSE`
- `linkedin_accepted`: `TRUE/FALSE`

Then rerun:

```bash
python src/performance_report.py --tracking-csv data/campaign_tracking.csv
```

Now the report switches to **observed** metrics when sent counts are non-zero.

## 3) Metric Definitions

- Email deliverability rate:
  - observed: `email_delivered / email_sent`
  - fallback: if delivered unavailable but bounce available, treated as `(not bounced)`

- LinkedIn acceptance rate:
  - observed: `linkedin_accepted / linkedin_sent`

- Message customization depth:
  - `name_mention_pct`
  - `company_mention_pct`
  - `session_topic_mention_pct`
  - `subject_unique_rate_pct`
  - `persona_theme_nonempty_pct`
  - combined into `customization_depth_score_pct`

## 4) Guardrails Checked in Report

- Subject line rule pass (`<= 8` words and no `?`)
- Mandatory YC intro line presence in generated email bodies
- LinkedIn note `< 300` chars
- Forbidden term (`Anakin`) violations
- Assignment-level send readiness and spam risk scoring
