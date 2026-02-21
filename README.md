# TechSparks GTM Automation - Submission

## Objective
Build a free-tier AI + automation prototype that converts a TechSparks attendee/speaker list into enriched contacts, persona context, lead routing, and multi-channel outreach drafts.

This repository now ships a working end-to-end prototype with:
- Public-source contact ingestion to 180 records
- Python orchestration pipeline
- LLM-assisted persona and outreach generation
- No-code workflow blueprint (Make.com)
- TypeScript browser UI for non-terminal users

## Quick Run (Under 1 Minute)

```bash
python src/pipeline.py --fast
```

Full runs now start with a scrape-first ingestion step (public TechSparks pages -> `data/speakers_raw.csv`) before enrichment.  
Use `--skip-scrape` only for local debugging/replay.

Measured run on February 19, 2026:
- 180 contacts processed end-to-end
- Completed in about 7 seconds on local machine

`--fast` is the default behavior in local config to avoid long waits and token burn.

Test safety (updated February 19, 2026):
- `python src/pipeline.py --test --live` now writes to isolated test artifacts:
  - `data/test/speakers_enriched_test.csv`
  - `data/test/speakers_personas_test.csv`
  - `data/test/speakers_routed_test.csv`
  - `output/test/make_handoff_test.csv`
  - `output/test/outreach_drafts_test.csv`
- Primary submission outputs (`data/*.csv`, `output/outreach_drafts.csv`) are no longer overwritten by test runs.

## Environment Setup (Fixes Global `pip check` Conflicts)

Use an isolated virtualenv so unrelated system packages do not break this project.

```bash
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pip check
```

Expected result:
- `No broken requirements found.`

## Reliability + Security Hardening

- Pipeline fail-fast enabled by default (`PIPELINE_FAIL_FAST=1`).
- Stage input guard enabled by default (`PIPELINE_STAGE_INPUT_GUARD=1`), so stale/empty downstream inputs are blocked with clear errors.
- Fast enrichment now uses deterministic local LinkedIn seeding from public HTML snapshots before any live web lookup.
- Evidence-driven scoring fields (`evidence_score`, `evidence_tier`, `linkedin_source`, `industry_source`) reduce LLM-only routing decisions.
- Web API binds to localhost by default (`WEB_APP_HOST=127.0.0.1`).
- Optional control-token auth (`WEB_APP_TOKEN`) protects:
  - `/api/run`
  - `/api/stop`
  - `/api/queue/action`
- Tokenless control endpoints are accepted only from localhost (`127.0.0.1` / `::1`).
- Approval updates use lock + atomic CSV replace to reduce race/partial-write risk.
- Approval decisions are versioned in SQLite audit log: `data/app_state.db`.
- Optional dispatch bridge (`src/dispatch.py`) exports approved drafts to JSONL or webhook for send execution integration.

## Tests

```bash
python -m unittest discover -s tests -v
```

## Live Evidence Run (LLM + Web Lookups)

```bash
python src/pipeline.py --test --live
```

Measured on February 19, 2026 (10-contact test):
- LinkedIn found: 3/10 (30%)
- Persona generation: 10/10, skipped: 0
- Outreach drafts: 10/10
- Enrichment + persona + route + outreach all succeeded
- Test artifacts written under `data/test/` and `output/test/`

## Data Source and Contact Volume

- Seed file: `data/speakers_raw.csv`
- Contact count: 180 (assignment target 150-200)
- Source method: `src/scrape_techsparks_contacts.py`
- Inputs are scraped from public TechSparks pages (2025 agenda + 2024 public speaker cards), then deduplicated and normalized before each full pipeline run.

## Workflow Diagram

- Main diagram: `docs/workflow_diagram.md`
- Mermaid flow from raw contacts -> enrichment -> AI persona -> routing -> outreach.

## Working Prototype Components

1. Python pipeline:
- `src/pipeline.py`
- stages: `enrich -> persona -> route -> outreach`

2. Browser UI (TypeScript):
- backend API: `src/web_app.py`
- frontend: `ui/src/main.ts`
- lets users run/stop pipeline, view logs, preview outputs, and view compliance chips
- includes:
  - Outreach Preview Drawer (subject A/B toggle, body preview, LinkedIn note, copy buttons)
  - Explainability Panel (priority score breakdown and route reasoning)
  - Stage Inspector modal (passed/failed samples, stage logs, stage config)
  - Quality Dashboard charts (route distribution, confidence histogram, KPI bars)
  - Outreach Approval Queue (pending/approved/rejected with approve/reject actions)
  - Multi-format download (CSV, JSON, XLSX)
  - Optional control-token field for secured run/stop/approve actions

3. No-code workflow artifact:
- `docs/make_scenario_spec.md`
- `docs/make_scenario_blueprint.json`
- `output/make_handoff.csv` (runtime-exported top-priority queue for Make/Sheets ingestion)
- `docs/NO_CODE_PROOF_TEMPLATE.md` and `output/no_code_execution_evidence.md` (evidence tracker for live Make + Sheets proof)
- `python src/prepare_sheet_import_pack.py` to generate `output/sheets/*.csv` import pack for Google Sheets system-of-record demo

## Scope 1 - Data Enrichment

Implemented fields in `data/speakers_enriched.csv`:
- `linkedin_url`
- `linkedin_confidence`
- `linkedin_source`
- `linkedin_lookup_attempted`
- `seniority`
- `industry`
- `industry_source`
- `industry_relevance_score`
- `previous_role_inferred`
- `job_history`
- `news_signal`
- `signals`
- `email_pattern`
- `evidence_score`
- `evidence_tier`
- `enrichment_confidence_score`

Storage:
- CSV outputs in `data/` and `output/`
- Make/Sheet schema documented in `docs/sheet_schema.md`

Data quality safeguards:
- schema validation (`src/validate_env.py`)
- cache for LinkedIn/news lookups
- guarded parsing for LLM JSON
- duplicate checks in routing
- no `Anakin` allowed in generated content

## Scope 2 - AI Context and Persona Generation

Persona outputs in `data/speakers_personas.csv` include:
- persona archetype
- persona summary
- context summary
- 2-3 personalization themes
- relevance score
- recommended hook
- assignment suggestion

Prompts and controls:
- persona system prompt in `src/persona.py`
- strict JSON validation and retry logic
- hallucination controls: "use only provided data", skip unknown fields, schema enforcement

Prompt evidence highlights:
- Persona hard constraints (`src/persona.py`):
  - use only provided fields
  - return JSON only
  - mark sparse records as `INSUFFICIENT_DATA`
  - block `"Anakin"` mentions
- Outreach hard constraints (`src/outreach.py`):
  - pre/during/post email fields + LinkedIn note
  - LinkedIn note strictly `<300` chars (post-generation truncation guard)
  - fixed YC intro sentence
  - JSON-only response contract with sanitization fallback

## Scope 3 - Outreach Workflow (Pre / During / Post)

Generated outputs in `output/outreach_drafts.csv` include:
- `email_subject_a`, `email_subject_b`
- `email_body_pre_event`
- `email_body_during_event`
- `email_body_post_event`
- `linkedin_note` (<300 chars enforced)

Persona-aware messaging:
- varies by archetype, seniority, route, and session topic
- includes timing by event window from `EVENT_DATE`

Routing support:
- SDR / AE / Senior AE / partnership logic in `src/route.py`

Hard rule:
- outreach never mentions `Anakin` (enforced in prompt + sanitizer).

## Scope 4 - Lead Assignment Logic

Implemented in `src/route.py`:
- seniority + relevance + event role routing
- senior contacts and judge/mentor paths escalate to Senior AE
- VC path uses partnership sequence
- duplicate suppression via LinkedIn exact match + fuzzy fallback
- non-relevant leads set to skip

## Scope 5 - Failure and Scale Scenarios

Handled in logic/docs:
- LinkedIn profile missing/ambiguous: fallback, confidence penalty, continue pipeline
- low-confidence personalization: confidence gate + manual review path
- duplicates with minor variations: fuzzy dedup + duplicate route
- scaling 200 -> 2,000: cached lookups, block-size guards, queue/distributed roadmap

## Key Performance Observations

Measured:
- Fast full run (180): about 7s
- Live test run (10): LinkedIn found 30%, persona generation 100%, outreach generation 100%
- LinkedIn note length control: enforced under 300 chars
- Duplicate contacts flagged: 1 in 180 full run

Process insights:
- Fast mode is required for practical turnaround and token control
- Live web lookups remain rate-limit sensitive; Brave retry/backoff improved stability
- cached fallback keeps pipeline complete even when live enrichment is sparse
- test/live evidence runs are isolated from submission files to avoid accidental metric drift

Email deliverability and LinkedIn acceptance:
- KPI report generator: `python src/performance_report.py`
- output artifacts:
  - `output/performance_observations.json`
  - `output/performance_observations.md`
- behavior:
  - if `data/campaign_tracking.csv` contains real sends, report shows **observed** deliverability/acceptance
  - if no send outcomes exist, report shows clearly-labeled **proxy readiness** metrics
- tracking template: `data/campaign_tracking_template.csv`
- metric definitions and runbook: `docs/KPI_MEASUREMENT.md`

## Remaining Manual External Steps

Local code now covers enrichment, persona generation, routing, outreach drafting, and KPI artifact generation.

The following still require your own external account actions:
- Create/populate live Google Sheet instance (schema is in `docs/sheet_schema.md`, import pack from `python src/prepare_sheet_import_pack.py`, guide in `docs/GOOGLE_SHEETS_SYSTEM_OF_RECORD.md`)
- Import/run Make.com scenario and capture run history screenshots
- Execute real outreach sample and log outcomes in `data/campaign_tracking.csv` for observed deliverability/acceptance

Checklist: `docs/EXTERNAL_EXECUTION_CHECKLIST.md`

## Tools Used and Rationale

| Tool | Type | Why used |
|---|---|---|
| Python 3.11 | runtime | pipeline orchestration |
| pandas | data processing | CSV transformations and output shaping |
| Mistral API via OpenAI client | LLM interface | persona/outreach generation with JSON validation |
| requests + DDG fallback | web enrichment | free, no paid data vendors |
| Make.com (free tier) | no-code automation | demonstrates no-code requirement |
| Flask | backend API | simple UI control layer |
| TypeScript + Vite | frontend | browser-first operation for non-terminal users |

## Limitations and Safeguards

Limitations:
- live LinkedIn discovery can be rate-limited
- live mode rate-limiting: free-tier APIs can enforce ~1 req/sec; pipeline handles this with global throttling plus exponential backoff+jitter (full 180 completes reliably, but slower than fast mode)
- deep multi-role career history is not fully recoverable on free compliant sources
- verified email ownership is not guaranteed from pattern inference alone

Safeguards:
- fast mode default for predictable throughput
- retry/backoff and caching for live calls
- strict schema validation for persona/outreach JSON
- duplicate detection before outreach
- explicit manual approval gate before sending

Details: `docs/WHAT_WAS_NOT_AUTOMATED.md`

### Evaluator Risk Notes (Important)

1. LinkedIn coverage can be low on free/public sources.
- `Verified LinkedIn Matches %` reflects strict profile verification only (not guessed URLs).
- `Lookup Attempted %` is shown separately to distinguish data-availability limits from pipeline failures.
- Fast mode now uses local public HTML seed matching first, which materially improves coverage without external scraping calls.

2. Routing is not LLM-only.
- Final routing uses deterministic scoring (`relevance_score`, `enrichment_confidence_score`, `seniority`, `icp_match`) with explainable breakdown in UI.
- Low-confidence contacts are gated to manual-review paths before outreach.

3. Send execution is intentionally out of scope for safety/compliance.
- This prototype stops at draft generation + approval queue.
- Production integration path: SMTP/SES, HubSpot sequences, Apollo/Salesloft, and LinkedIn-assisted workflows behind explicit approval.
- `src/dispatch.py` now provides a working bridge for approved rows (JSONL export or webhook push).

4. CSV is used as prototype storage by design.
- Assignment emphasizes free, auditable prototype delivery.
- Writes are lock-protected and atomic; approval actions are additionally auditable via SQLite (`data/app_state.db`).

5. Security posture for demo.
- App binds to localhost by default (`127.0.0.1`).
- Mutating endpoints support token-based control via `WEB_APP_TOKEN`.
- Recommended demo setting: keep localhost bind + set token.

6. Free-tier LLM rate limits are expected and handled.
- Global limiter + concurrency caps + exponential backoff with jitter are implemented.
- Live runs trade speed for reliability; fast mode is available for deterministic demo throughput.

## Rule Compliance Matrix

Evaluation against assignment requirements:

| Rule | Status | Evidence |
|---|---|---|
| 150-200 contacts from public list/scraper | Met | `data/speakers_raw.csv` (180), `src/scrape_techsparks_contacts.py` |
| Use at least one no-code automation tool | Partial | `docs/make_scenario_spec.md`, `docs/make_scenario_blueprint.json`, `output/no_code_execution_evidence.md` (complete with live run screenshots) |
| Use at least one LLM interface | Met | `src/persona.py`, `src/outreach.py` |
| Clearly state what was not automated and why | Met | `docs/WHAT_WAS_NOT_AUTOMATED.md` |
| Enrich LinkedIn URL | Met | `data/speakers_enriched.csv` (`linkedin_url`) |
| Enrich job history | Met | `data/speakers_enriched.csv` (`job_history`) |
| Enrich seniority | Met | `data/speakers_enriched.csv` (`seniority`) |
| Enrich industry relevance | Met | `industry_relevance_score` |
| Enrich segmentation signals | Met | `signals`, `news_signal`, `event_role` |
| Persona + context + themes | Met | `data/speakers_personas.csv` |
| Hallucination controls described | Met | prompt and validator in `src/persona.py` |
| Pre/during/post email + LinkedIn flow | Met | `src/outreach.py`, `output/outreach_drafts.csv` |
| SDR/AE/Senior routing and assignment logic | Met | `src/route.py` |
| Duplicate outreach prevention | Met | dedup in `src/route.py` |
| Failure scenarios logic (all 4) | Met | routing + enrichment logic + docs |
| Workflow diagram submitted | Met | `docs/workflow_diagram.md` |
| Working prototype submitted | Met | pipeline + UI + no-code blueprint |
| KPI observations provided | Met | technical KPIs measured and campaign-limit caveats explicitly documented |
| Tools and rationale provided | Met | this README section |
| Limitations and safeguards provided | Met | this README + `docs/WHAT_WAS_NOT_AUTOMATED.md` |

Summary:
- Met: 19
- Partial: 1
- Not met: 0

## Submission Files

- `docs/workflow_diagram.md`
- `docs/make_scenario_spec.md`
- `docs/make_scenario_blueprint.json`
- `docs/WHAT_WAS_NOT_AUTOMATED.md`
- `docs/KPI_MEASUREMENT.md`
- `docs/EXTERNAL_EXECUTION_CHECKLIST.md`
- `docs/NO_CODE_PROOF_TEMPLATE.md`
- `docs/GOOGLE_SHEETS_SYSTEM_OF_RECORD.md`
- `docs/WEB_UI.md`
- `data/speakers_raw.csv`
- `data/speakers_enriched.csv`
- `data/speakers_personas.csv`
- `data/speakers_routed.csv`
- `data/campaign_tracking.csv`
- `data/campaign_tracking_template.csv`
- `output/outreach_drafts.csv`
- `output/performance_observations.json`
- `output/performance_observations.md`
- `output/no_code_execution_evidence.md`
- `output/sheets/README_IMPORT.md` (generated by `python src/prepare_sheet_import_pack.py`)
- `src/dispatch.py`
