# External Execution Checklist (Manual Platform Steps)

These items cannot be fully completed from local code alone because they require your own platform accounts.

## A) Google Sheets (Required for visible storage demo)

0. Generate import pack:
   - `python src/prepare_sheet_import_pack.py`
   - use files in `output/sheets/`
1. Create sheet: `TechSparks_GTM_Automation_2025`
2. Create tabs per `docs/sheet_schema.md`
3. Import:
   - `output/sheets/Raw_Contacts.csv` -> `Raw_Contacts`
   - `output/sheets/Enriched_Contacts.csv` -> `Enriched_Contacts`
   - `output/sheets/AI_Personas.csv` -> `AI_Personas`
   - `output/sheets/Outreach_Queue.csv` -> `Outreach_Queue`
4. Add formulas from `docs/sheet_schema.md`
5. Capture screenshot evidence:
   - row count visible (`~180`)
   - enrichment columns visible
   - persona themes visible
   - route + outreach columns visible

## B) Make.com Scenario (Required for no-code execution proof)

1. Import `docs/make_scenario_blueprint.json`
2. Point source to `output/make_handoff.csv` (or Google Sheet equivalent)
3. Configure LLM HTTP module and Sheets write-back modules
4. Run on at least one small batch (5-10 contacts)
5. Capture screenshot evidence:
   - scenario canvas
   - successful run history
   - operations consumed
6. Fill `docs/NO_CODE_PROOF_TEMPLATE.md` and copy final links/screenshots to `output/no_code_execution_evidence.md`

## C) Real KPI Evidence (Deliverability + Acceptance)

1. Run approved outreach manually (email + LinkedIn) on a safe sample
2. Track outcomes in `data/campaign_tracking.csv`
3. Run:

```bash
python src/performance_report.py --tracking-csv data/campaign_tracking.csv
```

4. Include:
   - `output/performance_observations.md`
   - one screenshot of `campaign_tracking.csv` (or Sheet equivalent)

## D) Final Submission Bundle

Include:
- `README.md`
- `docs/workflow_diagram.md`
- `docs/WHAT_WAS_NOT_AUTOMATED.md`
- `docs/make_scenario_spec.md`
- `docs/sheet_schema.md`
- `docs/KPI_MEASUREMENT.md`
- `docs/EXTERNAL_EXECUTION_CHECKLIST.md`
- `docs/NO_CODE_PROOF_TEMPLATE.md`
- `output/performance_observations.md`
- `output/no_code_execution_evidence.md`
