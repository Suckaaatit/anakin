# Google Sheets System-of-Record Setup

This guide gives the shortest path to assignment-compliant proof that Google Sheets is the live record layer.

## 1) Generate Import Pack From Current Pipeline Outputs

Run:

```bash
python src/prepare_sheet_import_pack.py
```

Generated folder:

```text
output/sheets/
```

Files:
- `Raw_Contacts.csv`
- `Enriched_Contacts.csv`
- `AI_Personas.csv`
- `Outreach_Queue.csv`
- `Lead_Assignment.csv`
- `Suppression_List.csv`
- `README_IMPORT.md`

## 2) Create Google Sheet Tabs

Create one workbook with these tabs:
- `Raw_Contacts`
- `Enriched_Contacts`
- `AI_Personas`
- `Outreach_Queue`
- `Lead_Assignment`
- `Suppression_List`

Schema + formula details: `docs/sheet_schema.md`

## 3) Import CSVs

Import each CSV into the matching tab from `output/sheets/`.

Use:
- `File -> Import -> Upload`
- Import location: `Replace current sheet`

## 4) Validate Before Recording

Confirm these in Google Sheets:
- raw tab has 150-200 rows
- enriched tab includes `linkedin_url`, `job_history`, `seniority`, `industry`, `signals`
- persona tab includes summaries/themes
- outreach tab includes route + draft message columns
- queue status columns are populated

## 5) Evidence Required For Submission

Capture screenshots for:
- row count visible in `Raw_Contacts`
- enrichment columns in `Enriched_Contacts`
- persona outputs in `AI_Personas`
- route + outreach drafts in `Outreach_Queue`
- Make run history showing write-back to Sheets

Store proof references in `output/no_code_execution_evidence.md`.
