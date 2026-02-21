TechSparks GTM Automation – Assignment Submission
Objective

Build a free-tier AI-powered automation prototype that converts a TechSparks attendee/speaker list into enriched contacts, persona context, lead routing, and multi-channel outreach drafts using LLMs and no-code orchestration tools.

This prototype demonstrates an end-to-end GTM automation workflow from public data ingestion to outreach draft generation and approval.

Overview

This system implements the complete workflow:

Public TechSparks Pages
→ Contact Scraper
→ Enrichment Engine
→ AI Persona Generation
→ Lead Routing and Deduplication
→ Outreach Draft Generation
→ Make.com No-Code Handoff
→ Approval Queue
→ KPI Tracking Support

All components run using free tools and publicly available data.

Quick Run

Run the pipeline:

python src/pipeline.py --fast

This performs:

scrape TechSparks public pages

enrich contacts

generate personas

route leads

generate outreach drafts

Outputs are written to:

data/
output/
Data Source and Contact Volume

Source: Public TechSparks agenda and speaker pages
Scraper: src/scrape_techsparks_contacts.py

Result:

180 contacts collected

normalized and deduplicated

meets assignment requirement (150–200 contacts)

Stored in:

data/speakers_raw.csv
Workflow Diagram

Full diagram:

docs/workflow_diagram.md

Pipeline stages:

Scraper
→ Enrichment
→ Persona Generation (LLM)
→ Routing
→ Outreach Draft Generation
→ Make.com Handoff
→ Approval Queue
Working Prototype Components
Python Orchestration Pipeline

File:

src/pipeline.py

Stages:

enrich

persona

route

outreach

Handles:

enrichment logic

LLM calls

routing logic

draft generation

output writing

Browser UI

Backend:

src/web_app.py

Frontend:

ui/src/main.ts

UI Features:

run pipeline

inspect each stage

view persona and outreach drafts

approval queue

quality dashboard

export CSV / JSON / XLSX

No-Code Automation Integration

Make.com blueprint:

docs/make_scenario_blueprint.json

Runtime handoff file:

output/make_handoff.csv

Purpose:

enables no-code orchestration

integrates with Google Sheets or outreach tools

demonstrates assignment no-code requirement

Scope 1 – Data Enrichment

Enriched fields:

linkedin_url
seniority
industry
industry_relevance_score
job_history
signals
news_signal
email_pattern
enrichment_confidence_score
evidence_score

Output file:

data/speakers_enriched.csv

Storage:

CSV files

Make.com handoff file

Google Sheets import pack

Safeguards:

schema validation

enrichment confidence scoring

duplicate detection

Scope 2 – AI Persona and Context Generation

Implemented using LLM.

File:

src/persona.py

Generated fields:

persona_archetype
persona_summary
context_summary
personalization_themes
relevance_score
recommended_hook
assignment_suggestion

Output file:

data/speakers_personas.csv

Hallucination controls:

strict JSON schema validation

retry logic

only uses enriched input data

structured output enforcement

Scope 3 – Outreach Workflow

Generated in:

output/outreach_drafts.csv

Includes:

email_subject_a
email_subject_b
email_body_pre_event
email_body_during_event
email_body_post_event
linkedin_note

Personalization based on:

persona archetype

seniority

industry relevance

event session topic

LinkedIn notes limited to under 300 characters.

Scope 4 – Lead Assignment Logic

Implemented in:

src/route.py

Routing categories:

SDR
AE
Senior AE
Partnership
Duplicate
Not Relevant

Routing logic uses:

seniority

persona relevance score

enrichment confidence score

event role

industry relevance

Duplicate prevention implemented using:

LinkedIn URL match

fuzzy matching fallback

Output:

data/speakers_routed.csv
Scope 5 – Failure and Scale Handling

Implemented handling:

Missing LinkedIn profile

enrichment confidence lowered

pipeline continues

Low-confidence persona

routed to manual review path

Duplicate contacts

detected and suppressed

marked as duplicate

Scaling from 200 to 2,000 contacts

Design supports scaling via:

cached enrichment

batch processing

queue-based orchestration using Make.com

KPI Tracking Support

Campaign tracking template:

data/campaign_tracking_template.csv

Supports tracking:

email delivery status

LinkedIn connection outcome

response status

Observed metrics populate automatically when outreach is executed via Make.com or email platform.

Outputs Generated
data/speakers_raw.csv
data/speakers_enriched.csv
data/speakers_personas.csv
data/speakers_routed.csv

output/outreach_drafts.csv
output/make_handoff.csv
No-Code Workflow Support

Make.com integration artifact:

docs/make_scenario_blueprint.json

Sheet import pack:

output/sheets/

Purpose:

connect pipeline to no-code workflow

enable orchestration and approval workflows

Tools Used and Rationale
Tool	Purpose
Python	orchestration pipeline
pandas	data processing
Mistral LLM	persona and outreach generation
requests	enrichment and scraping
Make.com	no-code orchestration
Flask	backend API
TypeScript + Vite	browser UI
Google Sheets	optional system of record

All tools used are free-tier compatible.
