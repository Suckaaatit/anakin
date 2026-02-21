TechSparks GTM Automation

AI-powered GTM automation prototype that converts a TechSparks attendee list into enriched contacts, persona insights, routed leads, and personalized outreach drafts using free tools.

Built as part of the GTM Automation Engineer assignment.

Overview

This system automates the workflow from raw contact ingestion to outreach draft generation.

Workflow:

Public TechSparks Pages
→ Scraper
→ Enrichment
→ Persona Generation (LLM)
→ Lead Routing + Deduplication
→ Outreach Draft Generation
→ Make.com Handoff
→ Approval Queue and Export

Features

Scrapes 180 TechSparks contacts from public pages

Enriches contacts with LinkedIn, seniority, industry, and signals

Generates persona summaries using LLM

Routes leads to SDR, AE, Senior AE, or Partnership

Creates personalized email and LinkedIn outreach drafts

Provides Make.com integration for no-code automation

Includes browser UI for inspection, approval, and export

Supports KPI tracking template

Project Structure
src/
  pipeline.py
  scrape_techsparks_contacts.py
  persona.py
  route.py
  outreach.py
  web_app.py

data/
  speakers_raw.csv
  speakers_enriched.csv
  speakers_personas.csv
  speakers_routed.csv

output/
  outreach_drafts.csv
  make_handoff.csv

docs/
  workflow_diagram.md
  make_scenario_blueprint.json
Installation

Create virtual environment:

python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
Run Pipeline

Fast demo run:

python src/pipeline.py --fast

This will generate:

data/speakers_enriched.csv
data/speakers_personas.csv
data/speakers_routed.csv
output/outreach_drafts.csv
output/make_handoff.csv
Data Source

Contacts are scraped from public TechSparks pages.

Total contacts: 180

Scraper:

src/scrape_techsparks_contacts.py
Enrichment

Adds:

LinkedIn URL

seniority

industry

signals

enrichment confidence score

Output:

data/speakers_enriched.csv
Persona Generation

Uses LLM to generate:

persona archetype

summary

personalization themes

relevance score

Output:

data/speakers_personas.csv
Lead Routing

Routes contacts into:

SDR

AE

Senior AE

Partnership

Duplicate

Not Relevant

Output:

data/speakers_routed.csv
Outreach Draft Generation

Generates:

Email subject lines

Pre-event email

During-event email

Post-event email

LinkedIn note

Output:

output/outreach_drafts.csv
No-Code Automation

Make.com integration file:

docs/make_scenario_blueprint.json

Export file for automation:

output/make_handoff.csv
UI

Browser UI allows:

Run pipeline

Inspect enrichment, persona, routing, outreach

Approve or reject outreach drafts

Export data

Backend:

src/web_app.py
KPI Tracking

Template included:

data/campaign_tracking_template.csv

Supports tracking:

email delivery

LinkedIn acceptance

outreach outcomes

Tools Used

Python

pandas

Mistral LLM

requests

Make.com (free tier)

Flask

TypeScript
