# TechSparks GTM Automation


<img width="853" height="427" alt="image" src="https://github.com/user-attachments/assets/8bbcc4b2-3252-4684-b1fb-bf3cf36c5e59" />

<img width="839" height="397" alt="image" src="https://github.com/user-attachments/assets/9bfd7e26-201e-4472-830d-1fefaddf037c" />

<img width="832" height="409" alt="image" src="https://github.com/user-attachments/assets/2c83ced5-fce8-45b9-8c2d-7882535baeb3" />


<img width="853" height="290" alt="image" src="https://github.com/user-attachments/assets/330ddf0c-d00c-4815-9b14-6184dd9a3172" />




<img width="568" height="329" alt="image" src="https://github.com/user-attachments/assets/d6157442-c96c-4ab3-9935-ed0ce45a32c1" />

<img width="274" height="351" alt="image" src="https://github.com/user-attachments/assets/8d54b9cd-86a0-425e-aa05-70758afacbff" />


AI-powered GTM automation prototype that converts a TechSparks attendee list into enriched contacts, persona insights, lead routing, and personalized outreach drafts using free tools.

Built as part of a GTM Automation Engineer assignment.

---

# Overview

This system automates the workflow from raw contact ingestion to outreach draft generation.

**Workflow:**
Public TechSparks Pages  
→ Scraper  
→ Enrichment  
→ Persona Generation (LLM)  
→ Lead Routing + Deduplication  
→ Outreach Draft Generation  
→ Make.com Handoff  
→ Approval Queue and Export  

---

# Features

- Scrapes 180 TechSparks contacts from public pages  
- Enriches contacts with LinkedIn, seniority, industry, and signals  
- Generates persona summaries using LLM  
- Routes leads to SDR, AE, Senior AE, or Partnership  
- Creates personalized email and LinkedIn outreach drafts  
- Provides Make.com integration for no-code automation  
- Includes browser UI for inspection, approval, and export  
- Includes KPI tracking template  

---

# Project Structure

```
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
```

---

# Installation

Create virtual environment:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

---

# Run Pipeline

Fast demo run:

```bash
python src/pipeline.py --fast
```

This generates:

```
data/speakers_enriched.csv
data/speakers_personas.csv
data/speakers_routed.csv
output/outreach_drafts.csv
output/make_handoff.csv
```

---

# Data Source

Contacts are scraped from public TechSparks pages.

Total contacts: **180**

Scraper:

```
src/scrape_techsparks_contacts.py
```

---

# Enrichment

Adds:

- LinkedIn URL  
- seniority  
- industry  
- signals  
- enrichment confidence score  

Output:

```
data/speakers_enriched.csv
```

---

# Persona Generation

Uses LLM to generate:

- persona archetype  
- persona summary  
- personalization themes  
- relevance score  

Output:

```
data/speakers_personas.csv
```

---

# Lead Routing

Routes contacts into:

- SDR  
- AE  
- Senior AE  
- Partnership  
- Duplicate  
- Not Relevant  

Output:

```
data/speakers_routed.csv
```

---

# Outreach Draft Generation

Generates:

- Email subject lines  
- Pre-event email  
- During-event email  
- Post-event email  
- LinkedIn note  

Output:

```
output/outreach_drafts.csv
```

---

# No-Code Automation

Make.com integration blueprint:

```
docs/make_scenario_blueprint.json
```

Export file for automation:

```
output/make_handoff.csv
```

---

# UI

Browser UI allows:

- Run pipeline  
- Inspect enrichment, persona, routing, outreach  
- Approve or reject outreach drafts  
- Export CSV/JSON/XLSX  

Backend:

```
src/web_app.py
```

---

# KPI Tracking

Tracking template:

```
data/campaign_tracking_template.csv
```

Supports tracking:

- email delivery  
- LinkedIn acceptance  
- outreach outcomes  

---

# Tools Used

- Python  
- pandas  
- Mistral LLM  
- requests  
- Make.com (free tier)  
- Flask  
- TypeScript  

---

# Limitations

This prototype generates outreach drafts but does not send emails or LinkedIn messages.

---

# Summary

This project demonstrates an end-to-end AI-assisted GTM automation pipeline using public data, LLM-based personalization, intelligent routing, and no-code automation integration.
