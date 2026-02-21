# Google Sheet Schema

**Sheet Name:** `TechSparks_GTM_Automation_2025`

---

## Tab 1: Raw_Contacts

**Purpose:** Source data from TechSparks speaker list.

| Column | Name | Type | Description |
|--------|------|------|-------------|
| A | id | Integer | Unique identifier |
| B | name | String | Full name |
| C | title | String | Job title |
| D | company | String | Company name |
| E | event_role | String | Speaker/Panelist/Judge/etc. |
| F | session_topic | String | Session or topic |
| G | source_url | String | Source URL |
| H | created_at | DateTime | Import timestamp |

---

## Tab 2: Enriched_Contacts

**Purpose:** Enriched data from Python pipeline.

All Tab 1 columns PLUS:

| Column | Name | Type | Description |
|--------|------|------|-------------|
| I | seniority | String | Normalized seniority level |
| J | industry | String | Industry classification |
| K | industry_relevance_score | Integer | 0-100 relevance score |
| L | linkedin_url | String | LinkedIn profile URL |
| M | linkedin_confidence | String | HIGH/MEDIUM/NOT_FOUND |
| N | previous_role_inferred | String | Inferred previous role |
| O | job_history | String | Current role + prior role snapshot |
| P | email_pattern | String | Inferred email pattern |
| Q | email_pattern_confidence | String | GUESSED/NOT_AVAILABLE |
| R | news_signal | String | Company/news or session fallback |
| S | signals | String | Segmentation signal bundle |
| T | enrichment_confidence_score | Integer | 0-5 score |
| U | enrichment_status | String | ENRICHED/ERROR |
| V | is_duplicate | Boolean | Duplicate flag |
| W | last_enriched_at | DateTime | Timestamp |

**Formulas:**

```
enrichment_status (Column U):
=IF(T2>=1,"READY","NEEDS_REVIEW")

is_duplicate (Column V):
=IF(COUNTIFS($C$2:C2,C2,$D$2:D2,D2)>1,"DUPLICATE","UNIQUE")
```

---

## Tab 3: AI_Personas

**Purpose:** LLM-generated persona data.

All Tab 2 columns PLUS:

| Column | Name | Type | Description |
|--------|------|------|-------------|
| U | persona_archetype | String | Persona type |
| V | persona_summary | String | 2-3 sentence summary |
| W | context_summary | String | Relevance to data intelligence |
| X | personalization_themes | String | Pipe-separated themes |
| Y | relevance_score | Integer | 0-100 score |
| Z | recommended_hook | String | Opening line |
| AA | assign_to | String | Senior AE/AE/SDR/Not Relevant |
| AB | persona_reason | String | Why this archetype |
| AC | persona_status | String | GENERATED/ERROR/SKIPPED |
| AD | llm_error | String | Error message if failed |
| AE | persona_generated_at | DateTime | Timestamp |

---

## Tab 4: Outreach_Queue

**Purpose:** Final outreach queue with drafts.

All Tab 3 columns PLUS:

| Column | Name | Type | Description |
|--------|------|------|-------------|
| AF | final_route | String | Assigned route |
| AG | outreach_sequence | String | Sequence type |
| AH | outreach_priority | Integer | 0-100 priority |
| AI | icp_match | Boolean | ICP match flag |
| AJ | segment_cluster | String | Segment name |
| AK | account_priority_score | Integer | Composite score |
| AL | outreach_score | Integer | Final score |
| AM | route_reason | String | Why this route |
| AN | outreach_approved | String | PENDING_REVIEW/YES/NO |
| AO | outreach_send_window | String | PRE/DURING/POST |
| AP | email_subject_a | String | Variant A subject |
| AQ | email_subject_b | String | Variant B subject |
| AR | email_body_pre_event | String | Pre-event email |
| AS | email_body_during_event | String | During-event email |
| AT | email_body_post_event | String | Post-event email |
| AU | linkedin_note | String | LinkedIn connection note |
| AV | sequence_timing | String | Recommended timing |
| AW | message_variant | String | A/B variant |
| AX | experiment_group | String | CONTROL/TREATMENT |
| AY | outreach_status | String | DRAFT/SENT/REPLIED |
| AZ | sent_at | DateTime | Send timestamp |
| BA | replied_at | DateTime | Reply timestamp |
| BB | reply_label | String | Positive/Neutral/Negative |
| BC | notes | String | Manual notes |

**Formulas:**

```
outreach_score (Column AL):
=ROUND((VALUE(IFERROR(Y2,0))*0.4)+(VALUE(IFERROR(Q2,0))*8)+IF(I2="C-Suite / Founder",20,IF(I2="VP",12,IF(I2="Director / Head",6,2)))+IF(AI2=TRUE,10,0),0)

ready_to_send (helper column):
=AND(AN2="PENDING_REVIEW",NOT(ISNUMBER(MATCH(B2,Suppression_List!A:A,0))),AF2<>"Not Relevant",T2>=1)
```

---

## Tab 5: Lead_Assignment

**Purpose:** Track rep workload and performance.

| Column | Name | Type |
|--------|------|------|
| A | rep_name | String |
| B | rep_type | String |
| C | tier | String |
| D | assigned_count | Integer (formula) |
| E | pending_count | Integer (formula) |
| F | sent_count | Integer (formula) |
| G | replied_count | Integer (formula) |
| H | reply_rate | Percent (formula) |

**Hardcoded rows:**

| rep_name | rep_type | tier |
|----------|----------|------|
| Sarah Chen | Senior AE | Tier A |
| Raj Patel | AE | Tier B |
| Priya Kumar | SDR | Tier C |
| Partnership Lead | Senior AE | VC Track |

---

## Tab 6: Suppression_List

**Purpose:** Contacts to exclude from outreach.

| Column | Name | Type |
|--------|------|------|
| A | name | String |
| B | company | String |
| C | reason | String |
| D | added_at | DateTime |

**Reason values:**
- Opted Out
- Competitor
- Existing Customer
- Bounced
- Do Not Contact

---

## Tab 7: Analytics_Dashboard

**Purpose:** Visual analytics (charts only, no raw data).

### Chart 1: Persona Archetype Distribution
- **Type:** Pie chart
- **Source:** AI_Personas tab, `persona_archetype` column
- **Purpose:** See persona mix

### Chart 2: Lead Route Distribution
- **Type:** Bar chart
- **Source:** Outreach_Queue, `final_route` column
- **Purpose:** See routing distribution

### Chart 3: Relevance Score Distribution
- **Type:** Bar chart (buckets)
- **Buckets:** 0-30, 31-60, 61-80, 81-100
- **Source:** AI_Personas, `relevance_score` column
- **Purpose:** Quality distribution

### Chart 4: Enrichment Confidence Distribution
- **Type:** Bar chart
- **Source:** Enriched_Contacts, `enrichment_confidence_score` column
- **Purpose:** Data quality check

### Chart 5: Outreach Funnel
- **Type:** Funnel chart
- **Stages:** DRAFT → SENT → REPLIED → CONVERTED
- **Source:** Outreach_Queue, `outreach_status` column
- **Purpose:** Conversion tracking

### Chart 6: ICP Match Rate
- **Type:** Bar chart
- **Source:** Outreach_Queue, `icp_match` column
- **Purpose:** Target audience alignment
