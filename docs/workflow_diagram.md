# TechSparks GTM Automation - Workflow Diagram

```mermaid
flowchart TD
    A[Public TechSparks Pages] --> B[scrape_techsparks_contacts.py]
    B --> C[data/speakers_raw.csv]

    C --> D[validate_env.py]
    D --> E[enrich.py]

    E --> E1[LinkedIn Lookup<br/>Brave + optional DDG fallback]
    E --> E2[Seniority + Industry + Relevance]
    E --> E3[Job History Snapshot + Signals]
    E --> E4[Confidence Score]
    E --> F{Confidence >= 1}

    F -->|Yes| G[persona.py]
    F -->|No| G0[Manual Review Queue]

    G --> G1[Persona + Context + Themes]
    G --> G2[JSON Validation + Anakin Guard]
    G --> H[route.py]

    H --> H1[Dedup: LinkedIn exact + fuzzy fallback]
    H --> H2[Lead Routing: Senior AE / AE / SDR / Not Relevant]
    H --> H3[Sequence Selection: Pre/During/Post]
    H --> I[outreach.py]

    I --> I1[Email Subject A/B]
    I --> I2[Pre, During, Post email bodies]
    I --> I3[LinkedIn Note under 300 chars]
    I --> I4[Final Anakin sanitization]

    I --> J[data/speakers_routed.csv]
    I --> K[output/outreach_drafts.csv]

    K --> L[Manual approval gate before send]
    L --> M[Email + LinkedIn manual send]

    subgraph UI[TypeScript UI]
      U1[Run/Stop pipeline]
      U2[Live logs]
      U3[Dataset preview + compliance chips]
    end

    U1 --> D
    U2 --> D
    U3 --> J
    U3 --> K
```

## Notes

- The fast path (`python src/pipeline.py --fast`) is used for rapid full-output generation.
- Live mode (`--live`) is available for evidence sampling with real web/LLM calls.
- No outbound messaging is auto-sent; drafts are generated and manually approved.
