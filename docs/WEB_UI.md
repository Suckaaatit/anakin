# Web UI (TypeScript) - Run Guide

This project now includes a browser UI for non-terminal users.

## 1) Install Python dependencies

```bash
pip install -r requirements.txt
```

## 2) Start backend API

```bash
python src/web_app.py
```

Default URL: `http://127.0.0.1:8000`

## 3) Start TypeScript frontend

```bash
cd ui
npm install
npm run dev
```

Default URL: `http://127.0.0.1:5173`

The frontend proxies `/api/*` calls to `http://127.0.0.1:8000`.

## 4) Build frontend for production serving (optional)

```bash
cd ui
npm run build
```

After build, `src/web_app.py` can serve the UI from `ui/dist` directly on port `8000`.

## What users can do in UI

- Start full or test pipeline runs
- Resume from any stage (`enrich`, `persona`, `route`, `outreach`)
- Stop an in-flight run
- Watch live pipeline logs
- Inspect output row counts and warnings
  - summary counters start at `0000` until a run is started from the current app session
- Preview all generated CSV datasets in-table
- Download selected dataset as CSV/JSON/XLSX directly from the UI
- See compliance checks (`Anakin` check + output presence)
- Open Stage Inspector modal (passed/failed samples, stage logs, stage config)
- View Quality Dashboard charts (route mix, confidence histogram, KPI bars)
  - KPI bars include LinkedIn match/lookup coverage, deliverability/acceptance rates, send readiness, and spam risk
  - confidence distribution + persona-theme coverage + average confidence are visible in the dashboard
- See ICP metric annotation explaining fast-mode vs live-mode context
- Manage Outreach Approval Queue with Approve/Reject actions
  - queue summary shows pending/approved/rejected/not-queueable (no total badge)
  - not-queueable reason note is visible in the UI
- Open Outreach Preview drawer with:
  - subject A/B preview
  - post-event email body preview
  - LinkedIn note shown from actual generated output with char count
  - explainability breakdown for route decision
  - persona summary/context/themes and confidence/evidence scores
- Optional control-token input for secured `/api/run`, `/api/stop`, `/api/queue/action`

## Speed modes

- Default backend behavior uses `FAST_MODE=1` (quick cached path).
- In UI, **Cached Mode (faster, uses prior enrichment)** is available when you want speed and lower token usage.
- You can also run either mode directly from terminal:

```bash
python src/pipeline.py --fast
python src/pipeline.py --live
```

## Download formats

Dataset downloads now support:
- CSV: `/api/download/<dataset>?format=csv`
- JSON: `/api/download/<dataset>?format=json`
- XLSX: `/api/download/<dataset>?format=xlsx`
