import "./style.css";

type RunStatus = "idle" | "running" | "completed" | "failed";
type StageName = "enrich" | "persona" | "route" | "outreach";
type StageTab = "passed" | "failed" | "logs" | "config";
type FlashTone = "neutral" | "success" | "error";

interface StatusPayload {
  status: RunStatus;
  mode: string;
  from_stage: string | null;
  started_at: string | null;
  finished_at: string | null;
  return_code: number | null;
  logs: string[];
}

interface SummaryPayload {
  files: Record<string, { exists: boolean; rows: number }>;
  warnings: string[];
  compliance: { anakin_found: boolean; all_outputs_exist: boolean };
}

interface PreviewPayload {
  file: string;
  rows_total: number;
  rows_returned: number;
  columns: string[];
  data: Array<Record<string, unknown>>;
}

interface QualityPayload {
  route_distribution: Record<string, number>;
  confidence_histogram: Record<string, number>;
  kpis: {
    enrichment_success_rate: number;
    linkedin_missing_pct: number;
    linkedin_not_attempted_pct: number;
    linkedin_match_coverage_pct?: number;
    linkedin_lookup_coverage_pct?: number;
    icp_match_rate: number;
    outreach_generation_rate: number;
    confidence_avg_score?: number;
    low_confidence_pct?: number;
    email_deliverability_proxy_pct?: number;
    email_deliverability_rate_pct?: number;
    email_deliverability_measurement_mode?: string;
    linkedin_acceptance_proxy_pct?: number;
    linkedin_acceptance_rate_pct?: number;
    linkedin_acceptance_measurement_mode?: string;
    expected_linkedin_acceptance_rate_pct?: number;
    send_readiness_score_pct?: number;
    spam_risk_score_pct?: number;
    persona_theme_nonempty_pct?: number;
  };
}

interface StageDetailsPayload {
  stage: StageName;
  file: string;
  counts: { total: number; passed: number; failed: number };
  samples: { passed: Array<Record<string, unknown>>; failed: Array<Record<string, unknown>> };
  logs: string[];
  config: Record<string, unknown>;
}

interface QueuePayload {
  counts: { total?: number; pending?: number; approved?: number; rejected?: number; non_queueable?: number };
  rows: Array<Record<string, unknown>>;
}

interface ContactPayload {
  contact: Record<string, unknown>;
  explainability: {
    priority_score: number;
    breakdown: Array<{ label: string; value: number }>;
    segment_cluster: string;
    final_route: string;
    sequence: string;
    route_reason: string;
  };
}

const statusEl = byId<HTMLElement>("run-status");
const runMetaEl = byId<HTMLElement>("run-meta");
const lastUpdatedEl = byId<HTMLElement>("last-updated");
const flashEl = byId<HTMLElement>("flash-message");
const logOutputEl = byId<HTMLElement>("log-output");

const runFullBtn = byId<HTMLButtonElement>("run-full");
const runStopBtn = byId<HTMLButtonElement>("run-stop");
const refreshBtn = byId<HTMLButtonElement>("refresh-all");
const stageSelectEl = byId<HTMLSelectElement>("from-stage");
const fastModeEl = byId<HTMLInputElement>("fast-mode");
const apiTokenEl = byId<HTMLInputElement>("api-token");

const previewSelectEl = byId<HTMLSelectElement>("preview-dataset");
const previewFormatEl = byId<HTMLSelectElement>("preview-format");
const previewRefreshBtn = byId<HTMLButtonElement>("preview-refresh");
const previewDownloadBtn = byId<HTMLButtonElement>("preview-download");
const previewMetaEl = byId<HTMLElement>("preview-meta");
const previewTableEl = byId<HTMLElement>("preview-table");

const countRawEl = byId<HTMLElement>("count-raw");
const countEnrichedEl = byId<HTMLElement>("count-enriched");
const countPersonasEl = byId<HTMLElement>("count-personas");
const countRoutedEl = byId<HTMLElement>("count-routed");
const countOutreachEl = byId<HTMLElement>("count-outreach");

const warningsListEl = byId<HTMLElement>("warnings-list");
const complianceListEl = byId<HTMLElement>("compliance-list");

const qualityRefreshBtn = byId<HTMLButtonElement>("quality-refresh");
const chartRouteEl = byId<HTMLElement>("chart-route");
const chartKpiEl = byId<HTMLElement>("chart-kpi");
const chartTrendEl = byId<HTMLElement>("chart-trend");

const queueFilterEl = byId<HTMLSelectElement>("queue-filter");
const queueRefreshBtn = byId<HTMLButtonElement>("queue-refresh");
const queueTableEl = byId<HTMLElement>("queue-table");
const queuePendingEl = byId<HTMLElement>("queue-pending");
const queueApprovedEl = byId<HTMLElement>("queue-approved");
const queueRejectedEl = byId<HTMLElement>("queue-rejected");
const queueNonQueueableEl = byId<HTMLElement>("queue-non-queueable");
const queueNoteEl = byId<HTMLElement>("queue-note");

const stageModalEl = byId<HTMLElement>("stage-modal");
const stageModalTitleEl = byId<HTMLElement>("stage-modal-title");
const stageModalMetaEl = byId<HTMLElement>("stage-modal-meta");
const stageModalCloseBtn = byId<HTMLButtonElement>("stage-modal-close");
const stageTabContentEl = byId<HTMLElement>("stage-tab-content");
const tabPassedBtn = byId<HTMLButtonElement>("tab-passed");
const tabFailedBtn = byId<HTMLButtonElement>("tab-failed");
const tabLogsBtn = byId<HTMLButtonElement>("tab-logs");
const tabConfigBtn = byId<HTMLButtonElement>("tab-config");

const drawerEl = byId<HTMLElement>("outreach-drawer");
const drawerBackdropEl = byId<HTMLElement>("drawer-backdrop");
const drawerCloseBtn = byId<HTMLButtonElement>("drawer-close");
const drawerTitleEl = byId<HTMLElement>("drawer-title");
const drawerContentEl = byId<HTMLElement>("drawer-content");

let latestStatus: RunStatus = "idle";
let stageDetails: StageDetailsPayload | null = null;
let stageTab: StageTab = "passed";
let controlToken = window.localStorage.getItem("web_app_token") ?? "";
let hasDataReady = false;

const ZERO_METRIC_DISPLAY = "0000";

apiTokenEl.value = controlToken;

wireEvents();
void refreshAll(true);
schedulePoll();

function byId<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing #${id}`);
  return element as T;
}

function wireEvents(): void {
  runFullBtn.onclick = async () => startRun();
  runStopBtn.onclick = async () => stopRun();
  refreshBtn.onclick = async () => refreshAll(true);
  apiTokenEl.oninput = () => {
    controlToken = apiTokenEl.value.trim();
    window.localStorage.setItem("web_app_token", controlToken);
  };
  previewRefreshBtn.onclick = async () => loadPreview();
  previewDownloadBtn.onclick = async () => downloadDataset();
  previewSelectEl.onchange = async () => loadPreview();
  qualityRefreshBtn.onclick = async () => refreshQuality();
  queueRefreshBtn.onclick = async () => refreshQueue();
  queueFilterEl.onchange = async () => refreshQueue();
  stageModalCloseBtn.onclick = closeStageModal;
  tabPassedBtn.onclick = () => setStageTab("passed");
  tabFailedBtn.onclick = () => setStageTab("failed");
  tabLogsBtn.onclick = () => setStageTab("logs");
  tabConfigBtn.onclick = () => setStageTab("config");
  stageModalEl.onclick = (event) => {
    if (event.target === stageModalEl) closeStageModal();
  };
  drawerCloseBtn.onclick = closeDrawer;
  drawerBackdropEl.onclick = closeDrawer;
  document.querySelectorAll<HTMLElement>("[data-stage]").forEach((el) => {
    el.addEventListener("click", async () => {
      const stage = el.dataset.stage as StageName | undefined;
      if (!stage) return;
      await openStage(stage);
    });
  });
  queueTableEl.addEventListener("click", async (event) => {
    const target = event.target as HTMLElement;
    const btn = target.closest<HTMLButtonElement>("button[data-action]");
    if (btn) {
      const id = btn.dataset.id ?? "";
      const action = btn.dataset.action ?? "";
      if (action === "approve" || action === "reject") await queueAction(id, action);
      if (action === "view") await openDrawer(id);
      return;
    }
    const row = target.closest<HTMLTableRowElement>("tr[data-id]");
    if (row?.dataset.id) await openDrawer(row.dataset.id);
  });
}

async function refreshAll(includePreview: boolean): Promise<void> {
  await refreshStatusSummary(includePreview);
  await Promise.all([refreshQuality(), refreshQueue()]);
}

async function refreshStatusSummary(includePreview: boolean): Promise<void> {
  const [status, summary] = await Promise.all([
    api<StatusPayload>("/api/status"),
    api<SummaryPayload>("/api/summary"),
  ]);
  const started = Boolean(status.started_at);
  const finished = status.status !== "running" && status.return_code !== null;
  hasDataReady = started && finished;
  latestStatus = status.status;
  statusEl.textContent = status.status.toUpperCase();
  statusEl.className = `status-pill ${status.status}`;
  runMetaEl.textContent = `mode=${status.mode} | from=${status.from_stage ?? "start"} | exit=${status.return_code ?? "-"}`;
  const visibleLogs = status.logs.filter(shouldShowRunLogLine);
  logOutputEl.textContent = visibleLogs.join("\n") || "No run highlights yet.";
  runStopBtn.disabled = status.status !== "running";
  countRawEl.textContent = formatMetricCount(Number(summary.files.raw?.rows ?? 0), hasDataReady);
  countEnrichedEl.textContent = formatMetricCount(Number(summary.files.enriched?.rows ?? 0), hasDataReady);
  countPersonasEl.textContent = formatMetricCount(Number(summary.files.personas?.rows ?? 0), hasDataReady);
  countRoutedEl.textContent = formatMetricCount(Number(summary.files.routed?.rows ?? 0), hasDataReady);
  countOutreachEl.textContent = formatMetricCount(Number(summary.files.outreach?.rows ?? 0), hasDataReady);
  warningsListEl.innerHTML = (summary.warnings.length ? summary.warnings : ["No warnings."]).map((w) => `<li>${escapeHtml(w)}</li>`).join("");
  complianceListEl.innerHTML = [
    chip("Anakin Check", !summary.compliance.anakin_found),
    chip("Outputs Present", summary.compliance.all_outputs_exist),
    chip("API Reachable", true),
  ].join("");
  lastUpdatedEl.textContent = `Updated ${new Date().toLocaleTimeString()}`;
  if (includePreview) await loadPreview();
}

function chip(label: string, ok: boolean): string {
  return `<span class="chip ${ok ? "chip-ok" : "chip-fail"}">${ok ? "PASS" : "FAIL"} · ${escapeHtml(label)}</span>`;
}

async function refreshQuality(): Promise<void> {
  const quality = await api<QualityPayload>("/api/quality");
  const routeEntries = hasDataReady ? Object.entries(quality.route_distribution) : [];
  const routeTotal = routeEntries.reduce((sum, [, value]) => sum + (Number(value) || 0), 0);
  const routePercentages: Record<string, number> = {};
  for (const [route, countRaw] of routeEntries) {
    const count = Number(countRaw) || 0;
    const pct = routeTotal > 0 ? (count / routeTotal) * 100 : 0;
    routePercentages[`${route} (${count})`] = Number(pct.toFixed(1));
  }
  renderBars(chartRouteEl, routePercentages, "%");
  const linkedinCoverage = hasDataReady
    ? Number(quality.kpis.linkedin_match_coverage_pct ?? Math.max(0, 100 - Number(quality.kpis.linkedin_missing_pct || 0)))
    : 0;
  const attemptedLookupCoverage = hasDataReady
    ? Number(quality.kpis.linkedin_lookup_coverage_pct ?? Math.max(0, 100 - Number(quality.kpis.linkedin_not_attempted_pct || 0)))
    : 0;
  const outreachCoverage = hasDataReady ? Number(quality.kpis.outreach_generation_rate || 0) : 0;
  const emailDeliverabilityMode = String(quality.kpis.email_deliverability_measurement_mode || "none").toLowerCase();
  const linkedinAcceptanceMode = String(quality.kpis.linkedin_acceptance_measurement_mode || "none").toLowerCase();
  const deliverabilityObserved =
    hasDataReady && emailDeliverabilityMode === "observed" && quality.kpis.email_deliverability_rate_pct !== undefined && quality.kpis.email_deliverability_rate_pct !== null;
  const linkedinAcceptanceObserved =
    hasDataReady && linkedinAcceptanceMode === "observed" && quality.kpis.linkedin_acceptance_rate_pct !== undefined && quality.kpis.linkedin_acceptance_rate_pct !== null;
  const deliverabilityRate = deliverabilityObserved ? Number(quality.kpis.email_deliverability_rate_pct) : 0;
  const linkedinAcceptanceRate = linkedinAcceptanceObserved ? Number(quality.kpis.linkedin_acceptance_rate_pct) : 0;
  const sendReadiness = hasDataReady ? Number(quality.kpis.send_readiness_score_pct || 0) : 0;
  const spamRisk = hasDataReady ? Number(quality.kpis.spam_risk_score_pct || 0) : 0;
  const kpiBars: Record<string, number> = {
    "LinkedIn Match Coverage %": linkedinCoverage,
    "LinkedIn Lookup Coverage %": attemptedLookupCoverage,
    "Send Readiness Score %": sendReadiness,
    "Spam Risk Score %": spamRisk,
    "Outreach Draft Coverage %": outreachCoverage,
  };
  if (deliverabilityObserved) kpiBars["Email Deliverability Rate %"] = deliverabilityRate;
  if (linkedinAcceptanceObserved) kpiBars["LinkedIn Acceptance Rate %"] = linkedinAcceptanceRate;
  renderBars(
    chartKpiEl,
    kpiBars,
    "%",
  );
  const confidenceEntries = hasDataReady ? Object.entries(quality.confidence_histogram) : [];
  const confidenceTotal = confidenceEntries.reduce((sum, [, value]) => sum + (Number(value) || 0), 0);
  const confidencePercentages: Record<string, number> = {};
  for (const [bucket, countRaw] of confidenceEntries) {
    const count = Number(countRaw) || 0;
    const pct = confidenceTotal > 0 ? (count / confidenceTotal) * 100 : 0;
    confidencePercentages[`Confidence ${bucket} (${count})`] = Number(pct.toFixed(1));
  }
  const personaThemeCoverage = hasDataReady ? Number(quality.kpis.persona_theme_nonempty_pct || 0) : 0;
  let avgConfidence = hasDataReady ? Number(quality.kpis.confidence_avg_score || 0) : 0;
  const hasNonZeroConfidenceBucket = confidenceEntries.some(([bucket, countRaw]) => {
    const bucketNum = Number(bucket);
    const count = Number(countRaw) || 0;
    return bucketNum > 0 && count > 0;
  });
  if (hasDataReady && confidenceTotal > 0 && (!Number.isFinite(avgConfidence) || (avgConfidence <= 0 && hasNonZeroConfidenceBucket))) {
    const weightedSum = confidenceEntries.reduce((sum, [bucket, countRaw]) => {
      const bucketNum = Number(bucket) || 0;
      const count = Number(countRaw) || 0;
      return sum + bucketNum * count;
    }, 0);
    avgConfidence = weightedSum / confidenceTotal;
  }
  const lowConfidencePct = hasDataReady ? Number(quality.kpis.low_confidence_pct || 0) : 0;
  const emailModeText = deliverabilityObserved ? "observed" : "awaiting sample outcomes";
  const linkedinModeText = linkedinAcceptanceObserved ? "observed" : "awaiting sample outcomes";
  chartTrendEl.innerHTML = hasDataReady
    ? `<p class="mono">Enrichment success: ${quality.kpis.enrichment_success_rate.toFixed(1)}% | Persona-theme coverage: ${personaThemeCoverage.toFixed(1)}% | Avg confidence: ${avgConfidence.toFixed(2)}/5 | Low-confidence (<2): ${lowConfidencePct.toFixed(1)}% | Deliverability: ${emailModeText} | LinkedIn acceptance: ${linkedinModeText}</p>`
    : `<p class="mono">Confidence distribution will appear after first run.</p>`;
  if (hasDataReady) {
    const holder = document.createElement("div");
    holder.className = "chart-bars";
    chartTrendEl.appendChild(holder);
    renderBars(holder, confidencePercentages, "%");
  }
}

function renderBars(el: HTMLElement, data: Record<string, number>, suffix: string): void {
  const entries = Object.entries(data);
  if (!entries.length) {
    el.textContent = "No data";
    return;
  }
  const max = Math.max(...entries.map(([, v]) => Number(v) || 0), 1);
  el.innerHTML = entries
    .map(([k, v]) => {
      const value = Number(v) || 0;
      return `<div class="bar-row"><span class="bar-label">${escapeHtml(k)}</span><div class="bar-track"><div class="bar-fill" style="width:${(value / max) * 100}%"></div></div><span class="bar-value">${value.toFixed(1)}${suffix}</span></div>`;
    })
    .join("");
}

async function refreshQueue(): Promise<void> {
  if (!hasDataReady) {
    queuePendingEl.textContent = "Pending: 0";
    queueApprovedEl.textContent = "Approved: 0";
    queueRejectedEl.textContent = "Rejected: 0";
    queueNonQueueableEl.textContent = "Not Queueable: 0";
    queueNoteEl.textContent = "Not Queueable = skipped by routing guardrails (not relevant, duplicate, or low-confidence manual-review path).";
    queueTableEl.textContent =
      latestStatus === "running"
        ? "Pipeline running. Queue rows will appear after the run completes."
        : "Run the pipeline to populate queue rows.";
    return;
  }
  const payload = await api<QueuePayload>(`/api/queue?status=${encodeURIComponent(queueFilterEl.value)}&limit=200`);
  const pending = toCount(payload.counts?.pending);
  const approved = toCount(payload.counts?.approved);
  const rejected = toCount(payload.counts?.rejected);
  const nonQueueable = toCount(payload.counts?.non_queueable);
  queuePendingEl.textContent = `Pending: ${pending}`;
  queueApprovedEl.textContent = `Approved: ${approved}`;
  queueRejectedEl.textContent = `Rejected: ${rejected}`;
  queueNonQueueableEl.textContent = `Not Queueable: ${nonQueueable}`;
  queueNoteEl.textContent = `Not Queueable (${nonQueueable}) = skipped by routing guardrails (not relevant, duplicate, or low-confidence manual-review path).`;
  renderTable(
    queueTableEl,
    payload.rows,
    [
      "id",
      "name",
      "company",
      "final_route",
      "relevance_score",
      "enrichment_confidence_score",
      "evidence_score",
      "outreach_sequence",
      "outreach_approved",
      "approval_decision",
    ],
    true,
  );
}

async function loadPreview(): Promise<void> {
  if (!hasDataReady) {
    previewMetaEl.textContent = `${datasetDisplayName(previewSelectEl.value)} | rows 0/0`;
    previewTableEl.textContent =
      latestStatus === "running"
        ? "Pipeline running. Preview will load after the run completes."
        : "Run the pipeline to preview current-session dataset rows.";
    return;
  }
  const payload = await api<PreviewPayload>(`/api/preview/${previewSelectEl.value}?limit=20`);
  previewMetaEl.textContent = `${datasetDisplayName(previewSelectEl.value)} | rows ${payload.rows_returned}/${payload.rows_total}`;
  renderTable(previewTableEl, payload.data, payload.columns.slice(0, 10), false);
}

async function startRun(): Promise<void> {
  await api("/api/run", {
    method: "POST",
    body: JSON.stringify({ test_mode: false, from_stage: stageSelectEl.value || null, fast_mode: fastModeEl.checked }),
  });
  setFlash("Pipeline started.", "success");
}

async function stopRun(): Promise<void> {
  await api("/api/stop", { method: "POST" });
  setFlash("Stop requested.", "success");
}

async function openStage(stage: StageName): Promise<void> {
  if (!hasDataReady) {
    setFlash(
      latestStatus === "running"
        ? "Pipeline is running. Stage samples unlock after the run completes."
        : "Run the pipeline first to inspect stage samples.",
      "neutral",
    );
    return;
  }
  stageDetails = await api<StageDetailsPayload>(`/api/stage/${stage}/details?limit=20`);
  stageTab = "passed";
  renderStageModal();
  stageModalEl.classList.remove("hidden");
}

function closeStageModal(): void {
  stageModalEl.classList.add("hidden");
}

function setStageTab(tab: StageTab): void {
  stageTab = tab;
  renderStageModal();
}

function renderStageModal(): void {
  if (!stageDetails) return;
  stageModalTitleEl.textContent = `Stage Inspector: ${stageDisplayName(stageDetails.stage)}`;
  stageModalMetaEl.textContent = `total=${stageDetails.counts.total} passed=${stageDetails.counts.passed} failed=${stageDetails.counts.failed}`;
  [tabPassedBtn, tabFailedBtn, tabLogsBtn, tabConfigBtn].forEach((b) => b.classList.remove("active"));
  if (stageTab === "passed") {
    tabPassedBtn.classList.add("active");
    renderTable(stageTabContentEl, stageDetails.samples.passed, Object.keys(stageDetails.samples.passed[0] ?? {}), false);
  } else if (stageTab === "failed") {
    tabFailedBtn.classList.add("active");
    renderTable(stageTabContentEl, stageDetails.samples.failed, Object.keys(stageDetails.samples.failed[0] ?? {}), false);
  } else if (stageTab === "logs") {
    tabLogsBtn.classList.add("active");
    stageTabContentEl.innerHTML = `<pre class="log-box mono stage-log">${escapeHtml(stageDetails.logs.join("\n") || "No logs.")}</pre>`;
  } else {
    tabConfigBtn.classList.add("active");
    const safeConfig = sanitizeConfig(stageDetails.config);
    stageTabContentEl.innerHTML = `<pre class="config-box mono">${escapeHtml(JSON.stringify(safeConfig, null, 2))}</pre>`;
  }
}

async function queueAction(id: string, action: string): Promise<void> {
  await api("/api/queue/action", { method: "POST", body: JSON.stringify({ id, decision: action }) });
  setFlash(`Queue updated: ${action}`, "success");
  await refreshQueue();
}

async function openDrawer(id: string): Promise<void> {
  const payload = await api<ContactPayload>(`/api/contact/${encodeURIComponent(id)}`);
  const contact = payload.contact;
  const explain = payload.explainability;
  const subjectA = friendlyCellValue(contact.email_subject_a);
  const subjectB = friendlyCellValue(contact.email_subject_b);
  const postBody = friendlyCellValue(contact.email_body_post_event);
  const note = friendlyCellValue(contact.linkedin_note);
  const personaSummary = friendlyCellValue(contact.persona_summary);
  const contextSummary = friendlyCellValue(contact.context_summary);
  const themes = friendlyCellValue(contact.personalization_themes);
  const conf = Number(contact.enrichment_confidence_score ?? 0) || 0;
  const evidence = Number(contact.evidence_score ?? 0) || 0;
  const relevance = Number(contact.relevance_score ?? 0) || 0;
  const breakdown = explain.breakdown.map((item) => `<li><span>${escapeHtml(item.label)}</span><strong>${item.value >= 0 ? "+" : ""}${item.value}</strong></li>`).join("");
  drawerTitleEl.textContent = `Outreach Preview: ${friendlyCellValue(contact.name ?? id)}`;
  drawerContentEl.innerHTML = `
    <p class="mono">${escapeHtml(friendlyCellValue(contact.company ?? ""))} | <span class="chip chip-ok">${escapeHtml(friendlyCellValue(contact.persona_archetype ?? ""))}</span></p>
    <p class="mono">Relevance: ${relevance}</p>
    <p class="mono">Confidence: ${conf}/5 | Evidence: ${evidence}/100</p>
    <div class="bar-track"><div class="bar-fill" style="width:${Math.max(0, Math.min(relevance, 100))}%"></div></div>
    <div class="drawer-section"><h3>Persona Context</h3><p class="mono">${escapeHtml(personaSummary)}</p><p class="mono">${escapeHtml(contextSummary)}</p><p class="mono">Themes: ${escapeHtml(themes)}</p></div>
    <div class="drawer-section">
      <h3>Email Subject</h3>
      <div class="tab-row">
        <button id="subject-a-btn" class="btn btn-ghost tiny-btn active">Subject A</button>
        <button id="subject-b-btn" class="btn btn-ghost tiny-btn">Subject B</button>
        <button id="copy-subject-btn" class="btn btn-secondary tiny-btn">Copy</button>
      </div>
      <pre id="subject-preview" class="content-box mono">${escapeHtml(subjectA)}</pre>
    </div>
    <div class="drawer-section">
      <h3>Email Body (Post Event)</h3>
      <button id="copy-body-btn" class="btn btn-secondary tiny-btn">Copy</button>
      <pre class="content-box mono">${escapeHtml(postBody)}</pre>
    </div>
    <div class="drawer-section">
      <h3>LinkedIn Note</h3>
      <div class="tab-row"><p class="mono">${note.length}/299 chars</p><button id="copy-note-btn" class="btn btn-secondary tiny-btn">Copy</button></div>
      <pre class="content-box mono">${escapeHtml(note)}</pre>
    </div>
    <div class="drawer-section"><h3>Explainability</h3><p class="mono">Priority Score: ${explain.priority_score}</p><ul class="breakdown-list">${breakdown}</ul><p class="mono">Segment: ${escapeHtml(explain.segment_cluster)}</p><p class="mono">Route: ${escapeHtml(explain.final_route)} | Sequence: ${escapeHtml(explain.sequence)}</p><p class="mono">Reason: ${escapeHtml(explain.route_reason)}</p></div>
    <div class="action-row"><button id="drawer-approve" class="btn btn-primary tiny-btn">Approve</button><button id="drawer-reject" class="btn btn-danger tiny-btn">Reject</button></div>
  `;
  const subjectPreview = drawerContentEl.querySelector<HTMLElement>("#subject-preview");
  const subjectABtn = drawerContentEl.querySelector<HTMLButtonElement>("#subject-a-btn");
  const subjectBBtn = drawerContentEl.querySelector<HTMLButtonElement>("#subject-b-btn");
  const copySubjectBtn = drawerContentEl.querySelector<HTMLButtonElement>("#copy-subject-btn");
  const copyBodyBtn = drawerContentEl.querySelector<HTMLButtonElement>("#copy-body-btn");
  const copyNoteBtn = drawerContentEl.querySelector<HTMLButtonElement>("#copy-note-btn");
  const approveBtn = drawerContentEl.querySelector<HTMLButtonElement>("#drawer-approve");
  const rejectBtn = drawerContentEl.querySelector<HTMLButtonElement>("#drawer-reject");

  subjectABtn?.addEventListener("click", () => {
    if (subjectPreview) subjectPreview.textContent = subjectA;
    subjectABtn.classList.add("active");
    subjectBBtn?.classList.remove("active");
  });
  subjectBBtn?.addEventListener("click", () => {
    if (subjectPreview) subjectPreview.textContent = subjectB;
    subjectBBtn.classList.add("active");
    subjectABtn?.classList.remove("active");
  });
  copySubjectBtn?.addEventListener("click", async () => copyText(subjectPreview?.textContent ?? ""));
  copyBodyBtn?.addEventListener("click", async () => copyText(postBody));
  copyNoteBtn?.addEventListener("click", async () => copyText(note));
  approveBtn?.addEventListener("click", async () => {
    await queueAction(id, "approve");
    await openDrawer(id);
  });
  rejectBtn?.addEventListener("click", async () => {
    await queueAction(id, "reject");
    await openDrawer(id);
  });
  drawerEl.classList.remove("hidden");
  drawerBackdropEl.classList.remove("hidden");
}

function closeDrawer(): void {
  drawerEl.classList.add("hidden");
  drawerBackdropEl.classList.add("hidden");
}

async function downloadDataset(): Promise<void> {
  try {
    const url = `/api/download/${encodeURIComponent(previewSelectEl.value)}?format=${encodeURIComponent(previewFormatEl.value)}`;
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Download failed (${response.status})`);
    const blob = await response.blob();
    const name = readFilename(response.headers.get("Content-Disposition")) ?? `${previewSelectEl.value}.${previewFormatEl.value}`;
    const href = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = href;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(href);
    setFlash(`Downloaded ${name}`, "success");
  } catch (error) {
    setFlash(error instanceof Error ? error.message : "Download failed", "error");
  }
}

function renderTable(container: HTMLElement, rows: Array<Record<string, unknown>>, columns: string[], withActions: boolean): void {
  container.innerHTML = "";
  if (!rows.length) {
    container.textContent = "No rows available.";
    return;
  }
  const cols = columns.slice(0, 10);
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const tbody = document.createElement("tbody");
  thead.innerHTML = `<tr>${cols.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}${withActions ? "<th>actions</th>" : ""}</tr>`;
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    const rowId = String(row.id ?? "");
    tr.dataset.id = rowId;
    tr.innerHTML = `${cols.map((c) => `<td>${escapeHtml(displayTableCell(c, row))}</td>`).join("")}${
      withActions
        ? `<td><div class="action-row"><button class="btn btn-ghost tiny-btn" data-action="view" data-id="${escapeHtml(rowId)}">View</button>${
            String(row.outreach_approved ?? "").toUpperCase() === "PENDING_REVIEW"
              ? `<button class="btn btn-primary tiny-btn" data-action="approve" data-id="${escapeHtml(rowId)}">Approve</button><button class="btn btn-danger tiny-btn" data-action="reject" data-id="${escapeHtml(rowId)}">Reject</button>`
              : ""
          }</div></td>`
        : ""
    }`;
    tbody.appendChild(tr);
  });
  table.appendChild(thead);
  table.appendChild(tbody);
  container.appendChild(table);
}

async function api<T>(url: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json", ...(init?.headers as Record<string, string> | undefined) };
  if (controlToken) headers["X-API-Token"] = controlToken;
  const response = await fetch(url, { ...init, headers });
  const text = await response.text();
  const parsed = text ? (JSON.parse(text) as unknown) : {};
  if (!response.ok) throw new Error(typeof parsed === "object" && parsed && "error" in parsed ? String((parsed as { error: unknown }).error) : `HTTP ${response.status}`);
  return parsed as T;
}

function schedulePoll(): void {
  const ms = latestStatus === "running" ? 2500 : 8000;
  window.setTimeout(async () => {
    const prevStatus = latestStatus;
    try {
      await refreshAll(false);
      if (prevStatus === "running" && latestStatus !== "running") {
        await loadPreview();
      }
    } catch {
      // polling keeps trying
    }
    schedulePoll();
  }, ms);
}

function setFlash(message: string, tone: FlashTone): void {
  flashEl.className = `mono flash ${tone}`;
  flashEl.textContent = message;
}

function readFilename(disposition: string | null): string | null {
  if (!disposition) return null;
  const match = /filename\*?=(?:UTF-8''|\"?)([^\";]+)/i.exec(disposition);
  return match?.[1] ? decodeURIComponent(match[1]) : null;
}

function escapeHtml(value: string): string {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/\"/g, "&quot;").replace(/'/g, "&#39;");
}

const INTERNAL_PLACEHOLDERS = new Set([
  "NOT_FOUND",
  "NOT_ATTEMPTED_FAST_MODE",
  "SKIP_INVALID_NAME",
  "NOT_AVAILABLE",
  "NOT_PUBLICLY_AVAILABLE",
  "DRAFT_ERROR",
  "ERROR",
]);

function friendlyCellValue(value: unknown): string {
  if (value === null || value === undefined) return "Not Publicly Available";
  const raw = String(value).trim();
  if (!raw) return "Not Publicly Available";
  if (INTERNAL_PLACEHOLDERS.has(raw.toUpperCase())) return "Not Publicly Available";
  return raw;
}

function datasetDisplayName(dataset: string): string {
  const mapping: Record<string, string> = {
    raw: "Raw Contacts Dataset",
    enriched: "Enriched Contacts Dataset",
    personas: "Persona Dataset",
    routed: "Routing Dataset",
    outreach: "Outreach Dataset",
  };
  return mapping[dataset] ?? "Dataset";
}

function stageDisplayName(stage: string): string {
  const mapping: Record<string, string> = {
    enrich: "Enrichment",
    persona: "Persona Generation",
    route: "Routing",
    outreach: "Outreach Drafting",
  };
  return mapping[stage] ?? stage;
}

function sanitizeConfig(config: Record<string, unknown>): Record<string, unknown> {
  const safe = JSON.parse(JSON.stringify(config)) as Record<string, unknown>;
  if (typeof safe.input === "string") safe.input = "Input Dataset";
  if (typeof safe.output === "string") safe.output = "Output Dataset";
  return safe;
}

function shouldShowRunLogLine(line: string): boolean {
  const low = line.toLowerCase();
  const noisyTokens = [
    "warning",
    "debug",
    "http request",
    "retrying request to /chat/completions",
    "json parsing failed",
    "truncated from",
    "rate-limited",
    "overriding event_date",
  ];
  if (noisyTokens.some((token) => low.includes(token))) return false;
  return true;
}

function toCount(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, Math.round(parsed));
}

function displayTableCell(column: string, row: Record<string, unknown>): string {
  if (column === "approval_decision") {
    const raw = String(row[column] ?? "").trim().toUpperCase();
    if (raw) return raw;
    const approval = String(row.outreach_approved ?? "").trim().toUpperCase();
    if (approval === "PENDING_REVIEW") return "PENDING_REVIEW";
    if (approval === "YES") return "APPROVED";
    if (approval === "NO") return "NOT_QUEUEABLE";
  }
  return friendlyCellValue(row[column]);
}

function formatMetricCount(value: number, reveal: boolean): string {
  if (!reveal) return ZERO_METRIC_DISPLAY;
  if (!Number.isFinite(value)) return "0";
  return String(Math.max(0, Math.round(value)));
}

async function copyText(value: string): Promise<void> {
  if (!value) {
    setFlash("Nothing to copy.", "error");
    return;
  }
  try {
    await navigator.clipboard.writeText(value);
    setFlash("Copied to clipboard.", "success");
  } catch {
    setFlash("Clipboard copy failed.", "error");
  }
}
