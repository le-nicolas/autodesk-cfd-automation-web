const ui = {
  configText: document.getElementById("configText"),
  casesText: document.getElementById("casesText"),
  logs: document.getElementById("logs"),
  introspection: document.getElementById("introspection"),
  runState: document.getElementById("runState"),
  runMode: document.getElementById("runMode"),
  caseCounter: document.getElementById("caseCounter"),
  currentCase: document.getElementById("currentCase"),
  liveFailureWrap: document.getElementById("liveFailureWrap"),
  resultsLinks: document.getElementById("resultsLinks"),
  resultsTableWrap: document.getElementById("resultsTableWrap"),
  failureWrap: document.getElementById("failureWrap"),
  chartsWrap: document.getElementById("chartsWrap"),
  reloadBtn: document.getElementById("reloadBtn"),
  saveConfigBtn: document.getElementById("saveConfigBtn"),
  saveCasesBtn: document.getElementById("saveCasesBtn"),
  introspectBtn: document.getElementById("introspectBtn"),
  runAllBtn: document.getElementById("runAllBtn"),
  runFailedBtn: document.getElementById("runFailedBtn"),
  runChangedBtn: document.getElementById("runChangedBtn"),
  llmPrompt: document.getElementById("llmPrompt"),
  llmMaxRows: document.getElementById("llmMaxRows"),
  llmPreviewBtn: document.getElementById("llmPreviewBtn"),
  llmApplyBtn: document.getElementById("llmApplyBtn"),
  llmResult: document.getElementById("llmResult"),
  studyPathInput: document.getElementById("studyPathInput"),
  discoverStudiesBtn: document.getElementById("discoverStudiesBtn"),
  applyStudyPathBtn: document.getElementById("applyStudyPathBtn"),
  studyCandidates: document.getElementById("studyCandidates"),
  useSelectedStudyBtn: document.getElementById("useSelectedStudyBtn"),
  solveBanner: document.getElementById("solveBanner"),
  authBanner: document.getElementById("authBanner"),
  apiKeyInput: document.getElementById("apiKeyInput"),
};

let currentConfig = null;

function getApiKey() {
  const key = (ui.apiKeyInput.value || localStorage.getItem("cfd_api_key") || "").trim();
  return key;
}

function persistApiKey() {
  const key = getApiKey();
  if (key) {
    localStorage.setItem("cfd_api_key", key);
  } else {
    localStorage.removeItem("cfd_api_key");
  }
}

async function callApi(url, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (options.method && options.method !== "GET" && options.method !== "HEAD") {
    headers["Content-Type"] = "application/json";
  }
  const apiKey = getApiKey();
  if (apiKey) {
    headers["X-API-Key"] = apiKey;
  }

  const response = await fetch(url, {
    ...options,
    headers,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || `Request failed: ${url}`);
  }
  return payload;
}

function flash(message) {
  const stamp = new Date().toISOString();
  ui.logs.textContent = `[${stamp}] ${message}\n` + ui.logs.textContent;
}

function updateSolveBanner() {
  const solveEnabled = Boolean(currentConfig && currentConfig.solve && currentConfig.solve.enabled);
  ui.solveBanner.classList.toggle("hidden", solveEnabled);
}

function updateStatusView(status) {
  const running = Boolean(status.running);
  ui.runState.textContent = running ? "Running" : "Idle";
  ui.runState.className = `pill ${running ? "running" : "idle"}`;
  ui.runMode.textContent = `Mode: ${status.mode || "-"}`;
  ui.caseCounter.textContent = `${status.completed_case_count || 0} / ${status.selected_case_count || 0}`;
  ui.currentCase.textContent = `Current: ${status.current_case || "-"}`;

  const logLines = (status.logs || []).slice().reverse();
  if (status.last_error) {
    logLines.unshift(`[ERROR] ${status.last_error}`);
  }
  ui.logs.textContent = logLines.join("\n");

  ui.authBanner.classList.toggle("hidden", !status.auth_required);
  renderLiveFailures(status.recent_failures || []);
}

function renderLiveFailures(items) {
  ui.liveFailureWrap.innerHTML = "";
  if (!items.length) return;
  const latest = items.slice(-3).reverse();
  for (const item of latest) {
    const div = document.createElement("div");
    div.className = "failure-item";
    div.textContent = `${item.case_id} (attempt ${item.attempt}): ${item.reason || "Unknown failure"}`;
    ui.liveFailureWrap.appendChild(div);
  }
}

function renderLinks(summary) {
  ui.resultsLinks.innerHTML = "";
  const links = [];
  const results = summary.results || {};

  if (results.master_csv_url) links.push(["Master CSV", results.master_csv_url]);
  if (results.ranked_csv_url) links.push(["Ranked CSV", results.ranked_csv_url]);
  if (results.report_md_url) links.push(["Report MD", results.report_md_url]);
  if (results.report_html_url) links.push(["Report HTML", results.report_html_url]);

  for (const [label, href] of links) {
    const a = document.createElement("a");
    a.href = href;
    a.target = "_blank";
    a.rel = "noreferrer";
    a.textContent = label;
    ui.resultsLinks.appendChild(a);
  }
}

function renderResultsTable(summary) {
  const rows = summary.case_results || [];
  ui.resultsTableWrap.innerHTML = "";
  if (!rows.length) {
    ui.resultsTableWrap.textContent = "No run results yet.";
    return;
  }
  const metricKeys = new Set();
  for (const row of rows) {
    const metrics = row.metrics || {};
    Object.keys(metrics).forEach((key) => metricKeys.add(key));
  }
  const columns = ["case_id", "success", "failure_reason", ...Array.from(metricKeys)];

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trHead = document.createElement("tr");
  for (const column of columns) {
    const th = document.createElement("th");
    th.textContent = column;
    trHead.appendChild(th);
  }
  thead.appendChild(trHead);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const column of columns) {
      const td = document.createElement("td");
      if (column === "success") {
        td.textContent = row.success ? "true" : "false";
      } else if (column === "failure_reason") {
        td.textContent = row.failure_reason || row.error || "";
      } else if (column in row) {
        td.textContent = row[column] ?? "";
      } else {
        td.textContent = (row.metrics || {})[column] ?? "";
      }
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  ui.resultsTableWrap.appendChild(table);
}

function renderFailureDetails(summary) {
  ui.failureWrap.innerHTML = "";
  const rows = summary.case_results || [];
  const failed = rows.filter((row) => !row.success);
  if (!failed.length) {
    return;
  }
  const title = document.createElement("h3");
  title.textContent = "Failure Reasons";
  ui.failureWrap.appendChild(title);

  for (const row of failed) {
    const div = document.createElement("div");
    div.className = "failure-item";
    const reason = row.failure_reason || row.error || "Unknown failure";
    div.textContent = `${row.case_id}: ${reason}`;
    ui.failureWrap.appendChild(div);
  }
}

function renderCharts(summary) {
  ui.chartsWrap.innerHTML = "";
  const chartUrls = (summary.results && summary.results.chart_urls) || [];
  for (const chartUrl of chartUrls) {
    if (!chartUrl) continue;
    const img = document.createElement("img");
    img.src = chartUrl;
    img.loading = "lazy";
    ui.chartsWrap.appendChild(img);
  }
}

function renderSummary(summary) {
  renderLinks(summary);
  renderResultsTable(summary);
  renderFailureDetails(summary);
  renderCharts(summary);
}

function syncStudyPathInput() {
  const path = (currentConfig && currentConfig.study && currentConfig.study.template_model) || "";
  ui.studyPathInput.value = path;
}

function applyStudyPathIntoConfig(pathValue) {
  const path = (pathValue || "").trim();
  const parsed = JSON.parse(ui.configText.value);
  if (!parsed.study || typeof parsed.study !== "object") {
    parsed.study = {};
  }
  parsed.study.template_model = path;
  ui.configText.value = JSON.stringify(parsed, null, 2);
  currentConfig = parsed;
  updateSolveBanner();
}

function fillStudyCandidates(studies) {
  ui.studyCandidates.innerHTML = "";
  for (const item of studies) {
    const option = document.createElement("option");
    option.value = item.path;
    const dateText = item.modified_epoch
      ? new Date(item.modified_epoch * 1000).toLocaleString()
      : "-";
    option.textContent = `${item.path}  (modified: ${dateText})`;
    ui.studyCandidates.appendChild(option);
  }
}

async function loadConfig() {
  const config = await callApi("/api/config");
  currentConfig = config;
  ui.configText.value = JSON.stringify(config, null, 2);
  const llmMaxRows = config && config.llm ? config.llm.max_rows : "";
  ui.llmMaxRows.value = llmMaxRows ? String(llmMaxRows) : "";
  syncStudyPathInput();
  updateSolveBanner();
}

async function saveConfig() {
  const parsed = JSON.parse(ui.configText.value);
  await callApi("/api/config", {
    method: "POST",
    body: JSON.stringify(parsed),
  });
  currentConfig = parsed;
  syncStudyPathInput();
  updateSolveBanner();
  flash("Config saved.");
}

async function loadCases() {
  const payload = await callApi("/api/cases");
  ui.casesText.value = payload.csv || "";
}

async function saveCases() {
  await callApi("/api/cases", {
    method: "POST",
    body: JSON.stringify({ csv: ui.casesText.value }),
  });
  flash("Cases CSV saved.");
}

function renderLlmResult(payload, applied) {
  const lines = [];
  lines.push(`Provider: ${payload.provider || "-"}`);
  lines.push(`Model: ${payload.model || "-"}`);
  lines.push(`Rows generated: ${payload.row_count || 0}`);
  lines.push(`Applied to cases.csv: ${applied ? "yes" : "no"}`);
  if (payload.notes) {
    lines.push("");
    lines.push("Notes:");
    lines.push(payload.notes);
  }
  ui.llmResult.textContent = lines.join("\n");
}

async function generateCasesFromPrompt(apply) {
  const prompt = (ui.llmPrompt.value || "").trim();
  if (!prompt) {
    throw new Error("Prompt is empty.");
  }
  const maxRowsText = (ui.llmMaxRows.value || "").trim();
  const body = { prompt, apply };
  if (maxRowsText) {
    const parsed = Number.parseInt(maxRowsText, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new Error("Max rows must be a positive integer.");
    }
    body.max_rows = parsed;
  }
  const payload = await callApi("/api/llm/generate-cases", {
    method: "POST",
    body: JSON.stringify(body),
  });
  ui.casesText.value = payload.csv || ui.casesText.value;
  renderLlmResult(payload, apply);
  flash(`LLM generated ${payload.row_count || 0} row(s).`);
}

async function discoverStudies() {
  const payload = await callApi("/api/studies");
  fillStudyCandidates(payload.studies || []);
  flash(`Discovered ${payload.count || 0} study file(s).`);
}

async function runIntrospection() {
  ui.introspection.textContent = "Running introspection...";
  const payload = await callApi("/api/introspect", {
    method: "POST",
    body: JSON.stringify({ study_path: ui.studyPathInput.value.trim() || undefined }),
  });
  ui.introspection.textContent = JSON.stringify(payload.result.data || {}, null, 2);
  flash("Introspection completed.");
}

async function startRun(mode) {
  const payload = await callApi("/api/run", {
    method: "POST",
    body: JSON.stringify({ mode }),
  });
  flash(payload.message || `Run ${mode} triggered.`);
}

async function refreshStatus() {
  const status = await callApi("/api/status");
  updateStatusView(status);
}

async function refreshLatestRun() {
  const summary = await callApi("/api/latest-run");
  renderSummary(summary);
}

async function boot() {
  try {
    await Promise.all([loadConfig(), loadCases(), refreshStatus(), refreshLatestRun()]);
  } catch (err) {
    flash(`Initial load failed: ${err.message}`);
  }
}

ui.apiKeyInput.value = localStorage.getItem("cfd_api_key") || "";
ui.apiKeyInput.addEventListener("change", persistApiKey);
ui.apiKeyInput.addEventListener("keyup", (event) => {
  if (event.key === "Enter") {
    persistApiKey();
    flash("API key updated.");
  }
});

ui.reloadBtn.addEventListener("click", async () => {
  await boot();
  flash("Reloaded config, cases, status, and latest run.");
});

ui.saveConfigBtn.addEventListener("click", async () => {
  try {
    await saveConfig();
  } catch (err) {
    flash(`Save config failed: ${err.message}`);
  }
});

ui.saveCasesBtn.addEventListener("click", async () => {
  try {
    await saveCases();
  } catch (err) {
    flash(`Save cases failed: ${err.message}`);
  }
});

ui.discoverStudiesBtn.addEventListener("click", async () => {
  try {
    await discoverStudies();
  } catch (err) {
    flash(`Study discovery failed: ${err.message}`);
  }
});

ui.applyStudyPathBtn.addEventListener("click", () => {
  try {
    applyStudyPathIntoConfig(ui.studyPathInput.value);
    flash("Study path applied to config editor. Click 'Save Config' to persist.");
  } catch (err) {
    flash(`Apply study path failed: ${err.message}`);
  }
});

ui.useSelectedStudyBtn.addEventListener("click", () => {
  const selected = ui.studyCandidates.value;
  if (!selected) {
    flash("No discovered study selected.");
    return;
  }
  ui.studyPathInput.value = selected;
  try {
    applyStudyPathIntoConfig(selected);
    flash("Selected discovered study path applied to config editor.");
  } catch (err) {
    flash(`Use selected study failed: ${err.message}`);
  }
});

ui.introspectBtn.addEventListener("click", async () => {
  try {
    await runIntrospection();
  } catch (err) {
    ui.introspection.textContent = `Introspection failed: ${err.message}`;
    flash(`Introspection failed: ${err.message}`);
  }
});

ui.runAllBtn.addEventListener("click", async () => {
  try {
    await startRun("all");
  } catch (err) {
    flash(`Run all failed: ${err.message}`);
  }
});

ui.runFailedBtn.addEventListener("click", async () => {
  try {
    await startRun("failed");
  } catch (err) {
    flash(`Rerun failed failed: ${err.message}`);
  }
});

ui.runChangedBtn.addEventListener("click", async () => {
  try {
    await startRun("changed");
  } catch (err) {
    flash(`Rerun changed failed: ${err.message}`);
  }
});

ui.llmPreviewBtn.addEventListener("click", async () => {
  try {
    await generateCasesFromPrompt(false);
  } catch (err) {
    flash(`LLM preview failed: ${err.message}`);
    ui.llmResult.textContent = `LLM preview failed: ${err.message}`;
  }
});

ui.llmApplyBtn.addEventListener("click", async () => {
  try {
    await generateCasesFromPrompt(true);
    flash("Generated cases applied to cases.csv.");
  } catch (err) {
    flash(`LLM apply failed: ${err.message}`);
    ui.llmResult.textContent = `LLM apply failed: ${err.message}`;
  }
});

setInterval(async () => {
  try {
    await refreshStatus();
    await refreshLatestRun();
  } catch (err) {
    flash(`Auto-refresh failed: ${err.message}`);
  }
}, 1000);

boot();
