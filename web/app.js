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
  meshPrompt: document.getElementById("meshPrompt"),
  meshSuggestBtn: document.getElementById("meshSuggestBtn"),
  meshApplyBtn: document.getElementById("meshApplyBtn"),
  meshResult: document.getElementById("meshResult"),
  loopObjectiveAlias: document.getElementById("loopObjectiveAlias"),
  loopObjectiveGoal: document.getElementById("loopObjectiveGoal"),
  loopBatchSize: document.getElementById("loopBatchSize"),
  loopMaxBatches: document.getElementById("loopMaxBatches"),
  loopSearchSpace: document.getElementById("loopSearchSpace"),
  loopConstraints: document.getElementById("loopConstraints"),
  loopFixedValues: document.getElementById("loopFixedValues"),
  loopStartBtn: document.getElementById("loopStartBtn"),
  loopStopBtn: document.getElementById("loopStopBtn"),
  loopStatus: document.getElementById("loopStatus"),
  studyPathInput: document.getElementById("studyPathInput"),
  discoverStudiesBtn: document.getElementById("discoverStudiesBtn"),
  applyStudyPathBtn: document.getElementById("applyStudyPathBtn"),
  studyCandidates: document.getElementById("studyCandidates"),
  useSelectedStudyBtn: document.getElementById("useSelectedStudyBtn"),
  solveBanner: document.getElementById("solveBanner"),
  authBanner: document.getElementById("authBanner"),
  apiKeyInput: document.getElementById("apiKeyInput"),
  surrogateObjectiveAlias: document.getElementById("surrogateObjectiveAlias"),
  surrogateObjectiveGoal: document.getElementById("surrogateObjectiveGoal"),
  surrogateSampleCount: document.getElementById("surrogateSampleCount"),
  surrogateTopN: document.getElementById("surrogateTopN"),
  surrogateValidateTopN: document.getElementById("surrogateValidateTopN"),
  surrogateMinRows: document.getElementById("surrogateMinRows"),
  surrogateSearchSpace: document.getElementById("surrogateSearchSpace"),
  surrogateConstraints: document.getElementById("surrogateConstraints"),
  surrogateFixedValues: document.getElementById("surrogateFixedValues"),
  surrogateTrainBtn: document.getElementById("surrogateTrainBtn"),
  surrogateRefreshBtn: document.getElementById("surrogateRefreshBtn"),
  surrogatePredictBtn: document.getElementById("surrogatePredictBtn"),
  surrogateValidateBtn: document.getElementById("surrogateValidateBtn"),
  surrogateStatus: document.getElementById("surrogateStatus"),
  surrogatePredictOutput: document.getElementById("surrogatePredictOutput"),
  surrogateCoverage: document.getElementById("surrogateCoverage"),
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
    const typeText = item.failure_type ? `[${item.failure_type}] ` : "";
    const modeText = item.failure_mode ? ` (${item.failure_mode})` : "";
    div.textContent = `${item.case_id} (attempt ${item.attempt}) ${typeText}${item.reason || "Unknown failure"}${modeText}`;
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

  const rows = Array.isArray(summary.case_results) ? summary.case_results : [];
  const caseAssets = rows
    .map((row) => {
      const screenshots = Array.isArray(row.screenshot_urls)
        ? row.screenshot_urls.filter((url) => Boolean(url))
        : [];
      const linksForCase = [];
      if (row.summary_csv_url) linksForCase.push(["Summary CSV", row.summary_csv_url]);
      if (row.metrics_csv_url) linksForCase.push(["Metrics CSV", row.metrics_csv_url]);
      screenshots.forEach((url, idx) => linksForCase.push([`Screenshot ${idx + 1}`, url]));
      return {
        caseId: String(row.case_id || ""),
        links: linksForCase,
      };
    })
    .filter((item) => item.links.length > 0);

  if (!caseAssets.length) {
    return;
  }

  const heading = document.createElement("div");
  heading.className = "helper";
  heading.textContent = "Per-case output files:";
  ui.resultsLinks.appendChild(heading);

  for (const item of caseAssets) {
    const rowWrap = document.createElement("div");
    rowWrap.className = "helper";
    rowWrap.appendChild(document.createTextNode(`${item.caseId || "case"}: `));
    item.links.forEach(([label, href], idx) => {
      const a = document.createElement("a");
      a.href = href;
      a.target = "_blank";
      a.rel = "noreferrer";
      a.textContent = label;
      rowWrap.appendChild(a);
      if (idx < item.links.length - 1) {
        rowWrap.appendChild(document.createTextNode(" | "));
      }
    });
    ui.resultsLinks.appendChild(rowWrap);
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
  const columns = ["case_id", "success", "failure_type", "failure_reason", ...Array.from(metricKeys)];

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
      } else if (column === "failure_type") {
        td.textContent = row.failure_type || "";
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
    const typeText = row.failure_type ? ` [${row.failure_type}]` : "";
    div.textContent = `${row.case_id}${typeText}: ${reason}`;
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

   if (!ui.loopObjectiveAlias.value) {
    const ranking = (config && config.ranking) || [];
    if (ranking.length && ranking[0].alias) {
      ui.loopObjectiveAlias.value = String(ranking[0].alias);
      ui.loopObjectiveGoal.value = String(ranking[0].goal || "min").toLowerCase() === "max" ? "max" : "min";
    }
  }
  if (!ui.loopBatchSize.value) {
    const loopCfg = (config && config.design_loop) || {};
    if (loopCfg.batch_size_default) ui.loopBatchSize.value = String(loopCfg.batch_size_default);
    if (loopCfg.max_batches_default) ui.loopMaxBatches.value = String(loopCfg.max_batches_default);
  }
  if (!ui.loopSearchSpace.value) {
    ui.loopSearchSpace.value = JSON.stringify(
      [
        { name: "fin_height_mm", type: "real", min: 5, max: 20 },
        { name: "fin_spacing_mm", type: "real", min: 2, max: 10 },
        { name: "flow_rate_lpm", type: "real", min: 1, max: 5 },
      ],
      null,
      2
    );
  }
  if (!ui.loopConstraints.value) {
    const criteria = (config && config.criteria) || [];
    ui.loopConstraints.value = JSON.stringify(criteria, null, 2);
  }
  if (!ui.loopFixedValues.value) {
    ui.loopFixedValues.value = JSON.stringify({}, null, 2);
  }
  if (!ui.surrogateObjectiveAlias.value) {
    const ranking = (config && config.ranking) || [];
    if (ranking.length && ranking[0].alias) {
      ui.surrogateObjectiveAlias.value = String(ranking[0].alias);
      ui.surrogateObjectiveGoal.value = String(ranking[0].goal || "min").toLowerCase() === "max" ? "max" : "min";
    }
  }
  if (!ui.surrogateSampleCount.value) {
    ui.surrogateSampleCount.value = "10000";
  }
  if (!ui.surrogateTopN.value) {
    ui.surrogateTopN.value = "25";
  }
  if (!ui.surrogateValidateTopN.value) {
    ui.surrogateValidateTopN.value = "3";
  }
  if (!ui.surrogateMinRows.value) {
    ui.surrogateMinRows.value = "50";
  }
  if (!ui.surrogateSearchSpace.value) {
    ui.surrogateSearchSpace.value = JSON.stringify(
      [
        { name: "inlet_velocity_ms", type: "real", min: 1, max: 5 },
        { name: "ambient_temp_c", type: "real", min: 20, max: 40 },
        { name: "total_heat_w", type: "real", min: 50, max: 120 },
      ],
      null,
      2
    );
  }
  if (!ui.surrogateConstraints.value) {
    ui.surrogateConstraints.value = JSON.stringify((config && config.criteria) || [], null, 2);
  }
  if (!ui.surrogateFixedValues.value) {
    ui.surrogateFixedValues.value = JSON.stringify({}, null, 2);
  }
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

function renderMeshSuggestion(payload, apply) {
  const lines = [];
  lines.push(`Provider: ${payload.provider || "-"}`);
  lines.push(`Model: ${payload.model || "-"}`);
  lines.push(`Applied to config: ${apply ? "yes" : "no"}`);
  lines.push("");
  lines.push("Mesh Parameters:");
  lines.push(JSON.stringify(payload.mesh_params || {}, null, 2));
  lines.push("");
  lines.push("Quality Gate:");
  lines.push(JSON.stringify(payload.quality_gate || {}, null, 2));
  if (payload.notes) {
    lines.push("");
    lines.push("Notes:");
    lines.push(payload.notes);
  }
  ui.meshResult.textContent = lines.join("\n");
}

async function suggestMeshWithLlm(apply) {
  const prompt = (ui.meshPrompt.value || "").trim();
  const payload = await callApi("/api/llm/suggest-mesh", {
    method: "POST",
    body: JSON.stringify({
      prompt,
      apply,
    }),
  });
  renderMeshSuggestion(payload, apply);
  if (apply && payload.config) {
    currentConfig = payload.config;
    ui.configText.value = JSON.stringify(payload.config, null, 2);
    updateSolveBanner();
  }
  flash(`LLM mesh suggestion generated${apply ? " and applied" : ""}.`);
}

function parseJsonField(text, fieldName, fallback) {
  const trimmed = (text || "").trim();
  if (!trimmed) return fallback;
  try {
    return JSON.parse(trimmed);
  } catch (_err) {
    throw new Error(`${fieldName} is not valid JSON.`);
  }
}

function buildDesignLoopPayload() {
  const objectiveAlias = (ui.loopObjectiveAlias.value || "").trim();
  if (!objectiveAlias) {
    throw new Error("Objective alias is required.");
  }
  const objectiveGoal = (ui.loopObjectiveGoal.value || "min").trim().toLowerCase() === "max" ? "max" : "min";
  const batchSizeText = (ui.loopBatchSize.value || "").trim();
  const maxBatchesText = (ui.loopMaxBatches.value || "").trim();
  const batchSize = batchSizeText ? Number.parseInt(batchSizeText, 10) : undefined;
  const maxBatches = maxBatchesText ? Number.parseInt(maxBatchesText, 10) : undefined;
  if (batchSize !== undefined && (!Number.isFinite(batchSize) || batchSize <= 0)) {
    throw new Error("Batch size must be a positive integer.");
  }
  if (maxBatches !== undefined && (!Number.isFinite(maxBatches) || maxBatches <= 0)) {
    throw new Error("Max batches must be a positive integer.");
  }

  const searchSpace = parseJsonField(ui.loopSearchSpace.value, "Search Space", []);
  const constraints = parseJsonField(ui.loopConstraints.value, "Constraints", []);
  const fixedValues = parseJsonField(ui.loopFixedValues.value, "Fixed Values", {});

  if (!Array.isArray(searchSpace) || !searchSpace.length) {
    throw new Error("Search Space must be a non-empty JSON array.");
  }
  if (!Array.isArray(constraints)) {
    throw new Error("Constraints must be a JSON array.");
  }
  if (typeof fixedValues !== "object" || Array.isArray(fixedValues) || fixedValues === null) {
    throw new Error("Fixed Values must be a JSON object.");
  }

  return {
    objective_alias: objectiveAlias,
    objective_goal: objectiveGoal,
    batch_size: batchSize,
    max_batches: maxBatches,
    search_space: searchSpace,
    constraints,
    fixed_values: fixedValues,
  };
}

function renderDesignLoopStatus(payload) {
  const lines = [];
  lines.push(`Running: ${payload.running ? "yes" : "no"}`);
  lines.push(`Status: ${payload.status || "-"}`);
  lines.push(`Loop ID: ${payload.loop_id || "-"}`);
  lines.push(`Batch: ${payload.current_batch || 0} / ${payload.max_batches || 0}`);
  lines.push(`Completed Batches: ${payload.completed_batches || 0}`);
  if (payload.last_error) {
    lines.push(`Last Error: ${payload.last_error}`);
  }

  const summary = payload.last_summary || {};
  if (summary.best_case) {
    lines.push("");
    lines.push("Best Case:");
    lines.push(JSON.stringify(summary.best_case, null, 2));
  }

  const logs = (payload.logs || []).slice(-12);
  if (logs.length) {
    lines.push("");
    lines.push("Recent Logs:");
    for (const line of logs) lines.push(line);
  }

  ui.loopStatus.textContent = lines.join("\n");
}

async function startDesignLoop() {
  const body = buildDesignLoopPayload();
  const payload = await callApi("/api/design-loop/start", {
    method: "POST",
    body: JSON.stringify(body),
  });
  flash(payload.message || "Design loop started.");
}

async function stopDesignLoop() {
  const payload = await callApi("/api/design-loop/stop", {
    method: "POST",
    body: JSON.stringify({}),
  });
  flash(payload.message || "Design loop stop requested.");
}

async function refreshDesignLoopStatus() {
  const payload = await callApi("/api/design-loop/status");
  renderDesignLoopStatus(payload);
}

function renderCoverageMap(coveragePayload) {
  if (!coveragePayload || !coveragePayload.map || !Array.isArray(coveragePayload.map.cells)) {
    ui.surrogateCoverage.textContent = "Coverage map unavailable.";
    return;
  }
  const map = coveragePayload.map;
  const cells = map.cells || [];
  if (!cells.length) {
    ui.surrogateCoverage.textContent = "Coverage map unavailable.";
    return;
  }
  const symbols = { 0: "░", 1: "▓", 2: "█" };
  const lines = [];
  lines.push(`Coverage Map: ${map.y_feature || "-"} vs ${map.x_feature || "-"}`);
  for (const row of cells) {
    const chars = row.map((value) => symbols[value] || "░").join("");
    lines.push(chars);
  }
  lines.push("Legend: █ high  ▓ medium  ░ low");
  ui.surrogateCoverage.textContent = lines.join("\n");
}

function renderSurrogateStatus(statusPayload, coveragePayload) {
  const result = (statusPayload && statusPayload.result) || {};
  const lines = [];
  lines.push(`Trained: ${result.trained ? "yes" : "no"}`);
  lines.push(`Ready: ${result.ready ? "yes" : "no"}`);
  lines.push(`Model: ${result.model_name || "-"}`);
  lines.push(`Objective alias: ${result.target_alias || "-"}`);
  lines.push(`Rows: ${result.row_count || 0}`);
  const r2 = Number(result.best_r2);
  if (Number.isFinite(r2)) {
    lines.push(`R2: ${r2.toFixed(4)}`);
  }
  const coverageOverall = Number(result.coverage && result.coverage.overall);
  if (Number.isFinite(coverageOverall)) {
    lines.push(`Coverage: ${(coverageOverall * 100).toFixed(1)}%`);
  }
  if (result.message) {
    lines.push(`Info: ${result.message}`);
  }
  if (result.training_data_csv) {
    lines.push(`Training data: ${result.training_data_csv}`);
  }
  ui.surrogateStatus.textContent = lines.join("\n");
  renderCoverageMap((coveragePayload && coveragePayload.result) || {});
}

function buildSurrogatePayload() {
  const objectiveAlias = (ui.surrogateObjectiveAlias.value || "").trim();
  if (!objectiveAlias) {
    throw new Error("Surrogate objective alias is required.");
  }
  const objectiveGoal = (ui.surrogateObjectiveGoal.value || "min").trim().toLowerCase() === "max" ? "max" : "min";
  const sampleCount = Number.parseInt((ui.surrogateSampleCount.value || "10000").trim(), 10);
  const topN = Number.parseInt((ui.surrogateTopN.value || "25").trim(), 10);
  const validateTopN = Number.parseInt((ui.surrogateValidateTopN.value || "3").trim(), 10);
  const minRows = Number.parseInt((ui.surrogateMinRows.value || "50").trim(), 10);
  if (!Number.isFinite(sampleCount) || sampleCount <= 0) {
    throw new Error("Sample count must be a positive integer.");
  }
  if (!Number.isFinite(topN) || topN <= 0) {
    throw new Error("Top N must be a positive integer.");
  }
  if (!Number.isFinite(validateTopN) || validateTopN <= 0) {
    throw new Error("Validate Top N must be a positive integer.");
  }
  if (!Number.isFinite(minRows) || minRows <= 0) {
    throw new Error("Train min rows must be a positive integer.");
  }

  const searchSpace = parseJsonField(ui.surrogateSearchSpace.value, "Surrogate Search Space", []);
  const constraints = parseJsonField(ui.surrogateConstraints.value, "Surrogate Constraints", []);
  const fixedValues = parseJsonField(ui.surrogateFixedValues.value, "Surrogate Fixed Values", {});
  if (!Array.isArray(searchSpace) || !searchSpace.length) {
    throw new Error("Surrogate Search Space must be a non-empty JSON array.");
  }
  if (!Array.isArray(constraints)) {
    throw new Error("Surrogate Constraints must be a JSON array.");
  }
  if (typeof fixedValues !== "object" || fixedValues === null || Array.isArray(fixedValues)) {
    throw new Error("Surrogate Fixed Values must be a JSON object.");
  }

  return {
    objective_alias: objectiveAlias,
    objective_goal: objectiveGoal,
    sample_count: sampleCount,
    top_n: topN,
    validate_top_n: validateTopN,
    min_rows: minRows,
    search_space: searchSpace,
    constraints,
    fixed_values: fixedValues,
  };
}

async function refreshSurrogateStatus() {
  const [statusPayload, coveragePayload] = await Promise.all([
    callApi("/api/surrogate/status"),
    callApi("/api/surrogate/coverage"),
  ]);
  renderSurrogateStatus(statusPayload, coveragePayload);
}

async function trainSurrogate() {
  const body = buildSurrogatePayload();
  const payload = await callApi("/api/surrogate/train", {
    method: "POST",
    body: JSON.stringify({
      objective_alias: body.objective_alias,
      min_rows: body.min_rows,
      include_design_loops: true,
    }),
  });
  flash(`Surrogate training complete: ${payload.result.model_name || "model selected"}.`);
  await refreshSurrogateStatus();
}

async function predictSurrogate() {
  const body = buildSurrogatePayload();
  const payload = await callApi("/api/surrogate/predict", {
    method: "POST",
    body: JSON.stringify(body),
  });
  const result = payload.result || {};
  const lines = [];
  lines.push(`Rows evaluated: ${result.rows_evaluated || result.sample_count || 0}`);
  lines.push(`Model: ${result.model_name || "-"}`);
  if (Number.isFinite(result.best_r2)) {
    lines.push(`R2: ${Number(result.best_r2).toFixed(4)}`);
  }
  lines.push(`Low-confidence cases: ${result.low_confidence_count || 0}`);
  lines.push("");
  lines.push("Top candidates:");
  const top = Array.isArray(result.top_candidates) ? result.top_candidates : [];
  if (!top.length) {
    lines.push("(none)");
  } else {
    for (const item of top.slice(0, 20)) {
      lines.push(
        `#${item.rank || "-"} ${item.case_id || "-"} ` +
        `pred=${item.prediction} conf=${(Number(item.confidence || 0) * 100).toFixed(1)}% ` +
        `(${item.confidence_level || "low"})`
      );
      lines.push(`  params=${JSON.stringify(item.params || {})}`);
      if (Array.isArray(item.constraint_violations) && item.constraint_violations.length) {
        lines.push(`  violations=${item.constraint_violations.join("; ")}`);
      }
    }
  }
  if (Array.isArray(result.warnings) && result.warnings.length) {
    lines.push("");
    lines.push("Warnings:");
    result.warnings.slice(0, 10).forEach((line) => lines.push(`- ${line}`));
  }
  ui.surrogatePredictOutput.textContent = lines.join("\n");
  flash(`Surrogate predicted ${result.rows_evaluated || 0} combinations.`);
}

async function validateSurrogate() {
  const body = buildSurrogatePayload();
  const payload = await callApi("/api/run", {
    method: "POST",
    body: JSON.stringify({
      mode: "validate",
      objective_alias: body.objective_alias,
      objective_goal: body.objective_goal,
      sample_count: body.sample_count,
      top_n: body.top_n,
      validate_top_n: body.validate_top_n,
      search_space: body.search_space,
      constraints: body.constraints,
      fixed_values: body.fixed_values,
      auto_retrain: true,
      retrain_min_rows: body.min_rows,
    }),
  });
  flash(payload.message || "Validate mode started.");
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
    await Promise.all([
      loadConfig(),
      loadCases(),
      refreshStatus(),
      refreshLatestRun(),
      refreshDesignLoopStatus(),
      refreshSurrogateStatus(),
    ]);
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

ui.meshSuggestBtn.addEventListener("click", async () => {
  try {
    await suggestMeshWithLlm(false);
  } catch (err) {
    flash(`Mesh suggestion failed: ${err.message}`);
    ui.meshResult.textContent = `Mesh suggestion failed: ${err.message}`;
  }
});

ui.meshApplyBtn.addEventListener("click", async () => {
  try {
    await suggestMeshWithLlm(true);
  } catch (err) {
    flash(`Mesh apply failed: ${err.message}`);
    ui.meshResult.textContent = `Mesh apply failed: ${err.message}`;
  }
});

ui.loopStartBtn.addEventListener("click", async () => {
  try {
    await startDesignLoop();
    await refreshDesignLoopStatus();
  } catch (err) {
    flash(`Start design loop failed: ${err.message}`);
    ui.loopStatus.textContent = `Start design loop failed: ${err.message}`;
  }
});

ui.loopStopBtn.addEventListener("click", async () => {
  try {
    await stopDesignLoop();
    await refreshDesignLoopStatus();
  } catch (err) {
    flash(`Stop design loop failed: ${err.message}`);
    ui.loopStatus.textContent = `Stop design loop failed: ${err.message}`;
  }
});

ui.surrogateTrainBtn.addEventListener("click", async () => {
  try {
    await trainSurrogate();
  } catch (err) {
    flash(`Surrogate train failed: ${err.message}`);
    ui.surrogateStatus.textContent = `Surrogate train failed: ${err.message}`;
  }
});

ui.surrogateRefreshBtn.addEventListener("click", async () => {
  try {
    await refreshSurrogateStatus();
    flash("Surrogate status refreshed.");
  } catch (err) {
    flash(`Surrogate refresh failed: ${err.message}`);
  }
});

ui.surrogatePredictBtn.addEventListener("click", async () => {
  try {
    await predictSurrogate();
  } catch (err) {
    flash(`Surrogate predict failed: ${err.message}`);
    ui.surrogatePredictOutput.textContent = `Surrogate predict failed: ${err.message}`;
  }
});

ui.surrogateValidateBtn.addEventListener("click", async () => {
  try {
    await validateSurrogate();
  } catch (err) {
    flash(`Surrogate validate failed: ${err.message}`);
    ui.surrogatePredictOutput.textContent = `Surrogate validate failed: ${err.message}`;
  }
});

setInterval(async () => {
  try {
    await refreshStatus();
    await refreshLatestRun();
    await refreshDesignLoopStatus();
    await refreshSurrogateStatus();
  } catch (err) {
    flash(`Auto-refresh failed: ${err.message}`);
  }
}, 1000);

boot();
