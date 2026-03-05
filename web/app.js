const ui = {
  configText: document.getElementById("configText"),
  casesText: document.getElementById("casesText"),
  logs: document.getElementById("logs"),
  introspection: document.getElementById("introspection"),
  runState: document.getElementById("runState"),
  runMode: document.getElementById("runMode"),
  caseCounter: document.getElementById("caseCounter"),
  currentCase: document.getElementById("currentCase"),
  resultsLinks: document.getElementById("resultsLinks"),
  resultsTableWrap: document.getElementById("resultsTableWrap"),
  chartsWrap: document.getElementById("chartsWrap"),
  reloadBtn: document.getElementById("reloadBtn"),
  saveConfigBtn: document.getElementById("saveConfigBtn"),
  saveCasesBtn: document.getElementById("saveCasesBtn"),
  introspectBtn: document.getElementById("introspectBtn"),
  runAllBtn: document.getElementById("runAllBtn"),
  runFailedBtn: document.getElementById("runFailedBtn"),
  runChangedBtn: document.getElementById("runChangedBtn"),
};

async function callApi(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
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

function updateStatusView(status) {
  const running = Boolean(status.running);
  ui.runState.textContent = running ? "Running" : "Idle";
  ui.runState.className = `pill ${running ? "running" : "idle"}`;
  ui.runMode.textContent = `Mode: ${status.mode || "-"}`;
  ui.caseCounter.textContent = `${status.completed_case_count || 0} / ${status.selected_case_count || 0}`;
  ui.currentCase.textContent = `Current: ${status.current_case || "-"}`;
  ui.logs.textContent = (status.logs || []).slice().reverse().join("\n");
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
  const columns = ["case_id", "success", ...Array.from(metricKeys)];

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
  renderCharts(summary);
}

async function loadConfig() {
  const config = await callApi("/api/config");
  ui.configText.value = JSON.stringify(config, null, 2);
}

async function saveConfig() {
  const parsed = JSON.parse(ui.configText.value);
  await callApi("/api/config", {
    method: "POST",
    body: JSON.stringify(parsed),
  });
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

async function runIntrospection() {
  ui.introspection.textContent = "Running introspection...";
  const payload = await callApi("/api/introspect", {
    method: "POST",
    body: JSON.stringify({}),
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

setInterval(async () => {
  try {
    await refreshStatus();
    await refreshLatestRun();
  } catch (err) {
    flash(`Auto-refresh failed: ${err.message}`);
  }
}, 3500);

boot();
