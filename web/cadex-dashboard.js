(function () {
  "use strict";

  const ui = {
    navRunningBadge: document.getElementById("navRunningBadge"),
    activeStudyName: document.getElementById("activeStudyName"),
    activeStudyStatus: document.getElementById("activeStudyStatus"),
    monitorSubtitle: document.getElementById("monitorSubtitle"),
    logContainer: document.getElementById("logContainer"),
    historyRefreshBtn: document.getElementById("historyRefreshBtn"),
    historyQuickChips: document.getElementById("historyQuickChips"),
    historyRunMeta: document.getElementById("historyRunMeta"),
    historyRunsBody: document.getElementById("historyRunsBody"),
    historySummaryMeta: document.getElementById("historySummaryMeta"),
    historySummaryGrid: document.getElementById("historySummaryGrid"),
    historyCasesBody: document.getElementById("historyCasesBody"),
  };

  const state = {
    selectedHistoryRunId: "",
    historyRuns: [],
  };

  function getApiKey() {
    return (localStorage.getItem("cfd_api_key") || "").trim();
  }

  async function callApi(url) {
    const headers = {};
    const apiKey = getApiKey();
    if (apiKey) headers["X-API-Key"] = apiKey;
    const response = await fetch(url, { headers });
    let payload = {};
    try {
      payload = await response.json();
    } catch (_err) {
      payload = {};
    }
    if (!response.ok) {
      const err = payload.error || `Request failed: ${url}`;
      throw new Error(err);
    }
    return payload;
  }

  function basename(pathValue) {
    const text = String(pathValue || "").trim();
    if (!text) return "-";
    const parts = text.replace(/\\/g, "/").split("/");
    return parts[parts.length - 1] || text;
  }

  function formatDate(value) {
    const text = String(value || "").trim();
    if (!text) return "-";
    const dt = new Date(text);
    if (Number.isNaN(dt.getTime())) return text;
    return dt.toLocaleString();
  }

  function num(value) {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function classifyLog(line) {
    const text = String(line || "").toLowerCase();
    if (!text) return "dim";
    if (/(error|failed|python_exception|non_zero_exit)/.test(text)) return "error";
    if (/(warn|warning|retry|null_metric|bad_mesh|timeout)/.test(text)) return "warn";
    if (/(pass|success|succeeded|converged|finished)/.test(text)) return "success";
    if (/(info|started|study|batch|running)/.test(text)) return "info";
    return "dim";
  }

  function splitStampedLog(line) {
    const text = String(line || "");
    const match = text.match(/^\[(.*?)\]\s*(.*)$/);
    if (!match) return { time: "", message: text };
    const maybeDate = new Date(match[1]);
    if (Number.isNaN(maybeDate.getTime())) {
      return { time: "", message: text };
    }
    const hh = String(maybeDate.getHours()).padStart(2, "0");
    const mm = String(maybeDate.getMinutes()).padStart(2, "0");
    const ss = String(maybeDate.getSeconds()).padStart(2, "0");
    return { time: `${hh}:${mm}:${ss}`, message: match[2] || "" };
  }

  function renderLogs(lines) {
    const liveLog = document.getElementById("logContainer");
    if (!liveLog) return;
    const data = Array.isArray(lines) ? lines.slice(-180) : [];
    const pinnedToBottom =
      liveLog.scrollTop + liveLog.clientHeight >= liveLog.scrollHeight - 20;
    liveLog.innerHTML = "";
    data.forEach((raw) => {
      const parts = splitStampedLog(raw);
      const row = document.createElement("div");
      row.className = "log-line";

      const time = document.createElement("span");
      time.className = "log-time";
      time.textContent = parts.time || "--:--:--";
      row.appendChild(time);

      const msg = document.createElement("span");
      msg.className = `log-msg ${classifyLog(parts.message)}`;
      msg.textContent = parts.message || String(raw || "");
      row.appendChild(msg);
      liveLog.appendChild(row);
    });
    if (pinnedToBottom) {
      liveLog.scrollTop = liveLog.scrollHeight;
    }
  }

  function summarizeMode(status) {
    if (!status || !status.running) {
      const last = status && status.last_summary ? status.last_summary : {};
      if (last.run_id) return `Idle · last run ${last.run_id}`;
      return "Idle";
    }
    const mode = String(status.mode || "all");
    const done = Number(status.completed_case_count || 0);
    const total = Number(status.selected_case_count || 0);
    const current = status.current_case ? `${status.current_case}` : "-";
    const phase = status.current_phase ? `${status.current_phase}` : "startup";
    return `Running ${mode} · ${done}/${total} · ${current} (${phase})`;
  }

  function countRunningCases(status) {
    const rows = Array.isArray(status.case_table) ? status.case_table : [];
    const running = rows.filter((row) => {
      const st = String(row.status || "").toLowerCase();
      return st === "running" || st === "retrying";
    }).length;
    if (running > 0) return running;
    return status.running ? 1 : 0;
  }

  function setPageContent(page, markerId, html) {
    const container = document.querySelector(`#page-${page} .content`);
    if (!container) return;
    container.innerHTML = html;
  }

  function objectiveSpec(config, rows) {
    const ranking = Array.isArray(config && config.ranking) ? config.ranking : [];
    if (ranking.length && ranking[0] && ranking[0].alias) {
      return {
        alias: String(ranking[0].alias),
        goal: String(ranking[0].goal || "min").toLowerCase() === "max" ? "max" : "min",
      };
    }
    for (const row of rows || []) {
      const metrics = row && typeof row.metrics === "object" ? row.metrics : {};
      const keys = Object.keys(metrics);
      if (keys.length) return { alias: keys[0], goal: "min" };
    }
    return { alias: "", goal: "min" };
  }

  function pickBest(rows, spec) {
    const ok = (rows || []).filter((row) => row && row.success);
    if (!ok.length) return null;
    if (!spec.alias) return ok[0];
    const values = ok
      .map((row) => ({ row, value: num(row.metrics && row.metrics[spec.alias]) }))
      .filter((item) => item.value !== null);
    if (!values.length) return ok[0];
    values.sort((a, b) => (spec.goal === "max" ? b.value - a.value : a.value - b.value));
    return values[0].row;
  }

  function renderMonitorPanel(status, latest, cfg, loopStatus) {
    setPageContent(
      "monitor",
      "liveMonitorPanel",
      `<div id="liveMonitorPanel">
        <div class="grid-4">
          <div class="stat-card"><div class="stat-label">Cases</div><div id="lmCases" class="stat-value">0</div><div id="lmCasesSub" class="stat-sub">-</div></div>
          <div class="stat-card"><div class="stat-label">Pass Rate</div><div id="lmPass" class="stat-value">0%</div><div id="lmPassSub" class="stat-sub">-</div></div>
          <div class="stat-card"><div class="stat-label">Best Objective</div><div id="lmBest" class="stat-value">-</div><div id="lmBestSub" class="stat-sub">-</div></div>
          <div class="stat-card"><div class="stat-label">Loop</div><div id="lmLoop" class="stat-value">idle</div><div id="lmLoopSub" class="stat-sub">-</div></div>
        </div>
        <div class="grid-2" style="margin-top:16px;">
          <div class="card"><div class="card-header"><div class="card-title">Active Case</div><div id="lmActiveSub" class="card-subtitle">-</div></div><div class="card-body"><div id="lmPhase" class="mono">phase: -</div></div></div>
          <div class="card"><div class="card-header"><div class="card-title">Live Log</div><button id="liveLogClearBtn" class="btn btn-ghost" style="font-size:12px;">Clear</button></div><div class="card-body" style="padding:0;"><div class="log-container" id="logContainer"></div></div></div>
        </div>
        <div class="card" style="margin-top:16px;"><div class="card-header"><div class="card-title">Case Table</div><div id="lmTableMeta" class="card-subtitle">-</div></div><div style="overflow:auto;max-height:320px;"><table class="case-table"><thead><tr><th>case_id</th><th>status</th><th>phase</th><th>attempt</th><th>failure</th></tr></thead><tbody id="lmBody"></tbody></table></div></div>
      </div>`
    );
    const rows = Array.isArray(latest && latest.case_results) ? latest.case_results : [];
    const success = Number(latest && latest.successful_cases) || rows.filter((row) => row.success).length;
    const failed = Number(latest && latest.failed_cases) || rows.filter((row) => !row.success).length;
    const total = Number(latest && latest.selected_case_count) || rows.length || Number(status.selected_case_count || 0);
    const pass = total > 0 ? Math.round((success / total) * 100) : 0;
    const spec = objectiveSpec(cfg, rows);
    const best = pickBest(rows, spec);
    const runningRows =
      Array.isArray(status.case_table) && status.case_table.length
        ? status.case_table.filter((row) =>
            ["running", "retrying"].includes(String(row.status || "").toLowerCase())
          ).length
        : 0;

    document.getElementById("lmCases").textContent = String(success + failed);
    document.getElementById("lmCasesSub").textContent = `of ${total || 0} selected`;
    document.getElementById("lmPass").textContent = `${pass}%`;
    document.getElementById("lmPassSub").textContent = `${success} pass / ${failed} fail`;
    document.getElementById("lmBest").textContent =
      best && spec.alias ? String(best.metrics && best.metrics[spec.alias]) : "-";
    document.getElementById("lmBestSub").textContent = `${spec.alias || "objective"} · ${
      best ? best.case_id : "-"
    }`;
    document.getElementById("lmLoop").textContent = loopStatus && loopStatus.running ? "running" : "idle";
    document.getElementById("lmLoopSub").textContent = loopStatus && loopStatus.running ? `batch ${loopStatus.current_batch || 0}` : "no active loop";

    document.getElementById("lmActiveSub").textContent = `${status.current_case || "-"} (${status.mode || "-"})`;
    document.getElementById("lmPhase").textContent = `phase: ${status.current_phase || "startup"}`;
    const tableRows =
      Array.isArray(status.case_table) && status.case_table.length
        ? status.case_table
        : rows.map((row) => ({
            case_id: row.case_id,
            status: row.success ? "success" : "failed",
            phase: row.success ? "complete" : "startup",
            attempt: row.attempts || row.attempt || 1,
            failure_type: row.failure_type || "",
          }));
    document.getElementById("lmTableMeta").textContent = `${tableRows.length} row(s) · ${
      runningRows || (status.running ? 1 : 0)
    } running`;
    const body = document.getElementById("lmBody");
    body.innerHTML = "";
    if (!tableRows.length) {
      body.innerHTML = `<tr><td colspan="5" style="color:var(--text-muted)">No case rows yet.</td></tr>`;
    } else {
      tableRows.slice(0, 120).forEach((row) => {
        const st = String(row.status || "queued").toLowerCase();
        const badge =
          st === "success"
            ? "badge-pass"
            : st === "failed"
            ? "badge-fail"
            : st === "running" || st === "retrying"
            ? "badge-running"
            : "badge-pending";
        body.insertAdjacentHTML(
          "beforeend",
          `<tr><td class="mono">${String(row.case_id || "-")}</td><td><span class="badge ${badge}">${st}</span></td><td class="mono">${String(
            row.phase || "-"
          )}</td><td class="mono">${String(row.attempt || 1)}</td><td>${String(
            row.failure_type || ""
          )}</td></tr>`
        );
      });
    }
  }

  function renderSimplePage(page, markerId, subtitle, title, dataHtml) {
    setPageContent(
      page,
      markerId,
      `<div id="${markerId}"><div class="card"><div class="card-header"><div class="card-title">${title}</div></div><div class="card-body">${dataHtml}</div></div></div>`
    );
    const sub = document.querySelector(`#page-${page} .topbar-subtitle`);
    if (sub) sub.textContent = subtitle;
  }

  function renderLoopPanel(loopStatus, loopLatest, cfg) {
    const summary =
      loopStatus && loopStatus.last_summary && Object.keys(loopStatus.last_summary).length
        ? loopStatus.last_summary
        : loopLatest || {};
    const objectiveAlias = summary.objective_alias || (cfg.ranking && cfg.ranking[0] && cfg.ranking[0].alias) || "objective";
    const objectiveGoal = summary.objective_goal || "min";
    const history = Array.isArray(summary.history) ? summary.history : [];
    const rows = history
      .slice(-10)
      .map((batch) => {
        const cases = Array.isArray(batch.cases) ? batch.cases : [];
        const feasible = cases.filter((item) => item && item.constraints_pass).length;
        const best = batch.best_case_in_batch || {};
        return `<div class="preflight-item"><span class="mono">B${batch.batch_index || "-"}</span><span>${feasible}/${cases.length} feasible · best=${String(
          best.case_id || "-"
        )} · obj=${String(best.objective_value ?? "-")}</span></div>`;
      })
      .join("");
    const preflight = summary.metric_contract_preflight || loopStatus.preflight || {};
    const preflightHtml = !Object.keys(preflight).length
      ? `<div class="card-subtitle">Preflight not run yet.</div>`
      : preflight.skipped
      ? `<div class="alert alert-info"><span>Preflight skipped (${String(preflight.reason || "unknown")})</span></div>`
      : preflight.ok
      ? `<div class="alert alert-success"><span>Preflight passed · checked=${String(
          preflight.checked_metrics || 0
        )} · available=${String(preflight.available_metric_pairs || 0)}</span></div>`
      : `<div class="alert alert-warn"><span>Preflight failed.</span></div>`;
    const best = summary.best_case || {};
    renderSimplePage(
      "loop",
      "liveLoopPanel",
      `${loopStatus.running ? "running" : "idle"} · ${objectiveGoal}(${objectiveAlias})`,
      "Design Loop (Live)",
      `<div class="summary-grid"><div class="summary-item"><div class="summary-key">loop_id</div><div class="summary-val">${String(
        summary.loop_id || "-"
      )}</div></div><div class="summary-item"><div class="summary-key">batches</div><div class="summary-val">${String(
        loopStatus.completed_batches || summary.completed_batches || 0
      )}/${String(loopStatus.max_batches || summary.max_batches || "-")}</div></div><div class="summary-item"><div class="summary-key">best_case</div><div class="summary-val">${String(
        best.case_id || "-"
      )}</div></div><div class="summary-item"><div class="summary-key">best_objective</div><div class="summary-val">${String(
        best.objective_value ?? "-"
      )}</div></div></div><div style="margin-top:12px;">${preflightHtml}</div><div style="margin-top:12px;display:flex;flex-direction:column;gap:8px;">${
        rows || `<div class="card-subtitle">No batch history yet.</div>`
      }</div>`
    );
  }

  function renderCasesPanel(casesPayload, cfg) {
    const rows = Array.isArray(casesPayload && casesPayload.rows) ? casesPayload.rows : [];
    const columns = [];
    rows.forEach((row) =>
      Object.keys(row || {}).forEach((key) => {
        if (!columns.includes(key)) columns.push(key);
      })
    );
    const tableHead = columns.map((col) => `<th>${col}</th>`).join("");
    const tableRows = rows
      .slice(0, 240)
      .map(
        (row) =>
          `<tr>${columns
            .map((col) =>
              col === "turbulence_model" && String(row[col] ?? "").trim()
                ? `<td><span class="tag">${String(row[col])}</span></td>`
                : `<td class="mono">${String(row[col] ?? "")}</td>`
            )
            .join("")}</tr>`
      )
      .join("");
    renderSimplePage(
      "cases",
      "liveCasesPanel",
      `cases.csv · ${rows.length} rows · study=${basename(cfg && cfg.study && cfg.study.template_model)}`,
      "Case Matrix (Live)",
      rows.length
        ? `<div style="overflow:auto;max-height:560px;"><table class="case-table"><thead><tr>${tableHead}</tr></thead><tbody>${tableRows}</tbody></table></div>`
        : `<div class="card-subtitle">No rows found in cases.csv.</div>`
    );
  }

  function renderConfigPanel(cfg) {
    const items = [
      ["study_path", cfg.study && cfg.study.template_model],
      ["design_name", cfg.study && cfg.study.design_name],
      ["scenario_name", cfg.study && cfg.study.scenario_name],
      ["solve.enabled", cfg.solve && cfg.solve.enabled],
      ["cfd_executable", cfg.automation && cfg.automation.cfd_executable],
      ["timeout_minutes", cfg.automation && cfg.automation.timeout_minutes],
      ["max_retries", cfg.automation && cfg.automation.max_retries],
      ["metrics", Array.isArray(cfg.metrics) ? cfg.metrics.length : 0],
      ["criteria", Array.isArray(cfg.criteria) ? cfg.criteria.length : 0],
      ["parameter_mappings", Array.isArray(cfg.parameter_mappings) ? cfg.parameter_mappings.length : 0],
    ];
    renderSimplePage(
      "config",
      "liveConfigPanel",
      "study_config.yaml · live backend values",
      "Study Config (Live)",
      `<div class="summary-grid">${items
        .map(
          ([key, value]) =>
            `<div class="summary-item"><div class="summary-key">${key}</div><div class="summary-val">${String(
              value ?? "-"
            )}</div></div>`
        )
        .join("")}</div>`
    );
  }

  function renderResultsPanel(latest, cfg, loopLatest) {
    const rows = Array.isArray(latest && latest.case_results) ? latest.case_results : [];
    const spec = objectiveSpec(cfg, rows);
    const best = pickBest(rows, spec);
    const success = rows.filter((row) => row.success).length;
    const failed = rows.length - success;
    const ranked = rows.slice().sort((a, b) => {
      if (Boolean(a.success) !== Boolean(b.success)) return a.success ? -1 : 1;
      const av = spec.alias ? num(a.metrics && a.metrics[spec.alias]) : null;
      const bv = spec.alias ? num(b.metrics && b.metrics[spec.alias]) : null;
      if (av === null && bv !== null) return 1;
      if (av !== null && bv === null) return -1;
      if (av !== null && bv !== null) return spec.goal === "max" ? bv - av : av - bv;
      return String(a.case_id || "").localeCompare(String(b.case_id || ""));
    });
    const rankRows = ranked
      .slice(0, 220)
      .map(
        (row, idx) =>
          `<tr><td class="mono">${idx + 1}</td><td class="mono">${String(row.case_id || "-")}</td><td class="mono">${String(
            spec.alias ? row.metrics && row.metrics[spec.alias] : "-"
          )}</td><td><span class="badge ${row.success ? "badge-pass" : "badge-fail"}">${
            row.success ? "pass" : "fail"
          }</span></td><td>${String(row.success ? "" : row.failure_type || row.failure_reason || "")}</td></tr>`
      )
      .join("");
    const failCounts = {};
    rows.forEach((row) => {
      if (!row.success) {
        const key = String(row.failure_type || "unknown");
        failCounts[key] = (failCounts[key] || 0) + 1;
      }
    });
    const failHtml = Object.keys(failCounts).length
      ? Object.keys(failCounts)
          .sort((a, b) => failCounts[b] - failCounts[a])
          .map(
            (key) =>
              `<div class="preflight-item"><span class="mono">${key}</span><span>${failCounts[key]}</span></div>`
          )
          .join("")
      : `<div class="card-subtitle">No failures in latest run.</div>`;
    const loopHistory = Array.isArray(loopLatest && loopLatest.history) ? loopLatest.history : [];
    const narration =
      loopHistory.length &&
      loopHistory[loopHistory.length - 1] &&
      loopHistory[loopHistory.length - 1].narration &&
      loopHistory[loopHistory.length - 1].narration.text
        ? String(loopHistory[loopHistory.length - 1].narration.text)
        : "No explanation captured yet.";
    renderSimplePage(
      "results",
      "liveResultsPanel",
      `${rows.length} cases · latest run ${latest && latest.run_id ? latest.run_id : "-"}`,
      "Results (Live)",
      `<div class="summary-grid"><div class="summary-item"><div class="summary-key">best_objective</div><div class="summary-val">${String(
        best && spec.alias ? best.metrics && best.metrics[spec.alias] : "-"
      )}</div></div><div class="summary-item"><div class="summary-key">best_case</div><div class="summary-val">${String(
        best ? best.case_id : "-"
      )}</div></div><div class="summary-item"><div class="summary-key">success</div><div class="summary-val">${success}</div></div><div class="summary-item"><div class="summary-key">failed</div><div class="summary-val">${failed}</div></div></div><div class="divider" style="margin:12px 0;"></div><div class="summary-key">llm_explanation</div><div class="summary-val">${narration}</div><div class="divider" style="margin:12px 0;"></div><div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;"><div><div class="summary-key">ranked_cases</div><div style="overflow:auto;max-height:420px;"><table class="rank-table"><thead><tr><th>Rank</th><th>Case</th><th>${String(
        spec.alias || "objective"
      )}</th><th>Status</th><th>Failure</th></tr></thead><tbody>${rankRows || `<tr><td colspan="5" style="color:var(--text-muted)">No rows yet.</td></tr>`}</tbody></table></div></div><div><div class="summary-key">failure_summary</div>${failHtml}</div></div>`
    );
  }

  function renderSurrogatePanel(statusPayload, coveragePayload) {
    const result = statusPayload && statusPayload.result ? statusPayload.result : {};
    const coverage = coveragePayload && coveragePayload.result ? coveragePayload.result : {};
    const perFeature = coverage && coverage.per_feature ? coverage.per_feature : {};
    const bars = Object.entries(perFeature).length
      ? Object.entries(perFeature)
          .sort((a, b) => Number(b[1]) - Number(a[1]))
          .map(([name, value]) => {
            const pct = Math.max(0, Math.min(100, Number(value) * 100));
            return `<div style="margin-bottom:8px;"><div style="display:flex;justify-content:space-between;margin-bottom:4px;"><span>${name}</span><span class="mono">${pct.toFixed(
              1
            )}%</span></div><div class="progress-bar-wrap"><div class="progress-bar-fill" style="width:${pct}%"></div></div></div>`;
          })
          .join("")
      : `<div class="card-subtitle">No coverage data yet.</div>`;
    renderSimplePage(
      "surrogate",
      "liveSurrogatePanel",
      "live surrogate status",
      "Surrogate (Live)",
      `<div class="summary-grid"><div class="summary-item"><div class="summary-key">model</div><div class="summary-val">${String(
        result.model_name || "-"
      )}</div></div><div class="summary-item"><div class="summary-key">target_alias</div><div class="summary-val">${String(
        result.target_alias || "-"
      )}</div></div><div class="summary-item"><div class="summary-key">row_count</div><div class="summary-val">${String(
        result.row_count || 0
      )}</div></div><div class="summary-item"><div class="summary-key">best_r2</div><div class="summary-val">${
        result.best_r2 !== undefined && result.best_r2 !== null ? Number(result.best_r2).toFixed(4) : "-"
      }</div></div></div><div style="margin-top:12px;">${
        result.ready
          ? `<div class="alert alert-success"><span>Surrogate ready for predict/validate.</span></div>`
          : `<div class="alert alert-warn"><span>Surrogate not ready yet.</span></div>`
      }</div><div class="divider" style="margin:12px 0;"></div>${bars}`
    );
  }

  function showLiveError(err) {
    const message = err && err.message ? String(err.message) : "API unavailable";
    if (ui.monitorSubtitle) ui.monitorSubtitle.textContent = `API unavailable: ${message}`;
    if (ui.navRunningBadge) ui.navRunningBadge.textContent = "0";
    if (ui.activeStudyStatus) ui.activeStudyStatus.textContent = "API unavailable";
    renderMonitorPanel(
      { running: false, mode: "", selected_case_count: 0, case_table: [], logs: [] },
      { case_results: [], successful_cases: 0, failed_cases: 0, selected_case_count: 0 },
      { ranking: [], study: {} },
      { running: false, current_batch: 0 }
    );
    const errorHtml = `<div class="alert alert-warn"><span>Backend API is unavailable. Start the CADEX server (` + "`python app.py`" + `) and refresh.</span></div>`;
    renderSimplePage("loop", "liveLoopPanel", "api unavailable", "Design Loop (Live)", errorHtml);
    renderSimplePage("cases", "liveCasesPanel", "api unavailable", "Case Matrix (Live)", errorHtml);
    renderSimplePage("config", "liveConfigPanel", "api unavailable", "Study Config (Live)", errorHtml);
    renderSimplePage("results", "liveResultsPanel", "api unavailable", "Results (Live)", errorHtml);
    renderSimplePage("surrogate", "liveSurrogatePanel", "api unavailable", "Surrogate (Live)", errorHtml);
    renderLogs([]);
  }

  async function refreshLive() {
    const [
      status,
      cfg,
      casesPayload,
      latestRun,
      loopStatus,
      loopLatest,
      surrogateStatus,
      surrogateCoverage,
    ] = await Promise.all([
      callApi("/api/status"),
      callApi("/api/config"),
      callApi("/api/cases"),
      callApi("/api/latest-run"),
      callApi("/api/design-loop/status"),
      callApi("/api/design-loop/latest"),
      callApi("/api/surrogate/status"),
      callApi("/api/surrogate/coverage"),
    ]);

    if (ui.monitorSubtitle) ui.monitorSubtitle.textContent = summarizeMode(status);
    if (ui.navRunningBadge) ui.navRunningBadge.textContent = String(countRunningCases(status));
    if (ui.activeStudyName) {
      const studyPath =
        cfg &&
        cfg.study &&
        typeof cfg.study === "object" &&
        typeof cfg.study.template_model === "string"
          ? cfg.study.template_model
          : "";
      ui.activeStudyName.textContent = basename(studyPath);
    }
    if (ui.activeStudyStatus) {
      ui.activeStudyStatus.textContent = loopStatus.running
        ? "Design loop running"
        : status.running
        ? "Run active"
        : "Idle";
    }
    renderMonitorPanel(status, latestRun, cfg, loopStatus);
    renderLoopPanel(loopStatus, loopLatest, cfg);
    renderCasesPanel(casesPayload, cfg);
    renderConfigPanel(cfg);
    renderResultsPanel(latestRun, cfg, loopLatest);
    renderSurrogatePanel(surrogateStatus, surrogateCoverage);
    renderLogs(status.logs || []);
  }

  function renderHistorySummary(summary) {
    if (!ui.historySummaryMeta || !ui.historySummaryGrid) return;
    if (!summary || typeof summary !== "object" || !summary.run_id) {
      ui.historySummaryMeta.textContent = "No run selected";
      ui.historySummaryGrid.innerHTML = "";
      return;
    }
    ui.historySummaryMeta.textContent = `${summary.run_id} · ${formatDate(summary.created_at)}`;
    const items = [
      ["run_id", summary.run_id],
      ["date", formatDate(summary.created_at)],
      ["mode", summary.mode || "-"],
      ["study", summary.study_path || "-"],
      ["design", summary.design_name || "-"],
      ["scenario", summary.scenario_name || "-"],
      ["selected", summary.selected_case_count ?? "-"],
      ["success", summary.successful_cases ?? "-"],
      ["failed", summary.failed_cases ?? "-"],
    ];
    ui.historySummaryGrid.innerHTML = "";
    items.forEach(([key, value]) => {
      const box = document.createElement("div");
      box.className = "summary-item";
      box.innerHTML = `<div class="summary-key">${key}</div><div class="summary-val">${String(value ?? "-")}</div>`;
      ui.historySummaryGrid.appendChild(box);
    });
  }

  function renderHistoryCases(summary) {
    if (!ui.historyCasesBody) return;
    ui.historyCasesBody.innerHTML = "";
    const rows = Array.isArray(summary && summary.case_results) ? summary.case_results : [];
    if (!rows.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="4" style="color:var(--text-muted)">No case rows in this run.</td>`;
      ui.historyCasesBody.appendChild(tr);
      return;
    }
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      const ok = Boolean(row.success);
      const statusBadge = ok
        ? `<span class="badge badge-pass">Pass</span>`
        : `<span class="badge badge-fail">Fail</span>`;
      const failure = ok
        ? "-"
        : `${String(row.failure_type || "unknown")}${row.failure_reason ? ` · ${row.failure_reason}` : ""}`;

      const metrics = row.metrics && typeof row.metrics === "object" ? row.metrics : {};
      const preview = Object.keys(metrics)
        .slice(0, 3)
        .map((key) => `${key}=${metrics[key]}`)
        .join(", ");
      tr.innerHTML = `
        <td class="mono">${String(row.case_id || "-")}</td>
        <td>${statusBadge}</td>
        <td>${failure || "-"}</td>
        <td class="mono" style="font-size:11px;color:var(--text-secondary)">${preview || "-"}</td>
      `;
      ui.historyCasesBody.appendChild(tr);
    });
  }

  async function selectHistoryRun(runId) {
    const id = String(runId || "").trim();
    if (!id) return;
    state.selectedHistoryRunId = id;
    const summaryPayload = await callApi(`/api/history/runs/${encodeURIComponent(id)}`);
    const summary = summaryPayload && summaryPayload.summary ? summaryPayload.summary : {};
    renderHistorySummary(summary);
    renderHistoryCases(summary);
    renderHistoryRunsTable();
  }

  function renderQuickChips() {
    if (!ui.historyQuickChips) return;
    ui.historyQuickChips.innerHTML = "";
    const runs = state.historyRuns;
    if (!runs.length) return;

    const makeChip = (label, runId) => {
      const chip = document.createElement("button");
      chip.className = "quick-chip";
      chip.textContent = label;
      chip.addEventListener("click", () => {
        selectHistoryRun(runId).catch(() => {});
      });
      return chip;
    };

    const latest = runs[0];
    ui.historyQuickChips.appendChild(
      makeChip(`Latest · ${formatDate(latest.created_at)}`, latest.run_id)
    );

    const kani = runs.find((run) => String(run.study_path || "").toLowerCase().includes("kani yawa"));
    if (kani) {
      ui.historyQuickChips.appendChild(
        makeChip(`Kani Yawa · ${formatDate(kani.created_at)}`, kani.run_id)
      );
    }
  }

  function renderHistoryRunsTable() {
    if (!ui.historyRunsBody) return;
    ui.historyRunsBody.innerHTML = "";
    const runs = state.historyRuns;
    runs.forEach((run) => {
      const tr = document.createElement("tr");
      tr.className = "clickable";
      if (state.selectedHistoryRunId === run.run_id) tr.classList.add("selected");
      tr.addEventListener("click", () => {
        selectHistoryRun(run.run_id).catch(() => {});
      });
      const studyText = basename(run.study_path || "-");
      tr.innerHTML = `
        <td class="mono">${formatDate(run.created_at)}</td>
        <td class="mono">${String(run.run_id || "-")}</td>
        <td title="${String(run.study_path || "")}">${studyText}</td>
        <td class="mono">${String(run.successful_cases || 0)}/${String(run.failed_cases || 0)}</td>
      `;
      ui.historyRunsBody.appendChild(tr);
    });
  }

  async function refreshHistory() {
    if (!ui.historyRunMeta) return;
    const payload = await callApi("/api/history/runs?limit=80");
    const runs = Array.isArray(payload && payload.runs) ? payload.runs : [];
    state.historyRuns = runs;
    ui.historyRunMeta.textContent = `Stored runs: ${payload.total || 0}`;
    renderQuickChips();
    renderHistoryRunsTable();

    if (!runs.length) {
      renderHistorySummary({});
      renderHistoryCases({});
      return;
    }

    const runExists = runs.some((item) => item.run_id === state.selectedHistoryRunId);
    if (!runExists) {
      state.selectedHistoryRunId = runs[0].run_id;
    }
    await selectHistoryRun(state.selectedHistoryRunId);
  }

  function bindActions() {
    if (ui.historyRefreshBtn) {
      ui.historyRefreshBtn.addEventListener("click", () => {
        refreshHistory().catch(() => {});
      });
    }
    document.addEventListener("click", (event) => {
      const clearBtn =
        event && event.target && event.target.closest
          ? event.target.closest("#liveLogClearBtn")
          : null;
      if (!clearBtn) return;
      const log = document.getElementById("logContainer");
      if (log) log.innerHTML = "";
    });
  }

  function switchPage(name, el) {
    document.querySelectorAll(".page").forEach((page) => page.classList.remove("active"));
    const target = document.getElementById(`page-${name}`);
    if (target) target.classList.add("active");
    document.querySelectorAll(".nav-item").forEach((node) => node.classList.remove("active"));
    if (el) el.classList.add("active");
  }

  function switchConfigTab(el) {
    document.querySelectorAll(".config-nav-item").forEach((node) => node.classList.remove("active"));
    if (el) el.classList.add("active");
  }

  async function boot() {
    bindActions();
    const init = await Promise.allSettled([refreshLive(), refreshHistory()]);
    if (init[0] && init[0].status === "rejected") {
      showLiveError(init[0].reason);
    }
    if (init[1] && init[1].status === "rejected" && ui.historyRunMeta) {
      ui.historyRunMeta.textContent = "History unavailable";
    }
    setInterval(() => {
      refreshLive().catch((err) => {
        showLiveError(err);
      });
    }, 2000);
    setInterval(() => {
      refreshHistory().catch(() => {
        if (ui.historyRunMeta) ui.historyRunMeta.textContent = "History unavailable";
      });
    }, 20000);
  }

  window.switchPage = switchPage;
  window.switchConfigTab = switchConfigTab;

  boot().catch((err) => {
    if (ui.monitorSubtitle) ui.monitorSubtitle.textContent = `Dashboard init failed: ${err.message}`;
  });
})();
