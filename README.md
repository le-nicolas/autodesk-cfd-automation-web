# CFD Automated Design EXploration

CADEX is an open-source, locally-run design exploration platform for Autodesk CFD. It replaces manual parametric workflows with a closed-loop AI-assisted engine — from natural language case generation to Bayesian optimization — while keeping engineers in control of geometry and physics judgment. 

## What It Does
CADEX takes a CFD study and systematically explores its design space without requiring manual intervention between runs. Engineers define a goal — minimize temperature, maximize flow uniformity, minimize pressure drop — and CADEX proposes cases, runs them, reads the results, and proposes smarter cases automatically until it converges on an optimal design.

## How It Works
The engine has four layers working together:
Language — describe your study in plain English. CADEX generates the case matrix automatically via a local Ollama model or Groq cloud API.
Intelligence — before any solve runs, CADEX evaluates mesh quality and fails fast on bad geometry rather than wasting hours on a doomed simulation.
Optimization — a Bayesian optimizer proposes each new batch of cases based on what previous results revealed, focusing compute where it matters most.
Explanation — an LLM layer wraps the optimizer, translating mathematical convergence into plain engineering reasoning at every step.

## Why It Exists
Commercial tools like Ansys OptiSLang and SimScale charge enterprise prices for closed-loop design exploration. CADEX brings the same capability to any engineer with a Windows machine, Autodesk CFD, and an internet connection — for free.

| Built for Autodesk CFD 2026. Runs locally. No cloud required.


## How to Use CADEX

1. Install
bashgit clone https://github.com/le-nicolas/autodesk-cfd-automation-web
cd autodesk-cfd-automation-web
pip install -r requirements.txt
Choose your LLM provider:
Ollama (free, local, no API key):
bashwinget install Ollama.Ollama
ollama pull llama3.2:3b
Groq (free cloud):
bash$env:GROQ_API_KEY="your-key"

2. Launch
bashpython app.py
```
Open `http://127.0.0.1:5055`

---

### 3. Connect Your Study

- Click **Discover Studies** → select your `.cfdst` file
- Or type the path manually
- Click **Apply Path to Config** → **Save Config**

---

### 4. Introspect

Click **Introspect Study** — CADEX discovers:
- Available boundary conditions
- Materials
- Design scenarios
- Parts

This confirms your study is wired correctly before anything runs.

---

### 5. Get Mesh Recommendations

In the **Mesh Intelligence** panel:
- Click **Suggest Mesh Params**
- CADEX reads your physics config and returns:
```
y+ target:        1
Inflation layers: 5
Max element size: 0.01m
Min element size: 0.001m
```
- Click **Suggest + Apply To Config** to save
- Apply these values manually in Autodesk CFD mesher

---

### 6. Choose Your Run Mode

You have three ways to run CADEX depending on what you need:

---

#### Mode A — Manual Cases (v1.0)
Best for: when you already know exactly what to test.

- Open `config/cases.csv`
- Define your parameter rows manually
- Click **Run All**

---

#### Mode B — Natural Language Cases (v2.0)
Best for: fast parametric setup without editing CSV.

In the **LLM Case Builder** panel, type:
```
test inlet velocities from 1 to 5 m/s in 1 m/s steps 
with k-epsilon and k-omega turbulence models, 
ambient temp 25°C

Click Generate Cases → preview the rows
Click Apply to save to cases.csv
Click Run All


Mode C — Generative Design Loop (v4.0)
Best for: when you have a goal and want CADEX to find the answer autonomously.
In the Generative Design Loop panel, define:
json{
  "objective_alias": "temp_max_c",
  "objective_goal": "min",
  "search_space": [
    {"name": "fin_height_mm", "type": "real", "min": 5, "max": 20},
    {"name": "fin_spacing_mm", "type": "real", "min": 2, "max": 10},
    {"name": "flow_rate_lpm",  "type": "real", "min": 1, "max": 5}
  ],
  "constraints": [
    {"alias": "pressure_drop_pa", "operator": "<=", "threshold": 50}
  ],
  "batch_size": 10,
  "max_batches": 5,
  "use_llm_explanations": true
}
```
- Click **Start Loop**
- Watch live in the dashboard as CADEX:
```
Batch 1 → runs 10 cases → reads results
        → optimizer focuses on promising region
Batch 2 → runs 10 smarter cases → reads results
        → optimizer narrows further
...
Batch 5 → converged → optimal design found
```
- Click **Stop Loop** anytime for graceful exit

---

### 7. Monitor Live

During any run the dashboard shows:
- Live log stream per case
- Current batch progress
- Pass/fail status per case
- Failure type if something goes wrong:
```
timeout | non_zero_exit | python_exception | no_results | bad_mesh
```

---

### 8. Review Outputs

From **Latest Run Outputs**:

| Output | What it tells you |
|---|---|
| Per-case summary CSV | Every case result in detail |
| Master ranked CSV | All cases ranked by your criteria |
| Charts | Visual parameter vs metric relationships |
| Screenshots | CFD result images per case |
| HTML/markdown report | Shareable summary of the full study |

---

### 9. Iterate

**Run only what changed:**
```
Edit cases.csv → click Run Changed
```
Only cases with modified inputs re-run. Everything else is skipped.

**Run only failures:**
```
Click Run Failed
Retries failed cases with failure-mode aware logic:

Bad mesh → retries with adjusted mesh params
Solver divergence → retries with coarser mesh
Script failure → retries as-is

- Config-driven case execution from CSV.
- Direct CFD scripting execution via `CFD.exe -script`.
- Study introspection (design/scenario/BC/material/part discovery).
- Natural language to `cases.csv` generation (Ollama or Groq provider).
- LLM mesh intelligence (`mesh.default_params` + quality gate suggestions).
- Pre-solve mesh quality gating (skewness, aspect ratio, orthogonality, element count sanity).
- Generative Design Loop (closed-loop Bayesian optimization + optional LLM reasoning).
- Automatic retries on failed cases.
- Live log streaming into dashboard during active case execution.
- Run modes:
  - `all`
  - `failed` (rerun failed only)
  - `changed` (rerun cases whose inputs/config changed)
- Output generation:
  - per-case summary CSV
  - per-case metrics CSV
  - screenshots
  - aggregated master/ranked CSVs
  - charts
  - markdown/html report
- Pass/fail criteria and weighted ranking.

## Project Structure

- `app.py`: Flask server + API + background run manager.
- `cfd_automation/`: orchestration, config I/O, post-processing.
- `scripts/cfd_case_runner.py`: executed by Autodesk CFD for each case.
- `scripts/cfd_introspect.py`: executed by Autodesk CFD for introspection.
- `web/`: HTML/CSS/JS dashboard.
- `config/study_config.yaml`: run configuration.
- `config/cases.csv`: case matrix.

## Requirements

- Windows with Autodesk CFD installed (tested with CFD 2026).
- Python 3.10+.
- Packages in `requirements.txt`.

Install dependencies:

```powershell
pip install -r requirements.txt
```

## Run Locally

From the repo root:

```powershell
python app.py
```

Open:

`http://127.0.0.1:5055`

## First-Time Setup: Study Path

No user-specific study path is hardcoded now.

Set your `.cfdst` path in either way:

1. Web console:
   - Use **Discover Studies** and select one.
   - Or type path manually in **Study Path**.
   - Click **Apply Path To Config** then **Save Config**.
2. Manual edit:
   - Update `study.template_model` in `config/study_config.yaml`.

## Web API Summary

- `GET /api/config`: load config.
- `POST /api/config`: save config JSON.
- `GET /api/cases`: load cases CSV + parsed rows.
- `POST /api/cases`: save cases CSV.
- `POST /api/introspect`: inspect current study via Autodesk CFD API.
- `POST /api/llm/generate-cases`: generate case matrix from natural language (`apply=true` to persist).
- `POST /api/llm/suggest-mesh`: suggest mesh defaults + mesh gate thresholds (`apply=true` to persist).
- `POST /api/run`: start run (`mode`: `all|failed|changed`).
- `POST /api/design-loop/start`: start closed-loop optimization batches.
- `POST /api/design-loop/stop`: request graceful stop.
- `GET /api/design-loop/status`: live loop state/logs.
- `GET /api/design-loop/latest`: latest completed loop summary.
- `GET /api/status`: live status/logs.
- `GET /api/latest-run`: latest run summary.
- `GET /api/studies`: discover `.cfdst` files on this machine.
- `GET /runtime/<path>`: serve generated output files.

## Notes

- `runtime/` is ignored in git and contains all generated artifacts.
- By default `solve.enabled` is `false` so runs use existing results for fast testing.
- The web console shows a prominent warning banner whenever `solve.enabled` is `false`.
- To force actual solves, set:
  - `solve.enabled: true`
  - optional `force_solve=true` in specific case rows.

## Mesh Intelligence Layer (v2.1)

### LLM-Suggested Mesh Parameters

Use **Mesh Intelligence (v2.1)** panel in web console to generate:

- `mesh.default_params.target_y_plus`
- `mesh.default_params.inflation_layers`
- `mesh.default_params.max_element_size_m`
- `mesh.default_params.min_element_size_m`
- `mesh.default_params.refinement_zones`
- optional `mesh.quality_gate` threshold updates

Click **Suggest Mesh Params** for preview or **Suggest + Apply To Config** to save into `study_config.yaml`.

### Post-Mesh Quality Gate

Before solve, `scripts/cfd_case_runner.py` evaluates mesh quality gate using available metrics:

- `skewness <= skewness_max`
- `aspect_ratio <= aspect_ratio_max`
- `orthogonality >= orthogonality_min`
- `element_count_min <= element_count <= element_count_max`

If a check fails, solve is skipped and case fails early with `failure_type=bad_mesh`.

Optional case-row overrides (for explicit control/testing):

- `mesh_skewness`
- `mesh_aspect_ratio`
- `mesh_orthogonality`
- `mesh_element_count`
- `mesh_max_element_size_m`
- `mesh_min_element_size_m`
- `mesh_inflation_layers`
- `mesh_target_y_plus`

### Smarter Retry Logic

Retries are now failure-mode aware:

- `mesh_failure` or `solver_divergence`: retry with mesh adjustment (`coarsen`/`refine` strategy from `mesh.retry`).
- `script_failure`: retry as-is (same mesh settings).
- Other failures: retry with standard behavior.

## Generative Design Loop (Closed Loop)

This is the automated loop:

1. Propose next `cases.csv` batch from Bayesian optimizer.
2. Run existing CFD automation engine unchanged.
3. Read metrics/results and score objective + constraints.
4. Feed scores back to optimizer and generate next batch.
5. Repeat until `max_batches` or stop requested.

### Payload Example

`POST /api/design-loop/start`

```json
{
  "objective_alias": "temp_max_c",
  "objective_goal": "min",
  "search_space": [
    {"name": "fin_height_mm", "type": "real", "min": 5, "max": 20},
    {"name": "fin_spacing_mm", "type": "real", "min": 2, "max": 10},
    {"name": "flow_rate_lpm", "type": "real", "min": 1, "max": 5}
  ],
  "constraints": [
    {"alias": "pressure_drop_pa", "operator": "<=", "threshold": 50}
  ],
  "batch_size": 10,
  "max_batches": 5,
  "fixed_values": {"ambient_temp_c": 25},
  "use_llm_explanations": true
}
```

### Notes

- The optimizer uses `scikit-optimize` (GP/EI) when available.
- If unavailable, it falls back to random sampling (functional but less sample-efficient).
- Loop artifacts are saved in `runtime/design_loops/<loop_id>/`.
- Loop can be started/stopped from the web dashboard section **Generative Design Loop**.

## LLM Case Builder

You can generate `cases.csv` using plain language in the **LLM Case Builder** panel.

Example prompt:

`test inlet velocities from 1 to 5 m/s in 1 m/s steps with two turbulence models k-epsilon and k-omega while keeping ambient_temp_c at 25`

### Provider Configuration

`config/study_config.yaml` now includes:

- `llm.provider`: `ollama` or `groq`
- `llm.temperature`
- `llm.max_rows`
- `llm.ollama.*` (base URL, model, timeout)
- `llm.groq.*` (base URL, model, API key env var, timeout)

### Ollama Setup (local, no API key)

1. Install Ollama (Windows):
   - `winget install Ollama.Ollama`
2. Pull a model:
   - `ollama pull llama3.2:3b`
3. Ensure service is running:
   - `ollama list`
4. Keep config at:
   - `llm.provider: ollama`
   - `llm.ollama.model: llama3.2:3b`

Recommended baseline for low-memory laptops (8 GB RAM / 4 GB VRAM class): `llama3.2:3b`.

### Groq Setup (cloud)

1. Set key:
   - `$env:GROQ_API_KEY=\"your-key\"`
2. Set config:
   - `llm.provider: groq`
   - `llm.groq.model: llama-3.1-8b-instant`

## Failure Semantics and Retry Behavior

A case is marked `failed` when any of these occurs:

- CFD script process timeout.
- CFD script process non-zero exit.
- Python exception inside `scripts/cfd_case_runner.py`.
- No usable results/summary available for post-processing.
- Mesh quality gate failure before solve (`bad_mesh`).

Failure diagnosis is stored in:

- `failure_type` (`timeout | non_zero_exit | python_exception | no_results | bad_mesh`)
- `failure_mode` (`mesh_failure | solver_divergence | script_failure | generic_failure`)
- `failure_reason`

These are shown in:

- Dashboard results table.
- Dashboard "Failure Reasons" section.
- Live status panel while run is in progress.
- Case result JSON in `runtime/runs/<run_id>/cases/<case_id>/attempt_<n>/case_result.json`.

Retries are controlled by `automation.max_retries` in config.

## CI (GitHub Actions)

This repo includes CI at:

- `.github/workflows/ci.yml`

CI runs:

- Python compile check.
- `pytest` dry-run pipeline tests (`CFD_AUTOMATION_DRY_RUN=1`).

Dry-run mode validates orchestration/post-processing behavior in `solve.enabled: false` flow without requiring Autodesk CFD on CI runners.

## API Security (Local Tool)

By default there is no API auth, suitable for single-user localhost usage.

Optional protection:

1. Set environment variable:
   - `CFD_AUTOMATION_API_KEY=your-secret`
2. Restart server.
3. Use the top-right API key field in the web console (sent as `X-API-Key`).

When enabled, mutating endpoints (`POST /api/config`, `POST /api/cases`, `POST /api/introspect`, `POST /api/run`, `POST /api/llm/generate-cases`, `POST /api/llm/suggest-mesh`, `POST /api/design-loop/start`, `POST /api/design-loop/stop`) require the key.

## Example Workflow

1. Open the web console.
2. Set study path and save config.
3. Click `Introspect Study` to inspect available BCs/properties.
4. Adjust `parameter_mappings`, metrics, criteria, and ranking in Config.
5. Edit `cases.csv`.
6. Run `Run All`.
7. Review outputs from `Latest Run Outputs` (CSV/charts/report/screenshots).


