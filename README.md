# Autodesk CFD Automation Web Console

This project is a local web app and automation framework for Autodesk CFD that provides:

- Config-driven case execution from CSV.
- Direct CFD scripting execution via `CFD.exe -script`.
- Study introspection (design/scenario/BC/material/part discovery).
- Natural language to `cases.csv` generation (Ollama or Groq provider).
- LLM mesh intelligence (`mesh.default_params` + quality gate suggestions).
- Pre-solve mesh quality gating (skewness, aspect ratio, orthogonality, element count sanity).
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

When enabled, mutating endpoints (`POST /api/config`, `POST /api/cases`, `POST /api/introspect`, `POST /api/run`, `POST /api/llm/generate-cases`, `POST /api/llm/suggest-mesh`) require the key.

## Example Workflow

1. Open the web console.
2. Set study path and save config.
3. Click `Introspect Study` to inspect available BCs/properties.
4. Adjust `parameter_mappings`, metrics, criteria, and ranking in Config.
5. Edit `cases.csv`.
6. Run `Run All`.
7. Review outputs from `Latest Run Outputs` (CSV/charts/report/screenshots).


