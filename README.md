# Autodesk CFD Automation Web Console

This project is a local web app and automation framework for Autodesk CFD that provides:

- Config-driven case execution from CSV.
- Direct CFD scripting execution via `CFD.exe -script`.
- Study introspection (design/scenario/BC/material/part discovery).
- Automatic retries on failed cases.
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

The default config is pre-wired to your test study:

`C:/Users/User/Downloads/Kani yawa/Kani yawa.cfdst`

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

## Web API Summary

- `GET /api/config`: load config.
- `POST /api/config`: save config JSON.
- `GET /api/cases`: load cases CSV + parsed rows.
- `POST /api/cases`: save cases CSV.
- `POST /api/introspect`: inspect current study via Autodesk CFD API.
- `POST /api/run`: start run (`mode`: `all|failed|changed`).
- `GET /api/status`: live status/logs.
- `GET /api/latest-run`: latest run summary.
- `GET /runtime/<path>`: serve generated output files.

## Notes

- `runtime/` is ignored in git and contains all generated artifacts.
- By default `solve.enabled` is `false` so runs use existing results for fast testing.
- To force actual solves, set:
  - `solve.enabled: true`
  - optional `force_solve=true` in specific case rows.

## Example Workflow

1. Open the web console.
2. Click `Introspect Study` to inspect available BCs/properties.
3. Adjust `parameter_mappings`, metrics, criteria, and ranking in Config.
4. Edit `cases.csv`.
5. Run `Run All`.
6. Review outputs from `Latest Run Outputs` (CSV/charts/report/screenshots).

## GitHub Publish

If not yet pushed:

```powershell
git init
git add .
git commit -m "Initial Autodesk CFD automation web console"
gh repo create <new-repo-name> --public --source . --remote origin --push
```
