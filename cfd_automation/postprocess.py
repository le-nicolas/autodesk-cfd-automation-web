from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

from .utils import ensure_dir, to_float

matplotlib.use("Agg")


@dataclass
class PostprocessResult:
    master_csv: Path | None
    ranked_csv: Path | None
    chart_files: list[Path]
    report_md: Path | None
    report_html: Path | None
    summary: dict[str, Any]


def _evaluate_operator(value: float | None, operator: str, threshold: float) -> bool:
    if value is None:
        return False
    if operator == "<":
        return value < threshold
    if operator == "<=":
        return value <= threshold
    if operator == ">":
        return value > threshold
    if operator == ">=":
        return value >= threshold
    if operator == "==":
        return value == threshold
    if operator == "!=":
        return value != threshold
    return False


def _add_pass_fail(df: pd.DataFrame, criteria: list[dict[str, Any]]) -> pd.DataFrame:
    if df.empty:
        return df
    if not criteria:
        df["pass"] = True
        return df

    pass_mask = pd.Series([True] * len(df), index=df.index)
    for criterion in criteria:
        alias = criterion.get("alias")
        operator = criterion.get("operator", "<=")
        threshold = to_float(criterion.get("threshold"))
        if alias not in df.columns or threshold is None:
            pass_mask &= False
            continue
        values = pd.to_numeric(df[alias], errors="coerce")
        current = values.apply(
            lambda x: _evaluate_operator(
                None if pd.isna(x) else float(x),
                operator,
                float(threshold),
            )
        )
        pass_mask &= current
    df["pass"] = pass_mask
    return df


def _add_ranking(df: pd.DataFrame, ranking: list[dict[str, Any]]) -> pd.DataFrame:
    if df.empty:
        return df
    if not ranking:
        df["score"] = 0.0
        df["rank"] = range(1, len(df) + 1)
        return df

    total_weight = 0.0
    score = pd.Series([0.0] * len(df), index=df.index)
    for rule in ranking:
        alias = rule.get("alias")
        goal = str(rule.get("goal", "min")).lower()
        weight = to_float(rule.get("weight")) or 1.0
        if alias not in df.columns:
            continue

        series = pd.to_numeric(df[alias], errors="coerce")
        valid = series.dropna()
        if valid.empty:
            continue
        min_val = valid.min()
        max_val = valid.max()
        if max_val == min_val:
            normalized = pd.Series([1.0] * len(df), index=df.index)
        else:
            if goal == "max":
                normalized = (series - min_val) / (max_val - min_val)
            else:
                normalized = (max_val - series) / (max_val - min_val)
            normalized = normalized.fillna(0.0)

        total_weight += weight
        score += normalized * weight

    if total_weight > 0:
        df["score"] = score / total_weight
    else:
        df["score"] = 0.0

    df = df.sort_values(by="score", ascending=False, kind="mergesort").reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)
    return df


def _write_report(
    *,
    report_dir: Path,
    df: pd.DataFrame,
    chart_files: list[Path],
    case_results: list[dict[str, Any]],
    run_summary: dict[str, Any],
) -> tuple[Path, Path]:
    ensure_dir(report_dir)
    report_md = report_dir / "report.md"
    report_html = report_dir / "report.html"

    def rel_from_report(target: Path) -> str:
        return Path(
            os.path.relpath(str(target.resolve()), start=str(report_dir.resolve()))
        ).as_posix()

    lines: list[str] = []
    lines.append(f"# CFD Automation Report")
    lines.append("")
    lines.append(f"- Run ID: `{run_summary.get('run_id', '')}`")
    lines.append(f"- Successful Cases: `{run_summary.get('success_count', 0)}`")
    lines.append(f"- Failed Cases: `{run_summary.get('failed_count', 0)}`")
    lines.append("")

    if not df.empty:
        lines.append("## Ranked Results")
        lines.append("")
        lines.append(_dataframe_to_markdown(df))
        lines.append("")
    else:
        lines.append("No successful case results were available.")
        lines.append("")

    if chart_files:
        lines.append("## Charts")
        lines.append("")
        for chart_file in chart_files:
            rel = rel_from_report(chart_file)
            lines.append(f"![{chart_file.stem}]({rel})")
            lines.append("")

    lines.append("## Screenshots")
    lines.append("")
    for case_result in case_results:
        shots = case_result.get("screenshots", [])
        if not shots:
            continue
        lines.append(f"### {case_result.get('case_id', 'unknown')}")
        for shot in shots:
            shot_path = Path(shot)
            if shot_path.exists():
                rel = rel_from_report(shot_path)
                lines.append(f"![{shot_path.name}]({rel})")
        lines.append("")

    report_md.write_text("\n".join(lines), encoding="utf-8")

    html_parts: list[str] = []
    html_parts.append("<!doctype html>")
    html_parts.append("<html><head><meta charset='utf-8'>")
    html_parts.append("<title>CFD Automation Report</title>")
    html_parts.append(
        "<style>body{font-family:Segoe UI,Arial,sans-serif;background:#f5f7fb;color:#1d2433;padding:24px;}"
        "h1,h2,h3{color:#0b2545;}table{border-collapse:collapse;width:100%;margin:12px 0;}"
        "th,td{border:1px solid #cbd5e1;padding:8px;text-align:left;font-size:13px;}"
        "img{max-width:980px;border:1px solid #d1d5db;margin:8px 0;}code{background:#e2e8f0;padding:2px 4px;}"
        "</style></head><body>"
    )
    html_parts.append("<h1>CFD Automation Report</h1>")
    html_parts.append(
        f"<p><b>Run ID:</b> <code>{run_summary.get('run_id','')}</code><br>"
        f"<b>Successful Cases:</b> {run_summary.get('success_count',0)}<br>"
        f"<b>Failed Cases:</b> {run_summary.get('failed_count',0)}</p>"
    )

    if not df.empty:
        html_parts.append("<h2>Ranked Results</h2>")
        html_parts.append(df.to_html(index=False, border=0))
    else:
        html_parts.append("<p>No successful case results were available.</p>")

    if chart_files:
        html_parts.append("<h2>Charts</h2>")
        for chart_file in chart_files:
            rel = rel_from_report(chart_file)
            html_parts.append(f"<p><img src='{rel}' alt='{chart_file.name}'></p>")

    html_parts.append("<h2>Screenshots</h2>")
    for case_result in case_results:
        shots = case_result.get("screenshots", [])
        if not shots:
            continue
        html_parts.append(f"<h3>{case_result.get('case_id', 'unknown')}</h3>")
        for shot in shots:
            shot_path = Path(shot)
            if shot_path.exists():
                rel = rel_from_report(shot_path)
                html_parts.append(f"<p><img src='{rel}' alt='{shot_path.name}'></p>")

    html_parts.append("</body></html>")
    report_html.write_text("\n".join(html_parts), encoding="utf-8")
    return report_md, report_html


def _dataframe_to_markdown(df: pd.DataFrame) -> str:
    columns = [str(col) for col in df.columns]
    lines = []
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
    for _, row in df.iterrows():
        values = []
        for col in columns:
            value = row[col]
            if pd.isna(value):
                values.append("")
            else:
                text = str(value).replace("|", "\\|")
                values.append(text)
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def run_postprocess(
    *,
    run_dir: Path,
    case_results: list[dict[str, Any]],
    config: dict[str, Any],
) -> PostprocessResult:
    results_dir = ensure_dir(run_dir / "results")
    charts_dir = ensure_dir(results_dir / "charts")
    report_dir = ensure_dir(results_dir / "report")

    rows: list[dict[str, Any]] = []
    for case_result in case_results:
        row: dict[str, Any] = {
            "case_id": case_result.get("case_id"),
            "status": "success" if case_result.get("success") else "failed",
            "attempts": case_result.get("attempts", 1),
        }
        row.update(case_result.get("metrics", {}))
        rows.append(row)

    df = pd.DataFrame(rows)
    master_csv = results_dir / "master_results.csv"
    if df.empty:
        master_csv.write_text("case_id,status,attempts\n", encoding="utf-8")
        ranked_csv = results_dir / "ranked_results.csv"
        ranked_csv.write_text("case_id,status,attempts,score,rank,pass\n", encoding="utf-8")
        report_md, report_html = _write_report(
            report_dir=report_dir,
            df=df,
            chart_files=[],
            case_results=case_results,
            run_summary={
                "run_id": run_dir.name,
                "success_count": 0,
                "failed_count": len(case_results),
            },
        )
        return PostprocessResult(
            master_csv=master_csv,
            ranked_csv=ranked_csv,
            chart_files=[],
            report_md=report_md,
            report_html=report_html,
            summary={
                "rows": 0,
                "success_count": 0,
                "failed_count": len(case_results),
            },
        )

    for metric in config.get("metrics", []):
        alias = metric.get("alias")
        if alias and alias in df.columns:
            df[alias] = pd.to_numeric(df[alias], errors="coerce")

    df.to_csv(master_csv, index=False)

    scored = _add_pass_fail(df.copy(), config.get("criteria", []))
    scored = _add_ranking(scored, config.get("ranking", []))
    ranked_csv = results_dir / "ranked_results.csv"
    scored.to_csv(ranked_csv, index=False)

    chart_files: list[Path] = []
    metric_aliases = [m.get("alias") for m in config.get("metrics", []) if m.get("alias")]
    for alias in metric_aliases:
        if alias not in scored.columns:
            continue
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(scored["case_id"].astype(str), scored[alias], color="#0f766e")
        ax.set_title(alias)
        ax.set_xlabel("Case")
        ax.set_ylabel(alias)
        ax.tick_params(axis="x", rotation=35)
        fig.tight_layout()
        chart_path = charts_dir / f"{alias}.png"
        fig.savefig(chart_path, dpi=140)
        plt.close(fig)
        chart_files.append(chart_path)

    success_count = int((scored["status"] == "success").sum())
    failed_count = int((scored["status"] == "failed").sum())
    report_md, report_html = _write_report(
        report_dir=report_dir,
        df=scored,
        chart_files=chart_files,
        case_results=case_results,
        run_summary={
            "run_id": run_dir.name,
            "success_count": success_count,
            "failed_count": failed_count,
        },
    )

    return PostprocessResult(
        master_csv=master_csv,
        ranked_csv=ranked_csv,
        chart_files=chart_files,
        report_md=report_md,
        report_html=report_html,
        summary={
            "rows": len(scored),
            "success_count": success_count,
            "failed_count": failed_count,
        },
    )
    def rel_from_report(target: Path) -> str:
        return Path(
            os.path.relpath(str(target.resolve()), start=str(report_dir.resolve()))
        ).as_posix()
