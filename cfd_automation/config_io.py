from __future__ import annotations

import csv
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from .utils import stable_hash


DEFAULT_CONFIG: dict[str, Any] = {
    "study": {
        "template_model": "",
        "design_name": "",
        "scenario_name": "",
    },
    "automation": {
        "cfd_executable": "C:/Program Files/Autodesk/CFD 2026/CFD.exe",
        "timeout_minutes": 120,
        "max_retries": 1,
    },
    "llm": {
        "provider": "ollama",
        "temperature": 0.1,
        "max_rows": 200,
        "ollama": {
            "base_url": "http://127.0.0.1:11434",
            "model": "llama3.2:3b",
            "timeout_seconds": 120,
        },
        "groq": {
            "base_url": "https://api.groq.com/openai/v1",
            "model": "llama-3.1-8b-instant",
            "api_key_env": "GROQ_API_KEY",
            "timeout_seconds": 60,
        },
    },
    "solve": {
        "enabled": False,
        "skip_if_results_exist": True,
        "scenario_overrides": {
            "iterations": 100,
            "convergenceThreshold": 0.5,
        },
    },
    "outputs": {
        "save_all_summary": True,
        "screenshots": {
            "enabled": True,
            "views": ["default"],
        },
        "cutplanes": [],
        "report": {
            "enabled": True,
        },
    },
    "metrics": [
        {
            "alias": "temp_max_c",
            "section": "field variable results summary",
            "quantity": "temp.max",
            "unit": "C",
        },
        {
            "alias": "pressure_max_dyne_cm2",
            "section": "field variable results summary",
            "quantity": "press.max",
            "unit": "dyne/cm^2",
        },
        {
            "alias": "velocity_mag_max_cm_s",
            "section": "field variable results summary",
            "quantity": "vx vel.max",
            "unit": "cm/s",
        },
    ],
    "criteria": [
        {
            "alias": "temp_max_c",
            "operator": "<=",
            "threshold": 500.0,
        },
        {
            "alias": "pressure_max_dyne_cm2",
            "operator": "<=",
            "threshold": 2000.0,
        },
    ],
    "ranking": [
        {
            "alias": "temp_max_c",
            "goal": "min",
            "weight": 0.7,
        },
        {
            "alias": "pressure_max_dyne_cm2",
            "goal": "min",
            "weight": 0.3,
        },
    ],
    "parameter_mappings": [
        {
            "source_column": "inlet_velocity_ms",
            "target_type": "boundary_condition",
            "match": {"type": "Normal Velocity", "entity_ids": [18]},
            "property": "value",
            "units": "m/s",
        },
        {
            "source_column": "ambient_temp_c",
            "target_type": "boundary_condition",
            "match": {"type": "Temperature", "entity_ids": [18, 170]},
            "property": "value",
            "units": "Celsius",
        },
        {
            "source_column": "total_heat_w",
            "target_type": "boundary_condition",
            "match": {"type": "Total Heat Generation", "entity_names": ["Part1.Body24"]},
            "property": "value",
            "units": "W",
        },
        {
            "source_column": "chip_heat_gen_wm3",
            "target_type": "boundary_condition",
            "match": {"type": "Heat Generation"},
            "property": "value",
            "units": "W/m3",
        },
    ],
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return deepcopy(DEFAULT_CONFIG)
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Config file must contain a YAML object: {path}")
    return _deep_merge(DEFAULT_CONFIG, loaded)


def save_config(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )


def load_cases(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = [dict(row) for row in reader]
    for idx, row in enumerate(rows, start=1):
        if not row.get("case_id"):
            row["case_id"] = f"CASE_{idx:03d}"
    return rows


def save_cases(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("case_id\n", encoding="utf-8")
        return
    columns: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_cases_csv(csv_text: str) -> list[dict[str, str]]:
    lines = csv_text.splitlines()
    if not lines:
        return []
    reader = csv.DictReader(lines)
    rows = [dict(row) for row in reader]
    for idx, row in enumerate(rows, start=1):
        if not row.get("case_id"):
            row["case_id"] = f"CASE_{idx:03d}"
    return rows


def cases_to_csv(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "case_id\n"
    columns: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)
    output_lines: list[str] = []
    from io import StringIO

    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def case_fingerprint(case: dict[str, Any], config: dict[str, Any]) -> str:
    relevant = {
        "case": case,
        "parameter_mappings": config.get("parameter_mappings", []),
        "solve": config.get("solve", {}),
        "metrics": config.get("metrics", []),
    }
    return stable_hash(relevant)
