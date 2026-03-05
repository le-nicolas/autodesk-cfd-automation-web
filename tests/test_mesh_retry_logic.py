from __future__ import annotations

import json
import shutil
from pathlib import Path

import yaml

from cfd_automation.runner import AutomationRunner


def _make_project(
    tmp_path: Path,
    *,
    cases_csv: str,
    max_retries: int = 1,
) -> Path:
    project = tmp_path / "project"
    (project / "config").mkdir(parents=True, exist_ok=True)
    (project / "scripts").mkdir(parents=True, exist_ok=True)

    repo_root = Path(__file__).resolve().parents[1]
    shutil.copy2(repo_root / "scripts" / "cfd_case_runner.py", project / "scripts" / "cfd_case_runner.py")
    shutil.copy2(repo_root / "scripts" / "cfd_introspect.py", project / "scripts" / "cfd_introspect.py")

    config = {
        "study": {
            "template_model": "C:/nonexistent/model.cfdst",
            "design_name": "",
            "scenario_name": "",
        },
        "automation": {
            "cfd_executable": "CFD.exe",
            "timeout_minutes": 2,
            "max_retries": max_retries,
        },
        "solve": {
            "enabled": False,
            "skip_if_results_exist": True,
        },
        "mesh": {
            "quality_gate": {
                "enabled": True,
                "require_all_metrics": False,
                "skewness_max": 0.95,
                "aspect_ratio_max": 100.0,
                "orthogonality_min": 0.1,
                "element_count_min": 1000,
                "element_count_max": 50000000,
            },
            "retry": {
                "enabled": True,
                "strategy": ["coarsen", "refine"],
                "coarsen_size_scale": 1.35,
                "refine_size_scale": 0.75,
                "coarsen_inflation_delta": -1,
                "refine_inflation_delta": 1,
            },
            "default_params": {
                "max_element_size_m": 0.01,
                "min_element_size_m": 0.001,
                "inflation_layers": 5,
                "target_y_plus": 1.0,
                "refinement_zones": [],
            },
        },
        "outputs": {
            "save_all_summary": True,
            "screenshots": {"enabled": False, "views": ["default"]},
            "cutplanes": [],
            "report": {"enabled": True},
        },
        "metrics": [],
        "criteria": [],
        "ranking": [],
        "parameter_mappings": [],
    }
    (project / "config" / "study_config.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    (project / "config" / "cases.csv").write_text(cases_csv, encoding="utf-8")
    return project


def _get_failed_case(summary: dict) -> dict:
    failed = [case for case in summary["case_results"] if not case.get("success")]
    assert len(failed) == 1
    return failed[0]


def test_bad_mesh_retry_uses_mesh_adjustment(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(
        tmp_path,
        cases_csv="case_id,force_fail_type\nCASE_BAD_MESH,bad_mesh\n",
        max_retries=1,
    )
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")

    summary = AutomationRunner(project).run(mode="all")
    failed_case = _get_failed_case(summary)

    assert failed_case.get("failure_type") == "bad_mesh"
    assert failed_case.get("failure_mode") == "mesh_failure"
    assert int(failed_case.get("attempts", 0)) == 2

    payload_path = Path(failed_case["payload_path"])
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    assert payload["mesh_adjustment"]["direction"] in {"coarsen", "refine"}


def test_solver_divergence_retry_adjusts_mesh(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(
        tmp_path,
        cases_csv="case_id,force_fail_type\nCASE_DIVERGE,solver_divergence\n",
        max_retries=1,
    )
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")

    summary = AutomationRunner(project).run(mode="all")
    failed_case = _get_failed_case(summary)

    assert failed_case.get("failure_type") == "non_zero_exit"
    assert failed_case.get("failure_mode") == "solver_divergence"
    assert int(failed_case.get("attempts", 0)) == 2

    payload_path = Path(failed_case["payload_path"])
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    assert payload["mesh_adjustment"]["direction"] in {"coarsen", "refine"}


def test_script_failure_retry_keeps_same_mesh(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(
        tmp_path,
        cases_csv="case_id,force_fail_type\nCASE_SCRIPT,script_failure\n",
        max_retries=1,
    )
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")

    summary = AutomationRunner(project).run(mode="all")
    failed_case = _get_failed_case(summary)

    assert failed_case.get("failure_type") == "python_exception"
    assert failed_case.get("failure_mode") == "script_failure"
    assert int(failed_case.get("attempts", 0)) == 2

    payload_path = Path(failed_case["payload_path"])
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    assert payload["mesh_adjustment"] == {}
