from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from cfd_automation.design_loop import GenerativeDesignLoop
from cfd_automation.runner import AutomationRunner


def _make_project(tmp_path: Path, *, cases_csv: str = "case_id\n") -> Path:
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
            "max_retries": 0,
        },
        "design_loop": {
            "batch_size_default": 4,
            "max_batches_default": 2,
            "random_seed": 7,
            "penalty_missing_objective": 1e9,
            "penalty_constraint": 1e6,
            "restore_cases_csv_after_run": True,
            "use_llm_explanations": False,
        },
        "solve": {
            "enabled": False,
            "skip_if_results_exist": True,
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


def test_design_loop_runs_multiple_batches(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(tmp_path)
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")

    runner = AutomationRunner(project)
    loop = GenerativeDesignLoop(runner)
    summary = loop.run(
        payload={
            "objective_alias": "fin_height_mm",
            "objective_goal": "min",
            "search_space": [
                {"name": "fin_height_mm", "type": "real", "min": 5, "max": 20},
                {"name": "fin_spacing_mm", "type": "real", "min": 2, "max": 10},
            ],
            "constraints": [{"alias": "fin_spacing_mm", "operator": "<=", "threshold": 9}],
            "batch_size": 3,
            "max_batches": 2,
            "use_llm_explanations": False,
        }
    )

    assert summary["status"] == "finished"
    assert summary["completed_batches"] == 2
    assert summary["best_case"]["case_id"].startswith("LOOP_B")
    assert summary["best_case"]["objective_value"] is not None
    assert Path(summary["loop_dir"]).exists()
    assert Path(summary["loop_dir"]).joinpath("loop_summary.json").exists()


def test_design_loop_can_be_stopped(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(tmp_path)
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")

    runner = AutomationRunner(project)
    loop = GenerativeDesignLoop(runner)

    state = {"stop": False}

    def on_progress(event: dict) -> None:
        if event.get("type") == "loop_batch_finished":
            state["stop"] = True

    summary = loop.run(
        payload={
            "objective_alias": "fin_height_mm",
            "objective_goal": "min",
            "search_space": [
                {"name": "fin_height_mm", "type": "real", "min": 5, "max": 20},
            ],
            "batch_size": 2,
            "max_batches": 4,
            "use_llm_explanations": False,
        },
        progress=on_progress,
        should_stop=lambda: bool(state["stop"]),
    )

    assert summary["status"] == "stopped"
    assert summary["completed_batches"] == 1


def test_design_loop_reports_optimizer_mode(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(tmp_path)
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")

    runner = AutomationRunner(project)
    loop = GenerativeDesignLoop(runner)
    summary = loop.run(
        payload={
            "objective_alias": "fin_height_mm",
            "objective_goal": "min",
            "search_space": [
                {"name": "fin_height_mm", "type": "real", "min": 5, "max": 20},
            ],
            "batch_size": 1,
            "max_batches": 1,
            "use_llm_explanations": False,
        }
    )

    assert summary["optimizer_mode"] in {"bayesian_gp", "random_fallback"}
    if summary["optimizer_mode"] == "random_fallback":
        assert summary["optimizer_warning"]
    else:
        assert summary["optimizer_warning"] == ""


def test_design_loop_marks_null_objective_as_failed(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(tmp_path)
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")

    runner = AutomationRunner(project)
    loop = GenerativeDesignLoop(runner)
    summary = loop.run(
        payload={
            "objective_alias": "temp_max_c",
            "objective_goal": "min",
            "search_space": [
                {"name": "fin_height_mm", "type": "real", "min": 5, "max": 20},
            ],
            "batch_size": 2,
            "max_batches": 1,
            "use_llm_explanations": False,
        }
    )

    assert summary["completed_batches"] == 1
    batch = summary["history"][0]
    for item in batch["cases"]:
        assert item["success"] is False
        assert item["failure_type"] == "null_metric"
        assert item["objective_value"] is None


def test_metric_contract_validation_detects_missing_aliases(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(tmp_path)
    runner = AutomationRunner(project)

    cfg = runner.get_config()
    cfg["metrics"] = [
        {
            "alias": "temp_max_c",
            "section": "field variable results summary",
            "quantity": "temp.max",
            "unit": "C",
        }
    ]
    runner.save_config(cfg)

    monkeypatch.setattr(
        runner,
        "introspect",
        lambda study_override=None: {
            "data": {
                "ok": True,
                "selected": {
                    "summary_catalog": {
                        "available": True,
                        "warnings": [],
                        "sections": [
                            {
                                "name": "other section",
                                "quantities": [{"name": "other.quantity", "unit": ""}],
                            }
                        ],
                    }
                },
            }
        },
    )

    result = runner.validate_metric_contract()
    assert result["ok"] is False
    assert len(result["missing_metrics"]) == 1
    assert result["missing_metrics"][0]["alias"] == "temp_max_c"


def test_metric_contract_validation_rejects_stale_study_context(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(tmp_path)
    runner = AutomationRunner(project)

    cfg = runner.get_config()
    cfg["study"]["template_model"] = "C:/study_a/model.cfdst"
    cfg["metrics"] = [
        {
            "alias": "temp_max_c",
            "section": "field variable results summary",
            "quantity": "temp.max",
            "unit": "C",
        }
    ]
    runner.save_config(cfg)

    monkeypatch.setattr(
        runner,
        "introspect",
        lambda study_override=None: {
            "data": {
                "ok": True,
                "study_path": "C:/study_b/model.cfdst",
                "selected": {
                    "design": "",
                    "scenario": "",
                    "summary_catalog": {
                        "available": True,
                        "warnings": [],
                        "sections": [
                            {
                                "name": "field variable results summary",
                                "quantities": [{"name": "temp.max", "unit": "C"}],
                            }
                        ],
                    },
                },
            }
        },
    )

    with pytest.raises(ValueError, match="stale introspection context"):
        runner.validate_metric_contract()
