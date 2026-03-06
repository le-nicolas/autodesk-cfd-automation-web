from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from cfd_automation.config_io import DEFAULT_CONFIG, save_config
from cfd_automation.runner import AutomationRunner
from cfd_automation.surrogate import SurrogateEngine
from cfd_automation.utils import write_json


def _build_project(tmp_path: Path) -> Path:
    project = tmp_path
    (project / "config").mkdir(parents=True, exist_ok=True)
    (project / "runtime" / "runs").mkdir(parents=True, exist_ok=True)

    cfg = deepcopy(DEFAULT_CONFIG)
    cfg["solve"]["enabled"] = False
    save_config(project / "config" / "study_config.yaml", cfg)
    (project / "config" / "cases.csv").write_text("case_id,inlet_velocity_ms\nBASE,1\n", encoding="utf-8")

    run_id = "20260101_000000"
    run_dir = project / "runtime" / "runs" / run_id
    cases_root = run_dir / "cases"
    cases_root.mkdir(parents=True, exist_ok=True)

    case_results: list[dict[str, object]] = []
    for idx in range(1, 81):
        case_id = f"CASE_{idx:03d}"
        velocity = 1.0 + (idx % 15) * 0.25
        ambient = 20.0 + (idx % 8) * 2.0
        heat = 60.0 + (idx % 11) * 5.0
        temp = 0.65 * ambient + 0.42 * heat - 2.7 * velocity + 30.0
        pressure = 900.0 + 95.0 * velocity + 0.5 * heat

        attempt_dir = cases_root / case_id / "attempt_1"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        payload_path = attempt_dir / "payload.json"
        write_json(
            payload_path,
            {
                "case": {
                    "case_id": case_id,
                    "inlet_velocity_ms": velocity,
                    "ambient_temp_c": ambient,
                    "total_heat_w": heat,
                }
            },
        )

        case_results.append(
            {
                "case_id": case_id,
                "success": True,
                "metrics": {
                    "temp_max_c": temp,
                    "pressure_max_dyne_cm2": pressure,
                },
                "payload_path": str(payload_path.relative_to(project)).replace("/", "\\"),
                "screenshots": [],
            }
        )

    write_json(
        run_dir / "run_summary.json",
        {
            "run_id": run_id,
            "mode": "all",
            "results": {},
            "case_results": case_results,
            "postprocess": {"rows": len(case_results), "success_count": len(case_results), "failed_count": 0},
        },
    )
    return project


def test_surrogate_train_predict_and_coverage(tmp_path: Path) -> None:
    project = _build_project(tmp_path)
    runner = AutomationRunner(project)
    engine = SurrogateEngine(project, runner)

    trained = engine.train(
        objective_alias="temp_max_c",
        include_design_loops=False,
        min_rows=40,
    )
    assert trained["trained"] is True
    assert trained["row_count"] >= 70
    assert trained["model_name"]

    status = engine.status()
    assert status["trained"] is True
    assert status["target_alias"] == "temp_max_c"

    prediction = engine.predict_mode(
        {
            "objective_alias": "temp_max_c",
            "objective_goal": "min",
            "sample_count": 300,
            "top_n": 8,
            "search_space": [
                {"name": "inlet_velocity_ms", "type": "real", "min": 1.0, "max": 5.0},
                {"name": "ambient_temp_c", "type": "real", "min": 20.0, "max": 36.0},
                {"name": "total_heat_w", "type": "real", "min": 60.0, "max": 110.0},
            ],
        }
    )
    assert prediction["rows_evaluated"] == 300
    assert len(prediction["top_candidates"]) == 8
    assert "confidence" in prediction["top_candidates"][0]

    coverage = engine.coverage()
    assert "overall" in coverage
    assert "map" in coverage
    assert isinstance(coverage["map"].get("cells", []), list)


def test_surrogate_validate_mode_restores_cases(tmp_path: Path, monkeypatch) -> None:
    project = _build_project(tmp_path)
    runner = AutomationRunner(project)
    engine = SurrogateEngine(project, runner)
    engine.train(objective_alias="temp_max_c", include_design_loops=False, min_rows=40)

    original_cases_csv = runner.get_cases_csv()
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")
    result = engine.validate_mode(
        {
            "objective_alias": "temp_max_c",
            "objective_goal": "min",
            "sample_count": 60,
            "top_n": 5,
            "validate_top_n": 3,
            "search_space": [
                {"name": "inlet_velocity_ms", "type": "real", "min": 1.0, "max": 5.0},
                {"name": "ambient_temp_c", "type": "real", "min": 20.0, "max": 36.0},
                {"name": "total_heat_w", "type": "real", "min": 60.0, "max": 110.0},
            ],
            "auto_retrain": False,
        }
    )

    assert result["mode"] == "validate"
    assert result["validated_count"] == 3
    assert len(result["validation_table"]) == 3
    assert runner.get_cases_csv() == original_cases_csv
