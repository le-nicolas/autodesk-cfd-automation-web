from __future__ import annotations

from typing import Any

from cfd_automation.llm_cases import LLMCaseGenerator


def test_llm_generator_normalizes_rows_and_case_ids() -> None:
    def fake_transport(
        _url: str,
        _headers: dict[str, str],
        _payload: dict[str, Any],
        _timeout: int,
    ) -> dict[str, Any]:
        return {
            "message": {
                "content": (
                    '{"rows":[{"inlet_velocity_ms":1,"turbulence_model":"k-epsilon"},'
                    '{"case_id":"CASE_001","inlet_velocity_ms":2,"turbulence_model":"k-omega"}],'
                    '"notes":"Generated from range and variants."}'
                )
            }
        }

    cfg = {
        "provider": "ollama",
        "ollama": {
            "base_url": "http://127.0.0.1:11434",
            "model": "unit-test-model",
            "timeout_seconds": 5,
        },
        "max_rows": 50,
    }
    run_cfg = {
        "parameter_mappings": [
            {"source_column": "inlet_velocity_ms"},
            {"source_column": "turbulence_model"},
        ]
    }
    generator = LLMCaseGenerator(cfg, transport=fake_transport)
    result = generator.generate(
        prompt="test 1 and 2 m/s with two turbulence models",
        config=run_cfg,
        existing_rows=[],
    )

    assert result["row_count"] == 2
    assert result["rows"][0]["case_id"] == "CASE_001"
    assert result["rows"][1]["case_id"] == "CASE_001_2"
    assert "inlet_velocity_ms" in result["csv"]
    assert "turbulence_model" in result["csv"]


def test_llm_generate_endpoint_apply(monkeypatch) -> None:
    import app as web_app

    class FakeGenerator:
        def __init__(self, _cfg: dict[str, Any]) -> None:
            pass

        def generate(self, **_kwargs: Any) -> dict[str, Any]:
            csv_text = "case_id,inlet_velocity_ms\nCASE_001,1\nCASE_002,2\n"
            return {
                "provider": "ollama",
                "model": "fake-model",
                "row_count": 2,
                "rows": [
                    {"case_id": "CASE_001", "inlet_velocity_ms": "1"},
                    {"case_id": "CASE_002", "inlet_velocity_ms": "2"},
                ],
                "csv": csv_text,
                "notes": "ok",
            }

    saved: dict[str, Any] = {}

    def fake_save_cases(csv_text: str) -> list[dict[str, str]]:
        saved["csv"] = csv_text
        return []

    monkeypatch.setattr(web_app, "API_KEY", "")
    monkeypatch.setattr(web_app, "LLMCaseGenerator", FakeGenerator)
    monkeypatch.setattr(
        web_app.runner,
        "get_config",
        lambda: {"llm": {"provider": "ollama"}, "parameter_mappings": []},
    )
    monkeypatch.setattr(web_app.runner, "get_cases", lambda: [])
    monkeypatch.setattr(web_app.runner, "save_cases_csv", fake_save_cases)

    client = web_app.app.test_client()
    response = client.post(
        "/api/llm/generate-cases",
        json={"prompt": "make two rows", "apply": True},
    )
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["applied"] is True
    assert payload["row_count"] == 2
    assert "CASE_001" in saved["csv"]
