from __future__ import annotations

import json
import os
from typing import Any, Callable
from urllib import error, request

from .config_io import cases_to_csv


JsonDict = dict[str, Any]
TransportFn = Callable[[str, dict[str, str], JsonDict, int], JsonDict]


def _post_json(
    url: str,
    headers: dict[str, str],
    payload: JsonDict,
    timeout_seconds: int,
) -> JsonDict:
    req_headers = {"Content-Type": "application/json", **headers}
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url=url, data=data, headers=req_headers, method="POST")
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except error.HTTPError as ex:
        body = ex.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"LLM endpoint HTTP {ex.code}: {body[:500]}") from ex
    except error.URLError as ex:
        reason = getattr(ex, "reason", ex)
        raise RuntimeError(f"LLM endpoint unreachable: {reason}") from ex

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as ex:
        raise RuntimeError(f"LLM endpoint returned invalid JSON: {raw[:500]}") from ex
    if not isinstance(parsed, dict):
        raise RuntimeError("LLM endpoint returned non-object JSON payload.")
    return parsed


def _find_first_json_object(text: str) -> str:
    start = text.find("{")
    if start < 0:
        raise ValueError("No JSON object found in LLM output.")

    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        ch = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]

    raise ValueError("Could not locate a complete JSON object in LLM output.")


def _value_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, dict)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
    return str(value)


def _sanitize_case_id(value: str) -> str:
    clean = "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in value.strip())
    clean = clean.strip("._-")
    return clean


def _to_float_or_none(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _to_int_or_none(value: Any) -> int | None:
    if value in ("", None):
        return None
    try:
        return int(round(float(str(value).strip())))
    except Exception:
        return None


def _mapping_source_column(mapping: dict[str, Any]) -> str:
    if not isinstance(mapping, dict):
        return ""
    return str(mapping.get("source_column", mapping.get("param", ""))).strip()


def _mapping_match(mapping: dict[str, Any]) -> dict[str, Any]:
    match = mapping.get("match", {})
    if not isinstance(match, dict):
        match = {}
    else:
        match = dict(match)
    target_name = str(mapping.get("target_name", "")).strip()
    if target_name and not str(match.get("name", "")).strip():
        match["name"] = target_name
    return match


def _normalize_rows(
    rows: Any,
    *,
    suggested_columns: list[str],
    max_rows: int,
) -> list[dict[str, str]]:
    if not isinstance(rows, list):
        raise ValueError("LLM response must contain `rows` as a list.")
    if not rows:
        raise ValueError("LLM returned zero rows.")

    limited_rows = rows[:max_rows]
    normalized: list[dict[str, str]] = []
    seen_case_ids: set[str] = set()

    for index, row in enumerate(limited_rows, start=1):
        if not isinstance(row, dict):
            raise ValueError("Every generated row must be a JSON object.")
        item: dict[str, str] = {}
        for key, value in row.items():
            key_text = str(key).strip()
            if not key_text:
                continue
            item[key_text] = _value_to_text(value)

        raw_case_id = _sanitize_case_id(item.get("case_id", ""))
        if not raw_case_id or raw_case_id.isdigit():
            case_id = f"CASE_{index:03d}"
        else:
            case_id = raw_case_id
        base_case_id = case_id
        suffix = 2
        while case_id.lower() in seen_case_ids:
            case_id = f"{base_case_id}_{suffix}"
            suffix += 1
        item["case_id"] = case_id
        seen_case_ids.add(case_id.lower())
        normalized.append(item)

    ordered_columns = ["case_id"]
    for col in suggested_columns:
        if col and col not in ordered_columns:
            ordered_columns.append(col)

    reordered_rows: list[dict[str, str]] = []
    for item in normalized:
        for col in ordered_columns:
            item.setdefault(col, "")
        ordered: dict[str, str] = {}
        for col in ordered_columns:
            ordered[col] = item.get(col, "")
        for key, value in item.items():
            if key not in ordered:
                ordered[key] = value
        reordered_rows.append(ordered)

    return reordered_rows


def _extract_content_from_groq(payload: JsonDict) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("Groq response does not contain choices.")
    choice0 = choices[0]
    if not isinstance(choice0, dict):
        raise RuntimeError("Groq response choice format is invalid.")
    message = choice0.get("message")
    if not isinstance(message, dict):
        raise RuntimeError("Groq response message is missing.")
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("Groq response content is empty.")
    return content


def _extract_content_from_ollama(payload: JsonDict) -> str:
    # Native Ollama /api/chat format.
    message = payload.get("message")
    if isinstance(message, dict):
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content

    # Fallback for OpenAI-compatible proxy payloads.
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        choice0 = choices[0]
        if isinstance(choice0, dict):
            msg = choice0.get("message")
            if isinstance(msg, dict):
                content = msg.get("content")
                if isinstance(content, str) and content.strip():
                    return content
    raise RuntimeError("Ollama response content is empty.")


class LLMCaseGenerator:
    def __init__(self, llm_config: dict[str, Any], transport: TransportFn | None = None):
        self._cfg = llm_config or {}
        self._transport = transport or _post_json

    @staticmethod
    def _suggested_columns(
        *,
        config: dict[str, Any],
        existing_rows: list[dict[str, Any]],
    ) -> list[str]:
        columns: list[str] = ["case_id"]
        for mapping in config.get("parameter_mappings", []):
            if not isinstance(mapping, dict):
                continue
            name = _mapping_source_column(mapping)
            if name and name not in columns:
                columns.append(name)

        for row in existing_rows[:8]:
            if not isinstance(row, dict):
                continue
            for key in row.keys():
                name = str(key).strip()
                if name and name not in columns:
                    columns.append(name)
        return columns

    @staticmethod
    def _build_messages(
        *,
        prompt: str,
        config: dict[str, Any],
        existing_rows: list[dict[str, Any]],
        suggested_columns: list[str],
        max_rows: int,
    ) -> list[dict[str, str]]:
        mapping_hints: list[dict[str, Any]] = []
        for item in config.get("parameter_mappings", []):
            if not isinstance(item, dict):
                continue
            match = _mapping_match(item)
            mapping_hints.append(
                {
                    "source_column": _mapping_source_column(item),
                    "target_type": item.get("target_type", ""),
                    "target_name": match.get("name", ""),
                    "match_type": match.get("type", ""),
                    "property": item.get("property", ""),
                    "values": item.get("values", {}) if isinstance(item.get("values", {}), dict) else {},
                    "units": item.get("units", ""),
                }
            )
        existing_preview = existing_rows[:3]

        system_text = (
            "You generate CFD parametric case matrices.\n"
            "Return only one JSON object and no markdown.\n"
            "Required schema: {\"rows\":[{...}],\"notes\":\"...\"}\n"
            "Rules:\n"
            "- rows must be an array of objects.\n"
            "- Expand ranges/enumerations into explicit rows.\n"
            "- Include case_id in every row.\n"
            "- Prefer snake_case column names with unit suffixes when relevant.\n"
            "- Keep total rows <= max_rows."
        )
        user_text = (
            f"Natural language request:\n{prompt.strip()}\n\n"
            f"max_rows: {max_rows}\n"
            f"Suggested columns: {json.dumps(suggested_columns, ensure_ascii=True)}\n"
            f"Parameter mapping hints: {json.dumps(mapping_hints, ensure_ascii=True)}\n"
            f"Existing case examples: {json.dumps(existing_preview, ensure_ascii=True)}\n"
            "Return valid JSON only."
        )
        return [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ]

    def _run_ollama(
        self,
        *,
        messages: list[dict[str, str]],
        temperature: float,
    ) -> tuple[str, str]:
        ollama_cfg = self._cfg.get("ollama", {}) if isinstance(self._cfg, dict) else {}
        base_url = str(ollama_cfg.get("base_url", "http://127.0.0.1:11434")).rstrip("/")
        model = str(ollama_cfg.get("model", "llama3.2:3b")).strip()
        timeout = int(ollama_cfg.get("timeout_seconds", 120) or 120)
        if not model:
            raise ValueError("llm.ollama.model is empty.")

        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {"temperature": float(temperature)},
        }
        response = self._transport(f"{base_url}/api/chat", {}, payload, timeout)
        content = _extract_content_from_ollama(response)
        return model, content

    def _run_groq(
        self,
        *,
        messages: list[dict[str, str]],
        temperature: float,
    ) -> tuple[str, str]:
        groq_cfg = self._cfg.get("groq", {}) if isinstance(self._cfg, dict) else {}
        base_url = str(groq_cfg.get("base_url", "https://api.groq.com/openai/v1")).rstrip("/")
        model = str(groq_cfg.get("model", "llama-3.1-8b-instant")).strip()
        timeout = int(groq_cfg.get("timeout_seconds", 60) or 60)
        api_key_env = str(groq_cfg.get("api_key_env", "GROQ_API_KEY")).strip() or "GROQ_API_KEY"
        api_key = os.environ.get(api_key_env, "").strip()
        if not api_key:
            raise ValueError(f"Groq API key missing. Set environment variable: {api_key_env}")
        if not model:
            raise ValueError("llm.groq.model is empty.")

        payload = {
            "model": model,
            "messages": messages,
            "temperature": float(temperature),
        }
        headers = {"Authorization": f"Bearer {api_key}"}
        response = self._transport(f"{base_url}/chat/completions", headers, payload, timeout)
        content = _extract_content_from_groq(response)
        return model, content

    def generate(
        self,
        *,
        prompt: str,
        config: dict[str, Any],
        existing_rows: list[dict[str, Any]],
        max_rows_override: int | None = None,
    ) -> dict[str, Any]:
        task = str(prompt or "").strip()
        if not task:
            raise ValueError("Prompt is empty.")

        provider = str(self._cfg.get("provider", "ollama")).strip().lower() or "ollama"
        temperature = float(self._cfg.get("temperature", 0.1) or 0.1)
        max_rows_cfg = int(self._cfg.get("max_rows", 200) or 200)
        max_rows = max(1, min(int(max_rows_override or max_rows_cfg), 1000))

        suggested_columns = self._suggested_columns(config=config, existing_rows=existing_rows)
        messages = self._build_messages(
            prompt=task,
            config=config,
            existing_rows=existing_rows,
            suggested_columns=suggested_columns,
            max_rows=max_rows,
        )

        if provider == "ollama":
            model, content = self._run_ollama(messages=messages, temperature=temperature)
        elif provider == "groq":
            model, content = self._run_groq(messages=messages, temperature=temperature)
        else:
            raise ValueError(f"Unsupported llm.provider: {provider}")

        json_block = _find_first_json_object(content.strip())
        try:
            parsed = json.loads(json_block)
        except json.JSONDecodeError as ex:
            raise ValueError("LLM returned malformed JSON object.") from ex
        if not isinstance(parsed, dict):
            raise ValueError("LLM output JSON root must be an object.")

        rows = _normalize_rows(
            parsed.get("rows"),
            suggested_columns=suggested_columns,
            max_rows=max_rows,
        )
        csv_text = cases_to_csv(rows)
        return {
            "provider": provider,
            "model": model,
            "row_count": len(rows),
            "rows": rows,
            "csv": csv_text,
            "notes": str(parsed.get("notes", "")).strip(),
            "raw_content": content,
        }


class LLMMeshAdvisor:
    def __init__(self, llm_config: dict[str, Any], transport: TransportFn | None = None):
        self._cfg = llm_config or {}
        self._transport = transport or _post_json

    @staticmethod
    def _infer_numeric_ranges(existing_rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
        ranges: dict[str, dict[str, float]] = {}
        candidates: dict[str, list[float]] = {}
        for row in existing_rows:
            if not isinstance(row, dict):
                continue
            for key, value in row.items():
                key_text = str(key).strip()
                if key_text.lower() == "case_id":
                    continue
                parsed = _to_float_or_none(value)
                if parsed is None:
                    continue
                candidates.setdefault(key_text, []).append(parsed)

        for key, values in candidates.items():
            if not values:
                continue
            ranges[key] = {
                "min": min(values),
                "max": max(values),
            }
        return ranges

    @staticmethod
    def _mesh_context(config: dict[str, Any], existing_rows: list[dict[str, Any]]) -> dict[str, Any]:
        study_cfg = config.get("study", {}) if isinstance(config.get("study", {}), dict) else {}
        mesh_cfg = config.get("mesh", {}) if isinstance(config.get("mesh", {}), dict) else {}
        defaults = mesh_cfg.get("default_params", {}) if isinstance(mesh_cfg.get("default_params", {}), dict) else {}
        gate = mesh_cfg.get("quality_gate", {}) if isinstance(mesh_cfg.get("quality_gate", {}), dict) else {}

        mapping_hints: list[dict[str, Any]] = []
        for mapping in config.get("parameter_mappings", []):
            if not isinstance(mapping, dict):
                continue
            match = _mapping_match(mapping)
            mapping_hints.append(
                {
                    "source_column": _mapping_source_column(mapping),
                    "target_type": mapping.get("target_type", ""),
                    "target_name": match.get("name", ""),
                    "match_type": match.get("type", ""),
                    "values": mapping.get("values", {}) if isinstance(mapping.get("values", {}), dict) else {},
                    "units": mapping.get("units", ""),
                }
            )

        numeric_ranges = LLMMeshAdvisor._infer_numeric_ranges(existing_rows)
        return {
            "study": {
                "template_model": study_cfg.get("template_model", ""),
                "design_name": study_cfg.get("design_name", ""),
                "scenario_name": study_cfg.get("scenario_name", ""),
                "physics_type": study_cfg.get("physics_type", ""),
                "fluid": study_cfg.get("fluid", ""),
                "fluid_preset": study_cfg.get("fluid_preset", ""),
                "geometry_characteristic_length_m": study_cfg.get("geometry_characteristic_length_m", ""),
            },
            "parameter_mappings": mapping_hints,
            "case_numeric_ranges": numeric_ranges,
            "mesh_defaults": defaults,
            "mesh_quality_gate": gate,
            "sample_cases": existing_rows[:4],
        }

    @staticmethod
    def _build_messages(
        *,
        prompt: str,
        context: dict[str, Any],
    ) -> list[dict[str, str]]:
        system_text = (
            "You are a CFD mesh advisor.\n"
            "Return exactly one JSON object and no markdown.\n"
            "Schema:\n"
            "{"
            "\"mesh_params\":{"
            "\"target_y_plus\":number|null,"
            "\"inflation_layers\":integer|null,"
            "\"max_element_size_m\":number|null,"
            "\"min_element_size_m\":number|null,"
            "\"refinement_zones\":[{\"name\":\"...\",\"size_m\":number,\"rationale\":\"...\"}]"
            "},"
            "\"quality_gate\":{"
            "\"skewness_max\":number|null,"
            "\"aspect_ratio_max\":number|null,"
            "\"orthogonality_min\":number|null,"
            "\"element_count_min\":number|null,"
            "\"element_count_max\":number|null"
            "},"
            "\"notes\":\"...\""
            "}\n"
            "Rules:\n"
            "- Keep values realistic for external turbulent airflow and internal CFD if context is ambiguous.\n"
            "- Use SI units in all values.\n"
            "- Do not include any keys outside schema."
        )
        user_text = (
            f"Request: {prompt.strip()}\n\n"
            f"Context JSON:\n{json.dumps(context, ensure_ascii=True)}\n\n"
            "Return JSON only."
        )
        return [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ]

    def _run_ollama(
        self,
        *,
        messages: list[dict[str, str]],
        temperature: float,
    ) -> tuple[str, str]:
        ollama_cfg = self._cfg.get("ollama", {}) if isinstance(self._cfg, dict) else {}
        base_url = str(ollama_cfg.get("base_url", "http://127.0.0.1:11434")).rstrip("/")
        model = str(ollama_cfg.get("model", "llama3.2:3b")).strip()
        timeout = int(ollama_cfg.get("timeout_seconds", 120) or 120)
        if not model:
            raise ValueError("llm.ollama.model is empty.")

        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {"temperature": float(temperature)},
        }
        response = self._transport(f"{base_url}/api/chat", {}, payload, timeout)
        content = _extract_content_from_ollama(response)
        return model, content

    def _run_groq(
        self,
        *,
        messages: list[dict[str, str]],
        temperature: float,
    ) -> tuple[str, str]:
        groq_cfg = self._cfg.get("groq", {}) if isinstance(self._cfg, dict) else {}
        base_url = str(groq_cfg.get("base_url", "https://api.groq.com/openai/v1")).rstrip("/")
        model = str(groq_cfg.get("model", "llama-3.1-8b-instant")).strip()
        timeout = int(groq_cfg.get("timeout_seconds", 60) or 60)
        api_key_env = str(groq_cfg.get("api_key_env", "GROQ_API_KEY")).strip() or "GROQ_API_KEY"
        api_key = os.environ.get(api_key_env, "").strip()
        if not api_key:
            raise ValueError(f"Groq API key missing. Set environment variable: {api_key_env}")
        if not model:
            raise ValueError("llm.groq.model is empty.")

        payload = {
            "model": model,
            "messages": messages,
            "temperature": float(temperature),
        }
        headers = {"Authorization": f"Bearer {api_key}"}
        response = self._transport(f"{base_url}/chat/completions", headers, payload, timeout)
        content = _extract_content_from_groq(response)
        return model, content

    @staticmethod
    def _normalize_refinement_zones(raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        zones: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            size_m = _to_float_or_none(item.get("size_m"))
            rationale = str(item.get("rationale", "")).strip()
            zones.append(
                {
                    "name": name,
                    "size_m": size_m if size_m is not None else "",
                    "rationale": rationale,
                }
            )
        return zones

    @staticmethod
    def _normalize_payload(parsed: dict[str, Any]) -> dict[str, Any]:
        mesh_raw = parsed.get("mesh_params", {}) if isinstance(parsed.get("mesh_params", {}), dict) else {}
        gate_raw = parsed.get("quality_gate", {}) if isinstance(parsed.get("quality_gate", {}), dict) else {}
        mesh_params = {
            "target_y_plus": _to_float_or_none(mesh_raw.get("target_y_plus")),
            "inflation_layers": _to_int_or_none(mesh_raw.get("inflation_layers")),
            "max_element_size_m": _to_float_or_none(mesh_raw.get("max_element_size_m")),
            "min_element_size_m": _to_float_or_none(mesh_raw.get("min_element_size_m")),
            "refinement_zones": LLMMeshAdvisor._normalize_refinement_zones(
                mesh_raw.get("refinement_zones", [])
            ),
        }
        quality_gate = {
            "skewness_max": _to_float_or_none(gate_raw.get("skewness_max")),
            "aspect_ratio_max": _to_float_or_none(gate_raw.get("aspect_ratio_max")),
            "orthogonality_min": _to_float_or_none(gate_raw.get("orthogonality_min")),
            "element_count_min": _to_int_or_none(gate_raw.get("element_count_min")),
            "element_count_max": _to_int_or_none(gate_raw.get("element_count_max")),
        }
        return {
            "mesh_params": mesh_params,
            "quality_gate": quality_gate,
            "notes": str(parsed.get("notes", "")).strip(),
        }

    def suggest(
        self,
        *,
        prompt: str,
        config: dict[str, Any],
        existing_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        task = str(prompt or "").strip()
        if not task:
            raise ValueError("Prompt is empty.")

        provider = str(self._cfg.get("provider", "ollama")).strip().lower() or "ollama"
        temperature = float(self._cfg.get("temperature", 0.1) or 0.1)
        context = self._mesh_context(config, existing_rows)
        messages = self._build_messages(prompt=task, context=context)

        if provider == "ollama":
            model, content = self._run_ollama(messages=messages, temperature=temperature)
        elif provider == "groq":
            model, content = self._run_groq(messages=messages, temperature=temperature)
        else:
            raise ValueError(f"Unsupported llm.provider: {provider}")

        json_block = _find_first_json_object(content.strip())
        try:
            parsed = json.loads(json_block)
        except json.JSONDecodeError as ex:
            raise ValueError("LLM returned malformed JSON object.") from ex
        if not isinstance(parsed, dict):
            raise ValueError("LLM output JSON root must be an object.")

        normalized = self._normalize_payload(parsed)
        return {
            "provider": provider,
            "model": model,
            "mesh_params": normalized["mesh_params"],
            "quality_gate": normalized["quality_gate"],
            "notes": normalized["notes"],
            "raw_content": content,
        }


class LLMOptimizerNarrator:
    def __init__(self, llm_config: dict[str, Any], transport: TransportFn | None = None):
        self._cfg = llm_config or {}
        self._transport = transport or _post_json

    def _run_ollama(self, *, messages: list[dict[str, str]], temperature: float) -> tuple[str, str]:
        ollama_cfg = self._cfg.get("ollama", {}) if isinstance(self._cfg, dict) else {}
        base_url = str(ollama_cfg.get("base_url", "http://127.0.0.1:11434")).rstrip("/")
        model = str(ollama_cfg.get("model", "llama3.2:3b")).strip()
        timeout = int(ollama_cfg.get("timeout_seconds", 120) or 120)
        if not model:
            raise ValueError("llm.ollama.model is empty.")
        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {"temperature": float(temperature)},
        }
        response = self._transport(f"{base_url}/api/chat", {}, payload, timeout)
        content = _extract_content_from_ollama(response)
        return model, content

    def _run_groq(self, *, messages: list[dict[str, str]], temperature: float) -> tuple[str, str]:
        groq_cfg = self._cfg.get("groq", {}) if isinstance(self._cfg, dict) else {}
        base_url = str(groq_cfg.get("base_url", "https://api.groq.com/openai/v1")).rstrip("/")
        model = str(groq_cfg.get("model", "llama-3.1-8b-instant")).strip()
        timeout = int(groq_cfg.get("timeout_seconds", 60) or 60)
        api_key_env = str(groq_cfg.get("api_key_env", "GROQ_API_KEY")).strip() or "GROQ_API_KEY"
        api_key = os.environ.get(api_key_env, "").strip()
        if not api_key:
            raise ValueError(f"Groq API key missing. Set environment variable: {api_key_env}")
        if not model:
            raise ValueError("llm.groq.model is empty.")
        payload = {
            "model": model,
            "messages": messages,
            "temperature": float(temperature),
        }
        headers = {"Authorization": f"Bearer {api_key}"}
        response = self._transport(f"{base_url}/chat/completions", headers, payload, timeout)
        content = _extract_content_from_groq(response)
        return model, content

    @staticmethod
    def _build_messages(
        *,
        objective_alias: str,
        objective_goal: str,
        constraints: list[dict[str, Any]],
        batch_records: list[dict[str, Any]],
        prior_best: dict[str, Any] | None,
    ) -> list[dict[str, str]]:
        system_text = (
            "You explain Bayesian optimization steps for CFD design studies.\n"
            "Return only one JSON object in schema: {\"summary\":\"...\"}\n"
            "Keep summary concise (2-5 sentences), actionable, and physically grounded.\n"
            "Mention feasibility/constraints and why next exploration region makes sense."
        )
        payload = {
            "objective_alias": objective_alias,
            "objective_goal": objective_goal,
            "constraints": constraints,
            "batch_records": batch_records[:30],
            "prior_best": prior_best or {},
        }
        user_text = f"Context:\n{json.dumps(payload, ensure_ascii=True)}\n\nReturn JSON only."
        return [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ]

    def narrate_batch(
        self,
        *,
        objective_alias: str,
        objective_goal: str,
        constraints: list[dict[str, Any]],
        batch_records: list[dict[str, Any]],
        prior_best: dict[str, Any] | None,
    ) -> dict[str, Any]:
        provider = str(self._cfg.get("provider", "ollama")).strip().lower() or "ollama"
        temperature = float(self._cfg.get("temperature", 0.1) or 0.1)
        messages = self._build_messages(
            objective_alias=objective_alias,
            objective_goal=objective_goal,
            constraints=constraints,
            batch_records=batch_records,
            prior_best=prior_best,
        )
        if provider == "ollama":
            model, content = self._run_ollama(messages=messages, temperature=temperature)
        elif provider == "groq":
            model, content = self._run_groq(messages=messages, temperature=temperature)
        else:
            raise ValueError(f"Unsupported llm.provider: {provider}")

        block = _find_first_json_object(content.strip())
        parsed = json.loads(block)
        if not isinstance(parsed, dict):
            raise ValueError("Narrator output root must be JSON object.")
        summary = str(parsed.get("summary", "")).strip()
        if not summary:
            summary = content.strip()[:1000]
        return {
            "provider": provider,
            "model": model,
            "text": summary,
        }
