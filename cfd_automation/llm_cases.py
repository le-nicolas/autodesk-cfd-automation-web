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
            name = str(mapping.get("source_column", "")).strip()
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
            mapping_hints.append(
                {
                    "source_column": item.get("source_column", ""),
                    "target_type": item.get("target_type", ""),
                    "match_type": (item.get("match") or {}).get("type", ""),
                    "property": item.get("property", ""),
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
