import ast
import csv
import json
import os
import re
import shutil
import traceback
from pathlib import Path

import CFD.Results as R
import CFD.Setup as S


BUILTIN_FLUID_PRESETS = {
    "air": {
        "match": {"type": "fluid"},
        "properties": {
            "density": {"value": 1.225, "units": "kg/m^3"},
            "dynamic_viscosity": {"value": 1.81e-5, "units": "Pa.s"},
            "specific_heat": {"value": 1006.0, "units": "J/kg-K"},
            "thermal_conductivity": {"value": 0.0242, "units": "W/m-K"},
        },
    },
    "water": {
        "match": {"type": "fluid"},
        "properties": {
            "density": {"value": 997.0, "units": "kg/m^3"},
            "dynamic_viscosity": {"value": 8.9e-4, "units": "Pa.s"},
            "specific_heat": {"value": 4182.0, "units": "J/kg-K"},
            "thermal_conductivity": {"value": 0.6, "units": "W/m-K"},
        },
    },
    "oil": {
        "match": {"type": "fluid"},
        "properties": {
            "density": {"value": 870.0, "units": "kg/m^3"},
            "dynamic_viscosity": {"value": 0.065, "units": "Pa.s"},
            "specific_heat": {"value": 2000.0, "units": "J/kg-K"},
            "thermal_conductivity": {"value": 0.145, "units": "W/m-K"},
        },
    },
}

FLUID_PROPERTY_ALIASES = {
    "density": ["massDensity", "fluidDensity", "rho"],
    "dynamic_viscosity": ["dynamicViscosity", "viscosity", "mu"],
    "specific_heat": ["specificHeat", "cp", "specificHeatCapacity"],
    "thermal_conductivity": ["thermalConductivity", "conductivity", "k"],
}


def parse_scalar(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if text == "":
        return None
    lower = text.lower()
    if lower in {"none", "null", "nan"}:
        return None
    if lower == "true":
        return True
    if lower == "false":
        return False
    try:
        if "." in text or "e" in lower:
            return float(text)
        return int(text)
    except ValueError:
        return text


def to_float_or_none(value):
    if value in ("", None):
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def to_int_or_none(value):
    if value in ("", None):
        return None
    try:
        return int(round(float(str(value).strip())))
    except Exception:
        return None


def normalize_path(path):
    return str(Path(path)).replace("\\", "/")


def copy_study(template_model, case_dir):
    template = Path(template_model)
    source_dir = template.parent
    dest_root = case_dir / "study_copy"
    if dest_root.exists():
        shutil.rmtree(dest_root)
    shutil.copytree(source_dir, dest_root)
    dest_model = dest_root / template.name
    return dest_model


def variant_to_python(variant):
    try:
        type_name = str(variant.typeName())
    except Exception:
        type_name = ""
    try:
        if type_name in {"int", "uint", "qlonglong", "qulonglong"}:
            return type_name, variant.toInt()
        if type_name in {"double", "float"}:
            return type_name, variant.toDouble()
        if type_name == "bool":
            return type_name, variant.toBool()
        return type_name, variant.toString()
    except Exception:
        return type_name, str(variant)


def coerce_value(current, raw):
    parsed = parse_scalar(raw)
    if isinstance(current, bool):
        if isinstance(parsed, bool):
            return parsed
        if isinstance(parsed, (int, float)):
            return bool(parsed)
        if isinstance(parsed, str):
            return parsed.lower() in {"1", "true", "on", "yes"}
        return False
    if isinstance(current, int) and not isinstance(current, bool):
        if isinstance(parsed, (int, float, bool)):
            return int(parsed)
        try:
            return int(str(parsed))
        except Exception:
            return current
    if isinstance(current, float):
        if isinstance(parsed, (int, float, bool)):
            return float(parsed)
        try:
            return float(str(parsed))
        except Exception:
            return current
    if parsed is None:
        return ""
    return str(parsed)


def dump_bc_entities(bc):
    entities = S.EntityList()
    bc.entities(entities)
    output = []
    for ent in entities:
        output.append(
            {
                "id": int(ent.id()),
                "name": str(ent.name()),
                "tag_name": str(ent.tagName()),
            }
        )
    return output


def bc_matches(bc, match):
    if not match:
        return True
    type_expected = str(match.get("type", "")).strip().lower()
    if type_expected and str(bc.type).strip().lower() != type_expected:
        return False

    name_expected = str(match.get("name", "")).strip().lower()
    if name_expected and str(bc.name()).strip().lower() != name_expected:
        return False

    entities = dump_bc_entities(bc)

    wanted_ids = {int(v) for v in match.get("entity_ids", [])}
    if wanted_ids:
        bc_ids = {ent["id"] for ent in entities}
        if not wanted_ids.intersection(bc_ids):
            return False

    wanted_names = {str(v).strip().lower() for v in match.get("entity_names", [])}
    if wanted_names:
        bc_names = {str(ent["name"]).strip().lower() for ent in entities}
        if not wanted_names.intersection(bc_names):
            return False

    wanted_tags = {str(v).strip().lower() for v in match.get("entity_tags", [])}
    if wanted_tags:
        bc_tags = {str(ent["tag_name"]).strip().lower() for ent in entities}
        if not wanted_tags.intersection(bc_tags):
            return False

    return True


def material_matches(material, match):
    if not match:
        return True
    name_expected = str(match.get("name", "")).strip().lower()
    if name_expected and str(material.name).strip().lower() != name_expected:
        return False
    type_expected = str(match.get("type", "")).strip().lower()
    if type_expected and str(material.type).strip().lower() != type_expected:
        return False
    return True


def part_matches(part, match):
    if not match:
        return True
    name_expected = str(match.get("name", "")).strip().lower()
    if name_expected and str(part.name()).strip().lower() != name_expected:
        return False
    part_id = match.get("id")
    if part_id is not None:
        try:
            if int(part.id()) != int(part_id):
                return False
        except Exception:
            return False
    return True


def find_targets(scenario, target_type, match):
    target_type = str(target_type or "scenario").strip().lower()
    if target_type in {"scenario", "scenario_setting", "scenario_settings"}:
        return [scenario]
    if target_type in {"boundary_condition", "boundary_conditions", "boundary", "bc"}:
        bcs = S.BCList()
        scenario.bcs(bcs)
        return [bc for bc in bcs if bc_matches(bc, match)]
    if target_type in {"material", "materials"}:
        mats = S.MaterialList()
        scenario.materials(mats)
        return [mat for mat in mats if material_matches(mat, match)]
    if target_type in {"part", "parts"}:
        parts = S.PartList()
        scenario.parts(parts)
        return [part for part in parts if part_matches(part, match)]
    return []


def set_object_property(target, property_name, raw_value, units=None):
    if units and hasattr(target, "units"):
        try:
            setattr(target, "units", str(units))
        except Exception:
            pass

    if hasattr(target, property_name):
        current_value = getattr(target, property_name)
        value = coerce_value(current_value, raw_value)
        setattr(target, property_name, value)
        return

    if property_name == "value" and hasattr(target, "value"):
        current_value = getattr(target, "value")
        value = coerce_value(current_value, raw_value)
        setattr(target, "value", value)
        return

    if property_name == "units" and hasattr(target, "units"):
        setattr(target, "units", str(raw_value))
        return

    if hasattr(target, "setProperty"):
        parsed = parse_scalar(raw_value)
        if parsed is None:
            parsed = ""
        target.setProperty(property_name, parsed)
        return

    raise RuntimeError("Target object does not support property updates.")


def _normalize_mapping(mapping):
    if not isinstance(mapping, dict):
        return {}

    source_column = str(mapping.get("source_column", mapping.get("param", ""))).strip()
    target_type = str(mapping.get("target_type", "scenario")).strip() or "scenario"
    property_name = str(mapping.get("property", "value")).strip() or "value"
    units = mapping.get("units")

    match = mapping.get("match", {})
    if not isinstance(match, dict):
        match = {}
    else:
        match = dict(match)

    target_name = str(mapping.get("target_name", "")).strip()
    if target_name and not str(match.get("name", "")).strip():
        match["name"] = target_name

    target_id = mapping.get("target_id")
    if target_id not in ("", None) and match.get("id") in ("", None):
        match["id"] = target_id

    property_aliases = mapping.get("property_aliases", [])
    if not isinstance(property_aliases, list):
        property_aliases = []
    values_map = mapping.get("values", {})
    if not isinstance(values_map, dict):
        values_map = {}

    return {
        "source_column": source_column,
        "target_type": target_type,
        "property": property_name,
        "property_aliases": property_aliases,
        "values": values_map,
        "units": units,
        "match": match,
    }


def _set_object_property_with_aliases(target, property_names, raw_value, units=None):
    last_error = None
    seen = set()
    for name in property_names:
        prop_name = str(name).strip()
        if not prop_name or prop_name in seen:
            continue
        seen.add(prop_name)
        try:
            set_object_property(target, prop_name, raw_value, units=units)
            return prop_name
        except Exception as ex:
            last_error = ex
    if last_error is not None:
        raise last_error
    raise RuntimeError("No property names were provided.")


def _normalize_lookup_token(value):
    parsed = parse_scalar(value)
    if isinstance(parsed, bool):
        return str(parsed).lower()
    if isinstance(parsed, (int, float)):
        return str(parsed)
    return str(parsed).strip().lower()


def _resolve_mapping_value(raw_value, values_map):
    if raw_value in (None, ""):
        return True, raw_value, ""
    if not isinstance(values_map, dict) or not values_map:
        return True, raw_value, ""

    raw_token = _normalize_lookup_token(raw_value)
    for key, mapped in values_map.items():
        if _normalize_lookup_token(key) == raw_token:
            return True, mapped, str(key)

    known = ", ".join(str(key) for key in values_map.keys())
    return False, raw_value, known


def _normalize_fluid_preset_properties(raw_properties):
    normalized = []
    if isinstance(raw_properties, dict):
        iterator = raw_properties.items()
    elif isinstance(raw_properties, list):
        iterator = []
        for item in raw_properties:
            if not isinstance(item, dict):
                continue
            iterator.append((item.get("property", ""), item))
    else:
        return normalized

    for key, value in iterator:
        property_name = str(key).strip()
        if not property_name:
            continue

        if isinstance(value, dict):
            raw_value = value.get("value")
            units = value.get("units")
            aliases = value.get("aliases", [])
        else:
            raw_value = value
            units = None
            aliases = []

        if raw_value in ("", None):
            continue
        if not isinstance(aliases, list):
            aliases = []

        normalized.append(
            {
                "property": property_name,
                "value": raw_value,
                "units": units,
                "aliases": [str(alias).strip() for alias in aliases if str(alias).strip()],
            }
        )
    return normalized


def _resolve_fluid_preset_definition(config, preset_name):
    config_presets = config.get("fluid_presets", {})
    if isinstance(config_presets, dict):
        raw_preset = config_presets.get(preset_name)
        if raw_preset is not None and isinstance(raw_preset, dict):
            match = raw_preset.get("match", raw_preset.get("material_match", {}))
            if not isinstance(match, dict):
                match = {}
            properties = _normalize_fluid_preset_properties(raw_preset.get("properties", {}))
            if properties:
                if not match:
                    match = {"type": "fluid"}
                return {"match": match, "properties": properties}

    built_in = BUILTIN_FLUID_PRESETS.get(preset_name)
    if not built_in:
        return None
    return {
        "match": dict(built_in.get("match", {"type": "fluid"})),
        "properties": _normalize_fluid_preset_properties(built_in.get("properties", {})),
    }


def _selected_fluid_preset(case_row, config):
    case_value = case_row.get("fluid_preset")
    if case_value in (None, ""):
        study_cfg = config.get("study", {}) if isinstance(config.get("study", {}), dict) else {}
        case_value = study_cfg.get("fluid_preset", "")
    if case_value in (None, ""):
        case_value = config.get("fluid_preset", "")
    preset_name = str(case_value).strip().lower()
    return preset_name


def apply_fluid_preset(scenario, case_row, config, messages, warnings):
    preset_name = _selected_fluid_preset(case_row, config)
    if not preset_name:
        return

    preset = _resolve_fluid_preset_definition(config, preset_name)
    if not preset:
        warnings.append(
            "Unknown fluid_preset "
            f"'{preset_name}'. Supported built-ins: {', '.join(sorted(BUILTIN_FLUID_PRESETS.keys()))}."
        )
        return

    match = preset.get("match", {})
    targets = find_targets(scenario, "material", match)
    if not targets:
        warnings.append(
            f"fluid_preset '{preset_name}' found no matching material targets "
            f"(match={match})."
        )
        return

    property_updates = preset.get("properties", [])
    if not property_updates:
        warnings.append(f"fluid_preset '{preset_name}' has no properties to apply.")
        return

    applied_count = 0
    for target in targets:
        for item in property_updates:
            property_name = str(item.get("property", "")).strip()
            if not property_name:
                continue
            aliases = []
            aliases.extend(FLUID_PROPERTY_ALIASES.get(property_name, []))
            aliases.extend(item.get("aliases", []))
            property_names = [property_name] + aliases
            try:
                used_property = _set_object_property_with_aliases(
                    target,
                    property_names,
                    item.get("value"),
                    units=item.get("units"),
                )
                applied_count += 1
                messages.append(
                    f"fluid_preset '{preset_name}' applied material property "
                    f"{used_property}={item.get('value')}."
                )
            except Exception as ex:
                warnings.append(
                    f"fluid_preset '{preset_name}' failed to set material property "
                    f"'{property_name}': {ex}"
                )

    if applied_count:
        messages.append(
            f"Applied fluid_preset '{preset_name}' to {len(targets)} material target(s) "
            f"with {applied_count} property update(s)."
        )


def apply_parameter_mappings(scenario, case_row, mappings, messages, warnings):
    if not isinstance(mappings, list):
        return
    for raw_mapping in mappings:
        mapping = _normalize_mapping(raw_mapping)
        source_column = mapping.get("source_column")
        if not source_column:
            continue
        raw_value = case_row.get(source_column)
        if raw_value in (None, ""):
            continue

        target_type = mapping.get("target_type", "scenario")
        match = mapping.get("match", {})
        property_name = mapping.get("property", "value")
        property_aliases = mapping.get("property_aliases", [])
        values_map = mapping.get("values", {})
        units = mapping.get("units")
        resolved_ok, resolved_value, matched_label = _resolve_mapping_value(raw_value, values_map)
        if not resolved_ok:
            warnings.append(
                f"Mapping '{source_column}' value '{raw_value}' is not in values lookup. "
                f"Known keys: {matched_label}"
            )
            continue

        targets = find_targets(scenario, target_type, match)
        if not targets:
            warnings.append(
                f"No targets found for mapping '{source_column}' "
                f"(target_type={target_type}, match={match})."
            )
            continue

        for target in targets:
            try:
                _set_object_property_with_aliases(
                    target,
                    [property_name] + list(property_aliases),
                    resolved_value,
                    units=units,
                )
            except Exception as ex:
                warnings.append(
                    f"Failed to set {property_name} for {target_type} "
                    f"from '{source_column}': {ex}"
                )
        if matched_label:
            messages.append(
                f"Mapping '{source_column}' value '{raw_value}' resolved via values lookup key "
                f"'{matched_label}' -> '{resolved_value}'."
            )
        messages.append(
            f"Applied mapping '{source_column}' -> {target_type}.{property_name} "
            f"for {len(targets)} target(s)."
        )


def apply_solver_overrides(scenario, overrides, messages, warnings):
    if not isinstance(overrides, dict):
        return
    for key, value in overrides.items():
        try:
            set_object_property(scenario, key, value)
            messages.append(f"Solver override applied: {key}={value}")
        except Exception as ex:
            warnings.append(f"Could not apply solver override {key}: {ex}")


def dump_properties(obj):
    prop_defs = S.PropertyDefinitionList()
    values = S.VariantList()
    output = []
    try:
        obj.properties(prop_defs, values)
    except Exception:
        return output
    count = min(len(prop_defs), len(values))
    for idx in range(count):
        definition = prop_defs[idx]
        variant = values[idx]
        _, value = variant_to_python(variant)
        output.append(
            {
                "name": str(definition.name()),
                "value": value,
            }
        )
    return output


def find_numeric_metric(prop_rows, tokens):
    for row in prop_rows:
        name = str(row.get("name", "")).strip().lower()
        if not all(token in name for token in tokens):
            continue
        value = to_float_or_none(row.get("value"))
        if value is not None:
            return value
    return None


def collect_mesh_quality_metrics(scenario, case_row, messages):
    metrics = {
        "skewness": to_float_or_none(case_row.get("mesh_skewness")),
        "aspect_ratio": to_float_or_none(case_row.get("mesh_aspect_ratio")),
        "orthogonality": to_float_or_none(case_row.get("mesh_orthogonality")),
        "element_count": to_int_or_none(case_row.get("mesh_element_count")),
    }

    if all(value is not None for value in metrics.values()):
        messages.append("Mesh quality metrics loaded from case row overrides.")
        return metrics

    props = dump_properties(scenario)
    if props:
        if metrics["skewness"] is None:
            metrics["skewness"] = find_numeric_metric(props, ["skew"])
        if metrics["aspect_ratio"] is None:
            metrics["aspect_ratio"] = find_numeric_metric(props, ["aspect", "ratio"])
        if metrics["orthogonality"] is None:
            metrics["orthogonality"] = find_numeric_metric(props, ["orthogon"])
        if metrics["element_count"] is None:
            element_count = find_numeric_metric(props, ["element", "count"])
            metrics["element_count"] = int(element_count) if element_count is not None else None

    return metrics


def evaluate_mesh_quality(metrics, gate_cfg):
    gate = gate_cfg if isinstance(gate_cfg, dict) else {}
    if not bool(gate.get("enabled", True)):
        return {
            "enabled": False,
            "passed": True,
            "failed_checks": [],
            "missing_metrics": [],
            "metrics": metrics,
        }

    checks = [
        ("skewness", "<=", to_float_or_none(gate.get("skewness_max"))),
        ("aspect_ratio", "<=", to_float_or_none(gate.get("aspect_ratio_max"))),
        ("orthogonality", ">=", to_float_or_none(gate.get("orthogonality_min"))),
        ("element_count", ">=", to_float_or_none(gate.get("element_count_min"))),
        ("element_count", "<=", to_float_or_none(gate.get("element_count_max"))),
    ]
    require_all = bool(gate.get("require_all_metrics", False))
    failed_checks = []
    missing = []
    for metric_name, operator, threshold in checks:
        if threshold is None:
            continue
        value = metrics.get(metric_name)
        if value is None:
            missing.append(metric_name)
            continue
        if operator == "<=" and not (float(value) <= float(threshold)):
            failed_checks.append(f"{metric_name}={value} exceeds threshold={threshold}")
        if operator == ">=" and not (float(value) >= float(threshold)):
            failed_checks.append(f"{metric_name}={value} below threshold={threshold}")

    if require_all and missing:
        for metric_name in sorted(set(missing)):
            failed_checks.append(f"{metric_name} missing while require_all_metrics=true")

    return {
        "enabled": True,
        "passed": len(failed_checks) == 0,
        "failed_checks": failed_checks,
        "missing_metrics": sorted(set(missing)),
        "metrics": metrics,
    }


def resolve_mesh_params(config, case_row, mesh_adjustment):
    mesh_cfg = config.get("mesh", {})
    defaults = mesh_cfg.get("default_params", {}) if isinstance(mesh_cfg, dict) else {}
    params = {
        "max_element_size_m": to_float_or_none(case_row.get("mesh_max_element_size_m")),
        "min_element_size_m": to_float_or_none(case_row.get("mesh_min_element_size_m")),
        "inflation_layers": to_int_or_none(case_row.get("mesh_inflation_layers")),
        "target_y_plus": to_float_or_none(case_row.get("mesh_target_y_plus")),
        "refinement_zones": case_row.get("mesh_refinement_zones", ""),
    }

    if params["max_element_size_m"] is None:
        params["max_element_size_m"] = to_float_or_none(defaults.get("max_element_size_m"))
    if params["min_element_size_m"] is None:
        params["min_element_size_m"] = to_float_or_none(defaults.get("min_element_size_m"))
    if params["inflation_layers"] is None:
        params["inflation_layers"] = to_int_or_none(defaults.get("inflation_layers"))
    if params["target_y_plus"] is None:
        params["target_y_plus"] = to_float_or_none(defaults.get("target_y_plus"))

    if params["refinement_zones"] in ("", None):
        params["refinement_zones"] = defaults.get("refinement_zones", [])

    adjustment = mesh_adjustment if isinstance(mesh_adjustment, dict) else {}
    size_scale = to_float_or_none(adjustment.get("size_scale"))
    inflation_delta = to_int_or_none(adjustment.get("inflation_layer_delta"))
    if size_scale is not None:
        if params["max_element_size_m"] is not None:
            params["max_element_size_m"] = float(params["max_element_size_m"]) * size_scale
        if params["min_element_size_m"] is not None:
            params["min_element_size_m"] = float(params["min_element_size_m"]) * size_scale
    if inflation_delta is not None and params["inflation_layers"] is not None:
        params["inflation_layers"] = max(1, int(params["inflation_layers"]) + inflation_delta)

    return params


def try_set_aliases(target, aliases, value):
    for alias in aliases:
        try:
            set_object_property(target, alias, value)
            return alias
        except Exception:
            continue
    return ""


def apply_mesh_overrides(scenario, mesh_params, messages, warnings):
    if not isinstance(mesh_params, dict):
        return
    alias_map = {
        "max_element_size_m": ["maxElementSize", "meshMaxElementSize", "globalElementSize"],
        "min_element_size_m": ["minElementSize", "meshMinElementSize"],
        "inflation_layers": ["inflationLayers", "boundaryLayerCount", "nInflationLayers"],
        "target_y_plus": ["targetYPlus", "yPlus", "wallYPlus"],
    }
    for key, aliases in alias_map.items():
        value = mesh_params.get(key)
        if value in ("", None):
            continue
        used = try_set_aliases(scenario, aliases, value)
        if used:
            messages.append(f"Applied mesh parameter {key}={value} via property '{used}'.")
        else:
            warnings.append(
                f"Could not apply mesh parameter '{key}' using aliases {aliases}. "
                "Scenario API may expose different property names."
            )


def parse_summary_value(raw_value, requested_unit):
    unit = requested_unit or ""
    numeric = None
    raw_repr = raw_value

    if isinstance(raw_value, (list, tuple)):
        if len(raw_value) > 0:
            try:
                numeric = float(raw_value[0])
            except Exception:
                numeric = None
        if len(raw_value) > 1 and str(raw_value[1]).strip():
            unit = str(raw_value[1])
        return numeric, unit, str(raw_repr)

    if isinstance(raw_value, (int, float)):
        return float(raw_value), unit, str(raw_repr)

    text = str(raw_value)
    try:
        decoded = ast.literal_eval(text)
        if isinstance(decoded, (list, tuple)):
            if len(decoded) > 0:
                try:
                    numeric = float(decoded[0])
                except Exception:
                    numeric = None
            if len(decoded) > 1 and str(decoded[1]).strip():
                unit = str(decoded[1])
            return numeric, unit, text
        if isinstance(decoded, (int, float)):
            return float(decoded), unit, text
    except Exception:
        pass

    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", text)
    if match:
        try:
            numeric = float(match.group(0))
        except Exception:
            numeric = None
    return numeric, unit, text


def extract_summary_and_metrics(scenario, metrics_cfg, summary_csv_path):
    summary = R.Summary(scenario)
    summary.load()
    sections = summary.sections()

    metric_lookup = {}
    for metric in metrics_cfg:
        alias = metric.get("alias")
        section = str(metric.get("section", "")).strip().lower()
        quantity = str(metric.get("quantity", "")).strip().lower()
        metric_lookup[(section, quantity)] = metric
        if alias is not None:
            metric.setdefault("alias", alias)

    rows = []
    metric_values = {}
    for metric in metrics_cfg:
        alias = metric.get("alias")
        if alias:
            metric_values[alias] = None

    for section in sections:
        quantities = summary.quantities(section)
        section_text = str(section)
        for quantity in quantities:
            quantity_text = str(quantity)
            key = (section_text.strip().lower(), quantity_text.strip().lower())
            metric = metric_lookup.get(key)
            requested_unit = metric.get("unit", "") if metric else ""
            if not requested_unit:
                requested_unit = summary.unit(section, quantity)
            raw_value = summary.value(section, quantity, requested_unit)
            numeric, resolved_unit, raw_repr = parse_summary_value(raw_value, requested_unit)
            rows.append(
                {
                    "section": section_text,
                    "quantity": quantity_text,
                    "value_numeric": numeric if numeric is not None else "",
                    "value_raw": raw_repr,
                    "unit": resolved_unit,
                }
            )
            if metric:
                alias = metric.get("alias")
                if alias:
                    metric_values[alias] = numeric

    if summary_csv_path:
        with open(summary_csv_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=["section", "quantity", "value_numeric", "value_raw", "unit"],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    return rows, metric_values


def write_metrics_csv(metrics_csv_path, case_id, metrics):
    columns = ["case_id"] + list(metrics.keys())
    with open(metrics_csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        row = {"case_id": case_id}
        row.update(metrics)
        writer.writerow(row)


def export_screenshots(scenario, views, screenshots_dir, messages, warnings):
    screenshots = []
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    view_list = views if views else ["default"]
    try:
        results = scenario.results()
        results.activate()
        for view in view_list:
            safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(view)).strip("_") or "default"
            image_path = screenshots_dir / f"{safe_name}.png"
            rc = results.saveImage(normalize_path(image_path))
            messages.append(f"Saved screenshot {image_path.name} (rc={rc})")
            if image_path.exists():
                screenshots.append(str(image_path))
    except Exception as ex:
        warnings.append(f"Screenshot export failed via results API: {ex}")

    if not screenshots:
        thumb = Path(str(scenario.path)) / "thumbnail.jpg"
        if thumb.exists():
            fallback = screenshots_dir / "thumbnail.jpg"
            shutil.copy2(thumb, fallback)
            messages.append("Used scenario thumbnail.jpg as screenshot fallback.")
            screenshots.append(str(fallback))
    return screenshots


def export_cutplanes(scenario, cutplane_names, cutplane_dir, messages, warnings):
    cutplane_dir.mkdir(parents=True, exist_ok=True)
    outputs = []
    if not cutplane_names:
        return outputs
    try:
        results = scenario.results()
        results.activate()
        for cutplane_name in cutplane_names:
            cutplane = results.cutPlane(cutplane_name)
            csv_path = cutplane_dir / f"{re.sub(r'[^A-Za-z0-9._-]+', '_', cutplane_name)}.csv"
            cutplane.saveTable(normalize_path(csv_path))
            if csv_path.exists():
                outputs.append(str(csv_path))
                messages.append(f"Saved cutplane table: {csv_path.name}")
            else:
                warnings.append(f"Cutplane '{cutplane_name}' did not generate a table file.")
    except Exception as ex:
        warnings.append(f"Cutplane export failed: {ex}")
    return outputs


def select_design_and_scenario(study, design_name, scenario_name):
    designs = S.DesignList()
    study.designs(designs)
    if len(designs) == 0:
        raise RuntimeError("No designs found in study.")

    design = None
    if design_name:
        for candidate in designs:
            if str(candidate.name).strip().lower() == str(design_name).strip().lower():
                design = candidate
                break
    if design is None:
        design = designs[0]

    scenarios = S.ScenarioList()
    design.scenarios(scenarios)
    if len(scenarios) == 0:
        raise RuntimeError("No scenarios found in design.")

    scenario = None
    if scenario_name:
        for candidate in scenarios:
            if str(candidate.name).strip().lower() == str(scenario_name).strip().lower():
                scenario = candidate
                break
    if scenario is None:
        scenario = scenarios[0]

    return design, scenario


def main():
    payload_path = os.environ.get("CFD_AUTOMATION_PAYLOAD", "").strip()
    if not payload_path:
        raise RuntimeError("CFD_AUTOMATION_PAYLOAD is not set.")
    payload = json.loads(Path(payload_path).read_text(encoding="utf-8"))

    case = payload.get("case", {})
    config = payload.get("config", {})
    mesh_adjustment = payload.get("mesh_adjustment", {})
    case_dir = Path(payload.get("case_dir", ".")).resolve()
    case_dir.mkdir(parents=True, exist_ok=True)

    case_id = str(case.get("case_id", "CASE_UNKNOWN"))
    result_path = case_dir / "case_result.json"
    summary_csv = case_dir / "summary_all.csv"
    metrics_csv = case_dir / "metrics.csv"
    screenshots_dir = case_dir / "screenshots"
    cutplane_dir = case_dir / "cutplanes"

    result = {
        "case_id": case_id,
        "success": False,
        "messages": [],
        "warnings": [],
        "metrics": {},
        "screenshots": [],
        "cutplanes": [],
        "summary_csv": str(summary_csv),
        "metrics_csv": str(metrics_csv),
        "mesh_adjustment": mesh_adjustment if isinstance(mesh_adjustment, dict) else {},
        "mesh_params_used": {},
        "mesh_quality": {},
        "failure_type": "",
    }

    try:
        study_cfg = config.get("study", {})
        template_model = study_cfg.get("template_model")
        if not template_model:
            raise RuntimeError("study.template_model is required.")

        copied_model = copy_study(template_model, case_dir)
        result["messages"].append(f"Copied study to {copied_model.parent}")

        study = S.DesignStudy.Create()
        open_rc = study.open(normalize_path(copied_model))
        result["messages"].append(f"Study open rc={open_rc}")

        design, scenario = select_design_and_scenario(
            study,
            study_cfg.get("design_name", ""),
            study_cfg.get("scenario_name", ""),
        )
        scenario.activate()
        result["messages"].append(
            f"Active design/scenario: {design.name} / {scenario.name}"
        )

        solve_cfg = config.get("solve", {})
        apply_solver_overrides(
            scenario,
            solve_cfg.get("scenario_overrides", {}),
            result["messages"],
            result["warnings"],
        )
        apply_fluid_preset(
            scenario,
            case,
            config,
            result["messages"],
            result["warnings"],
        )
        apply_parameter_mappings(
            scenario,
            case,
            config.get("parameter_mappings", []),
            result["messages"],
            result["warnings"],
        )

        mesh_params = resolve_mesh_params(config, case, mesh_adjustment)
        result["mesh_params_used"] = mesh_params
        apply_mesh_overrides(scenario, mesh_params, result["messages"], result["warnings"])

        mesh_quality = evaluate_mesh_quality(
            collect_mesh_quality_metrics(scenario, case, result["messages"]),
            config.get("mesh", {}).get("quality_gate", {}),
        )
        result["mesh_quality"] = mesh_quality
        if mesh_quality.get("enabled"):
            result["messages"].append(
                "Mesh gate evaluated: "
                + ("PASS" if mesh_quality.get("passed") else "FAIL")
                + f", failed_checks={len(mesh_quality.get('failed_checks', []))}, "
                + f"missing_metrics={len(mesh_quality.get('missing_metrics', []))}"
            )
        if not mesh_quality.get("passed", True):
            result["failure_type"] = "bad_mesh"
            raise RuntimeError("Mesh quality gate failed before solve.")

        try:
            study.save()
            result["messages"].append("Study changes saved.")
        except Exception as ex:
            result["warnings"].append(f"Study save failed: {ex}")

        solve_enabled = bool(solve_cfg.get("enabled", False))
        if solve_enabled:
            skip_if_has_results = bool(solve_cfg.get("skip_if_results_exist", True))
            force_solve = bool(parse_scalar(case.get("force_solve")))
            if skip_if_has_results and bool(scenario.hasResults) and not force_solve:
                result["messages"].append("Skipped solve because results already exist.")
            else:
                run_rc = scenario.run()
                result["messages"].append(f"scenario.run() returned: {run_rc}")
        else:
            result["messages"].append("Solve disabled in config.")

        if not bool(scenario.hasResults):
            result["failure_type"] = "no_results"
            raise RuntimeError("Scenario has no results to post-process.")

        _, metrics = extract_summary_and_metrics(
            scenario,
            config.get("metrics", []),
            summary_csv if config.get("outputs", {}).get("save_all_summary", True) else None,
        )
        result["metrics"] = metrics
        write_metrics_csv(metrics_csv, case_id, metrics)
        result["messages"].append("Summary CSV and metrics CSV exported.")

        screenshots_cfg = config.get("outputs", {}).get("screenshots", {})
        if screenshots_cfg.get("enabled", True):
            result["screenshots"] = export_screenshots(
                scenario,
                screenshots_cfg.get("views", ["default"]),
                screenshots_dir,
                result["messages"],
                result["warnings"],
            )

        cutplane_names = config.get("outputs", {}).get("cutplanes", [])
        if cutplane_names:
            result["cutplanes"] = export_cutplanes(
                scenario,
                cutplane_names,
                cutplane_dir,
                result["messages"],
                result["warnings"],
            )

        result["success"] = True
    except Exception as ex:
        if not result.get("failure_type"):
            result["failure_type"] = "python_exception"
        result["error"] = str(ex)
        result["traceback"] = traceback.format_exc()

    with open(result_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2)


if __name__ == "__main__":
    main()
