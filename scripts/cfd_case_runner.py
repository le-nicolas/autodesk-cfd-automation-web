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
    if target_type == "scenario":
        return [scenario]
    if target_type == "boundary_condition":
        bcs = S.BCList()
        scenario.bcs(bcs)
        return [bc for bc in bcs if bc_matches(bc, match)]
    if target_type == "material":
        mats = S.MaterialList()
        scenario.materials(mats)
        return [mat for mat in mats if material_matches(mat, match)]
    if target_type == "part":
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


def apply_parameter_mappings(scenario, case_row, mappings, messages, warnings):
    for mapping in mappings:
        source_column = mapping.get("source_column")
        if not source_column:
            continue
        raw_value = case_row.get(source_column)
        if raw_value in (None, ""):
            continue

        target_type = mapping.get("target_type", "scenario")
        match = mapping.get("match", {})
        property_name = mapping.get("property", "value")
        units = mapping.get("units")

        targets = find_targets(scenario, target_type, match)
        if not targets:
            warnings.append(
                f"No targets found for mapping '{source_column}' "
                f"(target_type={target_type}, match={match})."
            )
            continue

        for target in targets:
            try:
                set_object_property(target, property_name, raw_value, units=units)
            except Exception as ex:
                warnings.append(
                    f"Failed to set {property_name} for {target_type} "
                    f"from '{source_column}': {ex}"
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
        apply_parameter_mappings(
            scenario,
            case,
            config.get("parameter_mappings", []),
            result["messages"],
            result["warnings"],
        )

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
        result["error"] = str(ex)
        result["traceback"] = traceback.format_exc()

    with open(result_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2)


if __name__ == "__main__":
    main()
