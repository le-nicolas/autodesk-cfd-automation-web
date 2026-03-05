import json
import os
import traceback
from pathlib import Path

import CFD.Setup as S


def variant_to_python(variant):
    try:
        type_name = str(variant.typeName())
    except Exception:
        type_name = ""
    try:
        if type_name in {"int", "uint", "qlonglong", "qulonglong"}:
            value = variant.toInt()
        elif type_name in {"double", "float"}:
            value = variant.toDouble()
        elif type_name == "bool":
            value = variant.toBool()
        else:
            value = variant.toString()
    except Exception:
        try:
            value = str(variant)
        except Exception:
            value = ""
    return type_name, value


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
        type_name, value = variant_to_python(variant)
        output.append(
            {
                "name": str(definition.name()),
                "definition_type": str(definition.type()),
                "variant_type": type_name,
                "value": value,
            }
        )
    return output


def dump_bc_entities(bc):
    entities = S.EntityList()
    bc.entities(entities)
    output = []
    for ent in entities:
        output.append(
            {
                "id": int(ent.id()),
                "part_id": int(ent.partID()),
                "name": str(ent.name()),
                "tag_name": str(ent.tagName()),
                "type": str(ent.type),
            }
        )
    return output


def dump_material_properties(material):
    props = S.PropertyList()
    output = []
    try:
        material.properties(props)
    except Exception:
        return output
    for prop in props:
        item = {"type": str(prop.type)}
        try:
            item["value"] = prop.value()
        except Exception:
            item["value"] = ""
        try:
            item["units"] = prop.units()
        except Exception:
            item["units"] = ""
        output.append(item)
    return output


def main():
    study_path = os.environ.get("CFD_AUTOMATION_STUDY", "").strip()
    output_path = os.environ.get("CFD_AUTOMATION_OUTPUT", "").strip()
    design_name = os.environ.get("CFD_AUTOMATION_DESIGN", "").strip()
    scenario_name = os.environ.get("CFD_AUTOMATION_SCENARIO", "").strip()

    payload = {
        "ok": False,
        "study_path": study_path,
        "errors": [],
    }

    try:
        if not study_path:
            raise RuntimeError("CFD_AUTOMATION_STUDY is not set.")
        if not output_path:
            raise RuntimeError("CFD_AUTOMATION_OUTPUT is not set.")

        study = S.DesignStudy.Create()
        open_rc = study.open(study_path)
        payload["open_rc"] = str(open_rc)
        payload["study_name"] = str(study.name)
        payload["study_dir"] = str(study.path)

        designs = S.DesignList()
        study.designs(designs)
        design_rows = []
        selected_design = None
        for design in designs:
            row = {"name": str(design.name), "scenarios": []}
            scenarios = S.ScenarioList()
            design.scenarios(scenarios)
            for scenario in scenarios:
                row["scenarios"].append(
                    {
                        "name": str(scenario.name),
                        "has_results": bool(scenario.hasResults),
                        "path": str(scenario.path),
                    }
                )
            design_rows.append(row)
            if design_name and str(design.name).lower() == design_name.lower():
                selected_design = design

        if selected_design is None and len(designs) > 0:
            selected_design = designs[0]

        selected_scenario = None
        if selected_design is not None:
            scenarios = S.ScenarioList()
            selected_design.scenarios(scenarios)
            for scenario in scenarios:
                if scenario_name and str(scenario.name).lower() == scenario_name.lower():
                    selected_scenario = scenario
                    break
            if selected_scenario is None and len(scenarios) > 0:
                selected_scenario = scenarios[0]

        payload["designs"] = design_rows

        if selected_scenario is not None:
            selected_scenario.activate()
            scenario_data = {
                "design": str(selected_scenario.design().name),
                "scenario": str(selected_scenario.name),
                "properties": dump_properties(selected_scenario),
            }

            bcs = S.BCList()
            selected_scenario.bcs(bcs)
            scenario_data["boundary_conditions"] = []
            for bc in bcs:
                scenario_data["boundary_conditions"].append(
                    {
                        "name": str(bc.name()),
                        "type": str(bc.type),
                        "value": str(bc.value),
                        "units": str(bc.units),
                        "info": str(bc.infoString()),
                        "properties": dump_properties(bc),
                        "entities": dump_bc_entities(bc),
                    }
                )

            materials = S.MaterialList()
            selected_scenario.materials(materials)
            scenario_data["materials"] = []
            for material in materials:
                scenario_data["materials"].append(
                    {
                        "name": str(material.name),
                        "type": str(material.type),
                        "properties": dump_material_properties(material),
                    }
                )

            parts = S.PartList()
            selected_scenario.parts(parts)
            scenario_data["parts"] = []
            for part in parts:
                scenario_data["parts"].append(
                    {
                        "name": str(part.name()),
                        "id": int(part.id()),
                        "properties": dump_properties(part),
                    }
                )

            payload["selected"] = scenario_data

        payload["ok"] = True
    except Exception as ex:
        payload["errors"].append(str(ex))
        payload["traceback"] = traceback.format_exc()

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)


if __name__ == "__main__":
    main()
