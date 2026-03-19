import copy
from collections import OrderedDict
from typing import Any


def _condition_key(condition: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(condition.items()))


def _extract_const_condition(if_schema: Any) -> dict[str, Any] | None:
    if not isinstance(if_schema, dict):
        return None
    properties = if_schema.get("properties")
    if not isinstance(properties, dict) or not properties:
        return None

    condition: dict[str, Any] = {}
    for field_name, field_schema in properties.items():
        if not isinstance(field_schema, dict) or "const" not in field_schema:
            return None
        condition[field_name] = field_schema["const"]
    return condition


def _extract_required(schema: Any) -> set[str]:
    if not isinstance(schema, dict):
        return set()
    required = schema.get("required")
    if not isinstance(required, list):
        return set()
    return {item for item in required if isinstance(item, str)}


def process_schema(schema: dict[str, Any]) -> dict[str, Any]:
    processed = copy.deepcopy(schema)
    properties = processed.get("properties")
    if not isinstance(properties, dict):
        return processed

    required = processed.get("required")
    if not isinstance(required, list):
        required = []
        processed["required"] = required

    grouped_fields: OrderedDict[tuple[tuple[str, Any], ...], dict[str, Any]] = OrderedDict()
    grouped_conditions: dict[tuple[tuple[str, Any], ...], dict[str, Any]] = {}

    for field_name in list(properties.keys()):
        field_schema = properties[field_name]
        if not isinstance(field_schema, dict):
            continue
        visible_when = field_schema.pop("visible_when", None)
        if not isinstance(visible_when, dict) or not visible_when:
            continue

        key = _condition_key(visible_when)
        grouped_fields.setdefault(key, {})[field_name] = field_schema
        grouped_conditions[key] = visible_when
        properties.pop(field_name, None)
        required[:] = [item for item in required if item != field_name]

    if not grouped_fields:
        if not required:
            processed.pop("required", None)
        return processed

    conditional_required: dict[tuple[tuple[str, Any], ...], set[str]] = {}
    root_if = processed.get("if")
    root_then = processed.get("then")
    root_else = processed.get("else")
    root_condition = _extract_const_condition(root_if)

    if root_condition:
        conditional_required[_condition_key(root_condition)] = _extract_required(root_then)
        if len(root_condition) == 1:
            ((field_name, value),) = root_condition.items()
            if isinstance(value, bool):
                else_condition = {field_name: not value}
                conditional_required[_condition_key(else_condition)] = _extract_required(root_else)

    all_of = processed.get("allOf")
    all_of_entries: list[dict[str, Any]] = list(all_of) if isinstance(all_of, list) else []

    for key, branch_properties in grouped_fields.items():
        condition = grouped_conditions[key]
        then_schema: dict[str, Any] = {"properties": branch_properties}
        branch_required = conditional_required.get(key, set())
        required_in_branch = [name for name in branch_properties if name in branch_required]
        if required_in_branch:
            then_schema["required"] = required_in_branch

        if_schema = {
            "properties": {name: {"const": value} for name, value in condition.items()},
            "required": list(condition.keys()),
        }
        all_of_entries.append({"if": if_schema, "then": then_schema})

    processed["allOf"] = all_of_entries
    processed.pop("if", None)
    processed.pop("then", None)
    processed.pop("else", None)
    if not required:
        processed.pop("required", None)
    return processed
