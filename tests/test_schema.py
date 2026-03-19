from typing import Any

from server.schema import process_schema


def test_process_schema_moves_visible_when_fields_to_all_of() -> None:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": {
            "has_bibcode": {"type": "boolean", "default": True},
            "table_name": {"type": "string"},
            "bibcode": {"type": "string", "visible_when": {"has_bibcode": True}},
            "pub_name": {"type": "string", "visible_when": {"has_bibcode": False}},
            "pub_authors": {
                "type": "array",
                "items": {"type": "string"},
                "visible_when": {"has_bibcode": False},
            },
            "pub_year": {"type": "integer", "visible_when": {"has_bibcode": False}},
        },
        "required": ["table_name"],
        "if": {"properties": {"has_bibcode": {"const": True}}},
        "then": {"required": ["bibcode"]},
        "else": {"required": ["pub_name", "pub_authors", "pub_year"]},
    }

    processed = process_schema(schema)

    assert "bibcode" not in processed["properties"]
    assert "pub_name" not in processed["properties"]
    assert "pub_authors" not in processed["properties"]
    assert "pub_year" not in processed["properties"]
    assert processed["required"] == ["table_name"]
    assert "if" not in processed
    assert "then" not in processed
    assert "else" not in processed

    all_of = processed["allOf"]
    assert len(all_of) == 2

    true_branch = next(branch for branch in all_of if branch["if"]["properties"]["has_bibcode"]["const"] is True)
    false_branch = next(branch for branch in all_of if branch["if"]["properties"]["has_bibcode"]["const"] is False)

    assert set(true_branch["then"]["properties"].keys()) == {"bibcode"}
    assert true_branch["then"]["required"] == ["bibcode"]

    assert set(false_branch["then"]["properties"].keys()) == {"pub_name", "pub_authors", "pub_year"}
    assert false_branch["then"]["required"] == ["pub_name", "pub_authors", "pub_year"]

    for branch in all_of:
        for field_schema in branch["then"]["properties"].values():
            assert "visible_when" not in field_schema


def test_process_schema_preserves_existing_all_of_and_noop_without_visible_when() -> None:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": {
            "flag": {"type": "boolean"},
            "name": {"type": "string"},
        },
        "allOf": [{"if": {"properties": {"flag": {"const": True}}}, "then": {"required": ["name"]}}],
    }

    processed = process_schema(schema)

    assert processed == schema
