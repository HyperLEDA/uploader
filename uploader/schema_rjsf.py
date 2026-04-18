from __future__ import annotations

import copy
from typing import Any


def split_rjsf_ui_schema(schema: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Remove ``ui:*`` keys from *schema* (recursively) and return them as an RJSF ``uiSchema``."""
    root = copy.deepcopy(schema)
    ui = _collect_ui_from_node(root)
    return root, ui


def _collect_ui_from_node(node: Any) -> dict[str, Any]:
    if not isinstance(node, dict):
        return {}
    out: dict[str, Any] = {}
    for k in list(node):
        if isinstance(k, str) and k.startswith("ui:"):
            out[k] = node.pop(k)
    props = node.get("properties")
    if isinstance(props, dict):
        for pname, pschema in props.items():
            if isinstance(pschema, dict):
                child = _collect_ui_from_node(pschema)
                if child:
                    out[pname] = child
    return out
