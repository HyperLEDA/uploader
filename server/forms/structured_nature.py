from collections.abc import Callable
from typing import Any, Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, ConfigDict, Field

from app.endpoints import db_dsn_map, env_map
from app.gen.client import adminapi
from app.storage import PgStorage
from app.structured.nature import upload_nature as run_upload_nature


class StructuredNatureForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    db_user: str = Field(..., title="Database user", description="PostgreSQL user for read access to rawdata.")
    db_password: str = Field(..., title="Database password", json_schema_extra={"ui:widget": "password"})
    table_name: str = Field(..., title="Rawdata table name")
    column_name: str = Field(
        default="",
        title="Type column",
        description="Column with object type; leave empty if every row uses default type only.",
    )
    default_type: str = Field(
        default="",
        title="Default LEDA type",
        description="LEDA type when column is empty or value unmapped; leave empty to pass raw values through.",
    )
    type_mappings: list[str] = Field(
        default_factory=list,
        title="Type mappings",
        description='Each entry "raw_value:leda_type" (e.g. G:galaxy).',
    )
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    write: bool = Field(
        default=False,
        title="Write to API",
        description="If enabled, upload results; otherwise dry-run (statistics only).",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "anyOf": [
                {
                    "properties": {"column_name": {"type": "string", "minLength": 1}},
                    "required": ["column_name"],
                },
                {
                    "properties": {"default_type": {"type": "string", "minLength": 1}},
                    "required": ["default_type"],
                },
            ],
        },
    )


def _parse_type_mappings(entries: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for s in entries:
        t = s.strip()
        if not t:
            continue
        if ":" not in t:
            raise ValueError(f"Invalid mapping (expected raw_value:leda_type): {t!r}")
        raw, _, leda = t.partition(":")
        if raw in out:
            raise ValueError(f"Duplicate mapping for raw value: {raw!r}")
        out[raw] = leda
    return out


def handle_structured_nature(
    form: BaseModel,
    report: Callable[[dict[str, Any]], None],
) -> None:
    f = cast(StructuredNatureForm, form)
    col = f.column_name.strip() or None
    default_t = f.default_type.strip() or None
    if col is None and default_t is None:
        raise ValueError("Either type column or default LEDA type (or both) must be set.")
    type_mapping = _parse_type_mappings(f.type_mappings)

    dsn = db_dsn_map[f.endpoint].format(
        user=quote_plus(f.db_user),
        password=quote_plus(f.db_password),
    )
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    with connect(dsn) as conn:
        storage = PgStorage(conn)
        total = run_upload_nature(
            storage,
            f.table_name.strip(),
            col,
            type_mapping,
            default_t,
            f.batch_size,
            client,
            write=f.write,
            report=report,
        )
    report({"type": "done", "total_rows": total})
