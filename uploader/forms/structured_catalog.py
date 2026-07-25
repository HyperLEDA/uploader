from collections.abc import Callable
from typing import Any, Literal
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field, create_model

import uploader.app.report as report
from uploader.app import log
from uploader.app.catalogs import fetch_catalogs
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.app.lib.expression import expression_syntax_help
from uploader.app.storage import PgStorage
from uploader.app.structured.generic import is_numeric_datatype, upload_catalog_columns
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.models.catalog_field import CatalogField
from uploader.clients.gen.client.adminapi.models.catalog_schema import CatalogSchema
from uploader.clients.gen.client.adminapi.models.datatype_enum import DatatypeEnum
from uploader.clients.gen.client.adminapi.types import Unset
from uploader.credentials import load_credentials, load_token
from uploader.tasks import TaskDefinition, register_task

GROUP = "Structured catalogs"

_RESERVED_FORM_FIELDS = frozenset({"table_name", "write", "advanced"})


class StructuredCatalogAdvancedSettings(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)


def _field_required(field: CatalogField) -> bool:
    if isinstance(field.required, Unset):
        return True
    return bool(field.required)


def _field_description(field: CatalogField, *, numeric: bool) -> str:
    if numeric:
        if isinstance(field.description, str) and field.description:
            return f"Expression. {field.description}"
        return f"Expression. Value for {field.name}."
    if isinstance(field.description, str) and field.description:
        return field.description
    return f"Source column for {field.name}."


def _field_unit(field: CatalogField) -> str | None:
    if isinstance(field.unit, str):
        return field.unit
    return None


def build_catalog_form(schema: CatalogSchema) -> type[BaseModel]:
    field_definitions: dict[str, Any] = {
        "table_name": (str, Field(..., title="Table name")),
    }
    for field in schema.fields:
        if field.name in _RESERVED_FORM_FIELDS:
            raise RuntimeError(f"Catalog {schema.catalog!r} field {field.name!r} conflicts with reserved form field")
        numeric = is_numeric_datatype(field.data_type)
        title = field.name if numeric else f"{field.name} column"
        description = _field_description(field, numeric=numeric)
        if _field_required(field):
            field_definitions[field.name] = (
                str,
                Field(..., title=title, description=description),
            )
        else:
            field_definitions[field.name] = (
                str,
                Field(default="", title=title, description=description),
            )
    field_definitions["write"] = (
        bool,
        Field(
            default=False,
            title="Write to API",
            description="If enabled, upload results; otherwise dry-run (statistics only).",
        ),
    )
    field_definitions["advanced"] = (
        StructuredCatalogAdvancedSettings,
        Field(
            default_factory=StructuredCatalogAdvancedSettings,
            title="Advanced settings",
        ),
    )
    model_name = f"StructuredCatalog{schema.catalog.title().replace('_', '')}Form"
    return create_model(model_name, **field_definitions)


def _make_handler(
    schema: CatalogSchema,
) -> Callable[[BaseModel, Callable[[report.Event], None]], None]:
    catalog_fields = list(schema.fields)
    catalog_name = schema.catalog
    required_names = {f.name for f in catalog_fields if _field_required(f)}
    field_types: dict[str, DatatypeEnum] = {f.name: f.data_type for f in catalog_fields}
    field_units: dict[str, str] = {f.name: unit for f in catalog_fields if (unit := _field_unit(f)) is not None}
    field_order = [f.name for f in catalog_fields]
    numeric_names = {f.name for f in catalog_fields if is_numeric_datatype(f.data_type)}

    def handler(
        form: BaseModel,
        report_func: Callable[[report.Event], None],
    ) -> None:
        values = form.model_dump()
        advanced = values["advanced"]
        endpoint = str(advanced["endpoint"])
        batch_size = int(advanced["batch_size"])
        table_name = str(values["table_name"]).strip()
        column_map: dict[str, str] = {}
        expressions: dict[str, str] = {}
        provided: set[str] = set()
        for name in field_order:
            raw = str(values.get(name, "") or "").strip()
            if not raw:
                continue
            provided.add(name)
            if name in numeric_names:
                expressions[name] = raw
            else:
                column_map[name] = raw
        missing = sorted(name for name in required_names if name not in provided)
        if missing:
            raise RuntimeError(f"Missing required field(s): {missing}")
        columns = [name for name in field_order if name in provided]

        db_user, db_password = load_credentials()
        dsn = db_dsn_map[endpoint].format(
            user=quote_plus(db_user),
            password=quote_plus(db_password),
        )
        client = adminapi.AuthenticatedClient(
            base_url=env_map[endpoint],
            token=load_token(),
        )
        with connect(dsn) as conn:
            storage = PgStorage(conn)
            upload_catalog_columns(
                storage,
                table_name,
                catalog_name,
                column_map,
                expressions,
                field_types,
                field_units,
                columns,
                batch_size,
                client,
                write=bool(values["write"]),
                report_func=report_func,
            )

    return handler


def _task_description(schema: CatalogSchema) -> str:
    base = schema.description.strip()
    help_text = expression_syntax_help()
    if base:
        return f"{base}\n\n{help_text}"
    return help_text


def register_structured_catalog_tasks(
    catalogs: list[CatalogSchema] | None = None,
) -> None:
    try:
        schemas = catalogs if catalogs is not None else fetch_catalogs()
    except Exception as e:
        log.logger.warning("failed to fetch catalogs for structured catalog tasks", error=str(e))
        return
    for schema in schemas:
        register_task(
            TaskDefinition(
                id=f"upload-{schema.catalog}",
                title=schema.title,
                description=_task_description(schema),
                form_model=build_catalog_form(schema),
                handler=_make_handler(schema),
                group=GROUP,
            ),
        )
