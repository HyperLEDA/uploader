from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

import app
from app.gen.client import adminapi
from app.plugins import get_plugin_instance
from app.upload import upload_for_web
from server.tasks import TaskDefinition, register_task

env_map = {
    "dev": "http://localhost:8080",
    "test": "https://leda.kraysent.dev",
    "prod": "https://leda.sao.ru",
}


class UploadRawForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    plugin_dir: str = Field(default="plugins", title="Plugin directory")
    plugin_name: str = Field(
        ...,
        title="Plugin name",
        description="Registered plugin id (e.g. csv, fits, vizier).",
    )
    plugin_args: list[str] = Field(
        default_factory=list,
        title="Plugin arguments",
        description="Positional constructor arguments, one per row (e.g. file path).",
    )
    table_name: str = Field(
        ...,
        title="Table name",
        description="Machine-readable table id in HyperLEDA (e.g. sdss_dr12).",
    )
    table_description: str = Field(
        default="",
        title="Table description",
        description="Human-readable description for search and display.",
    )
    has_bibcode: bool = Field(
        default=True,
        title="Use bibcode",
        description="If enabled, provide a NASA ADS bibcode; otherwise provide manual source metadata.",
    )
    bibcode: str | None = Field(default=None, title="Bibcode")
    pub_name: str | None = Field(default=None, title="Source name")
    pub_authors: list[str] = Field(
        default_factory=list,
        title="Authors",
        description="One author per entry when not using bibcode.",
    )
    pub_year: int | None = Field(default=None, title="Publication year")
    table_type: str = Field(
        default="regular",
        title="Table type",
        description="regular or COMPILATION (uppercased on submit).",
    )
    dry_run: bool = Field(
        default=False,
        title="Dry run",
        description="Show schema and process rows without creating table or uploading.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "if": {"properties": {"has_bibcode": {"const": True}}},
            "then": {"required": ["bibcode"]},
            "else": {"required": ["pub_name", "pub_authors", "pub_year"]},
        },
    )


def _handle_upload_raw(form: UploadRawForm, report: Callable[[dict[str, Any]], None]) -> None:
    client = adminapi.AuthenticatedClient(
        base_url=env_map[form.endpoint],
        token="fake",
    )
    plugins = app.discover_plugins(form.plugin_dir)
    plugin = get_plugin_instance(form.plugin_name, plugins, form.plugin_args)

    if form.has_bibcode:
        bibcode = (form.bibcode or "").strip()
    else:
        bibcode = ""

    pub_name = (form.pub_name or "").strip()
    pub_authors = list(form.pub_authors or [])
    pub_year = int(form.pub_year) if form.pub_year is not None else 0
    table_type = (form.table_type or "regular").upper()

    total_rows = upload_for_web(
        plugin,
        client,
        form.table_name.strip(),
        form.table_description.strip(),
        bibcode,
        pub_name,
        pub_authors,
        pub_year,
        table_type,
        dry_run=form.dry_run,
        report=report,
    )
    report({"type": "done", "total_rows": total_rows})


register_task(
    TaskDefinition(
        id="upload",
        title="Upload via plugin",
        description="Upload a raw data table to HyperLEDA using a plugin.",
        form_model=UploadRawForm,
        handler=_handle_upload_raw,
        group="Upload",
    ),
)
