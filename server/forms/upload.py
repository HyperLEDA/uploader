from collections.abc import Callable
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

import app
from app.endpoints import env_map
from app.gen.client import adminapi
from app.plugins import get_plugin_instance
from app.upload import upload_for_web


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
    bibcode: str = Field(default="", title="Bibcode")
    pub_name: str = Field(default="", title="Source name")
    pub_authors: list[str] = Field(
        default_factory=list,
        title="Authors",
        description="One author per entry when not using bibcode.",
    )
    pub_year: int = Field(default=0, title="Publication year")
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


def handle_upload_raw(form: BaseModel, report: Callable[[dict[str, Any]], None]) -> None:
    f = cast(UploadRawForm, form)
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    plugins = app.discover_plugins(f.plugin_dir)
    plugin = get_plugin_instance(f.plugin_name, plugins, f.plugin_args)

    bibcode = f.bibcode.strip() if f.has_bibcode else ""
    pub_name = f.pub_name.strip()
    pub_authors = list(f.pub_authors)
    pub_year = f.pub_year
    table_type = (f.table_type or "regular").upper()

    total_rows = upload_for_web(
        plugin,
        client,
        f.table_name.strip(),
        f.table_description.strip(),
        bibcode,
        pub_name,
        pub_authors,
        pub_year,
        table_type,
        dry_run=f.dry_run,
        report=report,
    )
    report({"type": "done", "total_rows": total_rows})
