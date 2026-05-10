import urllib.parse
from collections.abc import Callable
from typing import Literal, cast

from pydantic import BaseModel, Field

import uploader.app.report as report
import uploader.forms.common as common
from uploader.app.endpoints import env_map
from uploader.app.sources.vizier import VizierSource
from uploader.app.upload import upload_for_web
from uploader.clients.gen.client import adminapi
from uploader.credentials import load_token


class UploadVizierAdvancedSettings(BaseModel):
    cache_path: str = Field(default=".vizier_cache/", title="Cache path")
    batch_size: int = Field(default=100, title="Batch size", ge=1)
    dry_run: bool = Field(
        default=False,
        title="Dry run",
        description="Show schema and process rows without actually uploading any data. Useful for validation.",
    )
    endpoint: Literal["dev", "test", "prod"] = Field(
        default="prod",
        title="API endpoint",
        description="Where to upload. Leave unchanged if https://leda.sao.ru is needed.",
    )


class UploadVizierForm(BaseModel):
    catalog_name: str = Field(..., title="VizieR catalog name")
    source_table_name: str = Field(..., title="VizieR table name")
    table_name: str = common.TableNameField(
        default="",
        additional_description="Leave empty to derive from VizieR.",
    )
    table_description: str = common.TableDescriptionField(
        default="",
        additional_description="Leave empty to take from VizieR metadata.",
    )
    bibcode: str = common.BibcodeField(
        default="",
        additional_description="Leave empty to read from VizieR.",
    )
    table_type: common.TableType = common.TableTypeField()
    advanced: UploadVizierAdvancedSettings = Field(
        default_factory=UploadVizierAdvancedSettings,
        title="Advanced settings",
    )


def handle_upload_vizier(form: BaseModel, report_func: Callable[[report.Event], None]) -> None:
    f = cast(UploadVizierForm, form)
    advanced = f.advanced
    client = adminapi.AuthenticatedClient(
        base_url=env_map[advanced.endpoint],
        token=load_token(),
    )
    source = VizierSource(
        f.catalog_name,
        f.source_table_name,
        cache_path=advanced.cache_path,
        batch_size=advanced.batch_size,
    )

    table_name_in = f.table_name.strip()
    if table_name_in:
        resolved_table_name = table_name_in
    else:
        resolved_table_name = source.get_table_name()

    table_description_in = f.table_description.strip()
    if table_description_in:
        resolved_description = table_description_in
    else:
        resolved_description = source.get_description()

    bibcode_in = f.bibcode.strip()
    if bibcode_in:
        resolved_bibcode = bibcode_in
    else:
        resolved_bibcode = source.get_bibcode()

    if not resolved_bibcode:
        msg = "bibcode is empty: provide one or ensure VizieR metadata includes it"
        raise ValueError(msg)

    report_func(report.LogEvent(message=f"Table name: {resolved_table_name}"))
    report_func(report.LogEvent(message=f"Description: {resolved_description}"))
    ads_url = "https://ui.adsabs.harvard.edu/abs/" + urllib.parse.quote(resolved_bibcode, safe="")
    report_func(
        report.LogEvent(
            message=f"Paper: {ads_url}",
        ),
    )

    table_type = (f.table_type or "regular").upper()

    upload_for_web(
        source,
        client,
        resolved_table_name,
        resolved_description,
        resolved_bibcode,
        "",
        [],
        0,
        table_type,
        dry_run=advanced.dry_run,
        report_func=report_func,
    )
