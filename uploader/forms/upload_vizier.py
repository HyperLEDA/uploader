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


class UploadVizierForm(BaseModel):
    catalog_name: str = Field(..., title="VizieR catalog name")
    source_table_name: str = Field(..., title="VizieR table name")
    cache_path: str = Field(default=".vizier_cache/", title="Cache path")
    batch_size: int = Field(default=100, title="Batch size", ge=1)
    table_name: str = common.TableNameField(
        default="",
        additional_description="Leave empty to derive from VizieR.",
    )
    table_description: str = common.TableDescriptionField(
        default="",
        additional_description="Leave empty to take from VizieR metadata.",
    )
    bibcode: str = Field(
        default="",
        title="Bibcode",
        description="NASA ADS bibcode; leave empty to read from VizieR.",
    )
    table_type: Literal["regular", "compilation"] = Field(
        default="regular",
        title="Table type",
        description="Type of data that table represents.",
    )
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


def handle_upload_vizier(form: BaseModel, report_func: Callable[[report.Event], None]) -> None:
    f = cast(UploadVizierForm, form)
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    source = VizierSource(
        f.catalog_name,
        f.source_table_name,
        cache_path=f.cache_path,
        batch_size=f.batch_size,
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
        dry_run=f.dry_run,
        report_func=report_func,
    )
