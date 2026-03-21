from collections.abc import Callable
from typing import cast

from pydantic import BaseModel, Field

import app.report as report
from app.endpoints import env_map
from app.gen.client import adminapi
from app.sources.csv import CSVSource
from app.upload import upload_for_web
from uploader.forms.upload_base import UploadBaseForm


class UploadCsvForm(UploadBaseForm):
    filename: str = Field(..., title="CSV file path")


def handle_upload_csv(form: BaseModel, report_func: Callable[[report.Event], None]) -> None:
    f = cast(UploadCsvForm, form)
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    source = CSVSource(f.filename)
    bibcode = f.bibcode.strip() if f.has_bibcode else ""
    pub_name = f.pub_name.strip()
    pub_authors = list(f.pub_authors)
    pub_year = f.pub_year
    table_type = (f.table_type or "regular").upper()

    upload_for_web(
        source,
        client,
        f.table_name.strip(),
        f.table_description.strip(),
        bibcode,
        pub_name,
        pub_authors,
        pub_year,
        table_type,
        dry_run=f.dry_run,
        report_func=report_func,
    )
