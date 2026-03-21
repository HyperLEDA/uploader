from collections.abc import Callable
from typing import cast

from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import env_map
from uploader.app.sources.fits import FITSSource
from uploader.app.upload import upload_for_web
from uploader.clients.gen.client import adminapi
from uploader.forms.upload_base import UploadBaseForm


class UploadFitsForm(UploadBaseForm):
    filename: str = Field(..., title="FITS file path")
    hdu_index: int = Field(default=1, title="HDU index", ge=0)


def handle_upload_fits(form: BaseModel, report_func: Callable[[report.Event], None]) -> None:
    f = cast(UploadFitsForm, form)
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    source = FITSSource(f.filename, f.hdu_index)
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
