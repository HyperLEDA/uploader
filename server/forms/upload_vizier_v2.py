from collections.abc import Callable
from typing import cast

from pydantic import BaseModel, Field

import app.report as report
import server.paths as paths
from app.endpoints import env_map
from app.gen.client import adminapi
from app.sources.vizier_v2 import VizierV2Source
from app.upload import upload_for_web
from server.forms.upload_base import UploadBaseForm

_DEFAULT_VIZIER_CACHE = str(paths.DATA_DIR / "vizier_cache")


class UploadVizierV2Form(UploadBaseForm):
    catalog_name: str = Field(..., title="VizieR catalog name")
    source_table_name: str = Field(..., title="VizieR table name")
    index_column: str = Field(..., title="Index column")
    constraints: list[str] = Field(
        default_factory=list,
        title="Constraints",
        description="Provide as repeating triples: column, operator, value.",
    )
    cache_path: str = Field(default=_DEFAULT_VIZIER_CACHE, title="Cache path")
    batch_size: int = Field(default=1000, title="Batch size", ge=1)


def handle_upload_vizier_v2(form: BaseModel, report_func: Callable[[report.Event], None]) -> None:
    f = cast(UploadVizierV2Form, form)
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    source = VizierV2Source(
        f.catalog_name,
        f.source_table_name,
        f.index_column,
        *f.constraints,
        cache_path=f.cache_path,
        batch_size=f.batch_size,
    )
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
