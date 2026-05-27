from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.app.storage import PgStorage
from uploader.app.structured.icrs import upload_icrs as run_upload_icrs
from uploader.clients.gen.client import adminapi
from uploader.credentials import load_credentials, load_token


class StructuredIcrsAdvancedSettings(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)


class StructuredIcrsForm(BaseModel):
    table_name: str = Field(..., title="Rawdata table name")
    ra_column: str = Field(..., title="RA column", description="Column containing right ascension.")
    dec_column: str = Field(..., title="Dec column", description="Column containing declination.")
    e_ra: str = Field(..., title="e_ra", description="Expression. Positional error for RA.")
    e_dec: str = Field(..., title="e_dec", description="Expression. Positional error for Dec.")
    write: bool = Field(
        default=False,
        title="Write to API",
        description="If enabled, upload results; otherwise dry-run (statistics only).",
    )
    advanced: StructuredIcrsAdvancedSettings = Field(
        default_factory=StructuredIcrsAdvancedSettings,
        title="Advanced settings",
    )


def handle_structured_icrs(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(StructuredIcrsForm, form)
    advanced = f.advanced
    db_user, db_password = load_credentials()
    dsn = db_dsn_map[advanced.endpoint].format(
        user=quote_plus(db_user),
        password=quote_plus(db_password),
    )
    client = adminapi.AuthenticatedClient(
        base_url=env_map[advanced.endpoint],
        token=load_token(),
    )
    expressions: dict[str, str] = {
        "e_ra": f.e_ra.strip(),
        "e_dec": f.e_dec.strip(),
    }
    with connect(dsn) as conn:
        storage = PgStorage(conn)
        run_upload_icrs(
            storage,
            f.table_name.strip(),
            f.ra_column.strip(),
            f.dec_column.strip(),
            expressions,
            advanced.batch_size,
            client,
            write=f.write,
            report_func=report_func,
        )
