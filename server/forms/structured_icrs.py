from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import app.report as report
from app.endpoints import db_dsn_map, env_map
from app.gen.client import adminapi
from app.storage import PgStorage
from app.structured.icrs import upload_icrs as run_upload_icrs
from server.credentials import load_credentials


class StructuredIcrsForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    table_name: str = Field(..., title="Rawdata table name")
    ra_column: str = Field(..., title="RA column", description="Column containing right ascension.")
    dec_column: str = Field(..., title="Dec column", description="Column containing declination.")
    ra_error: float = Field(..., title="RA error", description="Positional error for RA (all rows).")
    ra_error_unit: str = Field(..., title="RA error unit", description="e.g. arcsec")
    dec_error: float = Field(..., title="Dec error", description="Positional error for Dec (all rows).")
    dec_error_unit: str = Field(..., title="Dec error unit", description="e.g. arcsec")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    write: bool = Field(
        default=False,
        title="Write to API",
        description="If enabled, upload results; otherwise dry-run (statistics only).",
    )


def handle_structured_icrs(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(StructuredIcrsForm, form)
    db_user, db_password = load_credentials()
    dsn = db_dsn_map[f.endpoint].format(
        user=quote_plus(db_user),
        password=quote_plus(db_password),
    )
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    with connect(dsn) as conn:
        storage = PgStorage(conn)
        run_upload_icrs(
            storage,
            f.table_name.strip(),
            f.ra_column.strip(),
            f.dec_column.strip(),
            f.batch_size,
            client,
            write=f.write,
            ra_error=f.ra_error,
            ra_error_unit=f.ra_error_unit.strip(),
            dec_error=f.dec_error,
            dec_error_unit=f.dec_error_unit.strip(),
            report_func=report_func,
        )
