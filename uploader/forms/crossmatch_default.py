from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.crossmatch import run_crossmatch as run_crossmatch_cmd
from uploader.app.crossmatch.resolver import DefaultResolver
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.app.storage import PgStorage
from uploader.clients.gen.client import adminapi
from uploader.credentials import load_credentials, load_token


class CrossmatchDefaultForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    table_name: str = Field(..., title="Layer 0 table name")
    radius: float = Field(..., title="Search radius (arcsec)", gt=0)
    pgc_column: str = Field(
        default="",
        title="PGC column",
        description="Column containing claimed PGC in raw table; leave empty to disable.",
    )
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    print_pending: bool = Field(default=False, title="Log pending cases")
    write: bool = Field(default=False, title="Write to API")


def handle_crossmatch_default(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(CrossmatchDefaultForm, form)
    db_user, db_password = load_credentials()
    dsn = db_dsn_map[f.endpoint].format(
        user=quote_plus(db_user),
        password=quote_plus(db_password),
    )
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token=load_token(),
    )
    resolver = DefaultResolver(
        radius_deg=f.radius / 3600.0,
        pgc_column=f.pgc_column.strip() or None,
    )
    with connect(dsn) as conn:
        storage = PgStorage(conn)
        run_crossmatch_cmd(
            storage,
            f.table_name.strip(),
            f.batch_size,
            client,
            resolver=resolver,
            print_pending=f.print_pending,
            write=f.write,
            report_func=report_func,
        )
