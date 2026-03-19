from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import app.report as report
from app.crossmatch import run_crossmatch as run_crossmatch_cmd
from app.crossmatch.resolver import LayeredResolver
from app.endpoints import db_dsn_map, env_map
from app.gen.client import adminapi
from app.storage import PgStorage
from server.credentials import load_credentials


class CrossmatchLayeredForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    table_name: str = Field(..., title="Table name")
    radius: float = Field(..., title="Search radius in arcseconds", gt=0)
    pgc_column: str = Field(
        default="",
        title="PGC column",
        description="Column containing claimed PGC in raw table; "
        "will not use PGC numbers for cross-idenitification if left empty.",
    )
    redshift_tolerance: float = Field(
        default=0,
        title="Redshift tolerance",
        description="Tolerance for redshift matching; will not use redshift for cross-identification if left empty",
        ge=0,
    )
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    print_pending: bool = Field(default=False, title="Log pending cases")
    write: bool = Field(default=False, title="Write to API")


def handle_crossmatch_layered(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(CrossmatchLayeredForm, form)
    db_user, db_password = load_credentials()
    dsn = db_dsn_map[f.endpoint].format(
        user=quote_plus(db_user),
        password=quote_plus(db_password),
    )
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    resolver = LayeredResolver(
        radius_deg=f.radius / 3600.0,
        pgc_column=f.pgc_column.strip() or None,
        redshift_tolerance=f.redshift_tolerance if f.redshift_tolerance > 0 else None,
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
