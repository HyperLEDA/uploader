from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import app.report as report
from app.crossmatch import run_crossmatch as run_crossmatch_cmd
from app.crossmatch.resolver import TwoRadiiResolver
from app.endpoints import db_dsn_map, env_map
from app.gen.client import adminapi
from app.storage import PgStorage
from uploader.credentials import load_credentials


class CrossmatchTwoRadiiForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    table_name: str = Field(..., title="Layer 0 table name")
    r1: float = Field(..., title="Inner radius (arcsec)", gt=0)
    r2: float = Field(..., title="Outer radius (arcsec)", gt=0)
    redshift_tolerance: float = Field(
        default=0.0003,
        title="Redshift tolerance",
        description="Tolerance in z used for redshift-based disambiguation.",
        ge=0,
    )
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    print_pending: bool = Field(default=False, title="Log pending cases")
    write: bool = Field(default=False, title="Write to API")


def handle_crossmatch_two_radii(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(CrossmatchTwoRadiiForm, form)
    db_user, db_password = load_credentials()
    dsn = db_dsn_map[f.endpoint].format(
        user=quote_plus(db_user),
        password=quote_plus(db_password),
    )
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    resolver = TwoRadiiResolver(
        r1_deg=f.r1 / 3600.0,
        r2_deg=f.r2 / 3600.0,
        redshift_tolerance=f.redshift_tolerance,
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
