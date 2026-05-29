from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.crossmatch.submit import run_submit_crossmatch
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.app.storage import PgStorage
from uploader.clients.gen.client import adminapi
from uploader.credentials import load_credentials, load_token


class SubmitCrossmatchForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    table_name: str = Field(..., title="Table name")
    batch_size: int = Field(default=1000, title="Batch size", ge=1, le=10000)
    write: bool = Field(
        default=False,
        title="Write to API",
        description="If disabled, only counts records that would be submitted.",
    )


def handle_submit_crossmatch(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(SubmitCrossmatchForm, form)
    db_user, db_password = load_credentials()
    dsn = db_dsn_map[f.endpoint].format(
        user=quote_plus(db_user),
        password=quote_plus(db_password),
    )
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token=load_token(),
    )
    with connect(dsn) as conn:
        storage = PgStorage(conn)
        run_submit_crossmatch(
            storage,
            f.table_name.strip(),
            f.batch_size,
            client,
            report_func=report_func,
            write=f.write,
        )
