from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import app.report_events as report_events
from app.endpoints import db_dsn_map, env_map
from app.gen.client import adminapi
from app.storage import PgStorage
from app.structured.redshift import upload_redshift as run_upload_redshift


class StructuredRedshiftForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    db_user: str = Field(..., title="Database user", description="PostgreSQL user for read access to rawdata.")
    db_password: str = Field(..., title="Database password", json_schema_extra={"ui:widget": "password"})
    table_name: str = Field(..., title="Rawdata table name")
    z_column: str = Field(..., title="z column", description="Column with redshift z.")
    z_error: float = Field(..., title="z error", description="Fixed error on z (same for all rows).")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    write: bool = Field(
        default=False,
        title="Write to API",
        description="If enabled, upload results; otherwise dry-run (statistics only).",
    )


def handle_structured_redshift(
    form: BaseModel,
    report: Callable[[report_events.ReportEvent], None],
) -> None:
    f = cast(StructuredRedshiftForm, form)
    dsn = db_dsn_map[f.endpoint].format(
        user=quote_plus(f.db_user),
        password=quote_plus(f.db_password),
    )
    client = adminapi.AuthenticatedClient(
        base_url=env_map[f.endpoint],
        token="fake",
    )
    with connect(dsn) as conn:
        storage = PgStorage(conn)
        total = run_upload_redshift(
            storage,
            f.table_name.strip(),
            f.z_column.strip(),
            f.batch_size,
            client,
            write=f.write,
            z_error=f.z_error,
            report=report,
        )
    report(report_events.ReportDone(total_rows=total))
