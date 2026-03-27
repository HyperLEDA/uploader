from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.app.storage import PgStorage
from uploader.app.structured.redshift import upload_redshift as run_upload_redshift
from uploader.clients.gen.client import adminapi
from uploader.credentials import load_credentials


class StructuredRedshiftAdvancedSettings(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    write: bool = Field(
        default=False,
        title="Write to API",
        description="If enabled, upload results; otherwise dry-run (statistics only).",
    )


class StructuredRedshiftForm(BaseModel):
    table_name: str = Field(..., title="Rawdata table name")
    z_column: str = Field(..., title="z column", description="Column with redshift z.")
    z_error: float = Field(..., title="z error", description="Fixed error on z (same for all rows).")
    advanced: StructuredRedshiftAdvancedSettings = Field(
        default_factory=StructuredRedshiftAdvancedSettings,
        title="Advanced settings",
    )


def handle_structured_redshift(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(StructuredRedshiftForm, form)
    advanced = f.advanced
    db_user, db_password = load_credentials()
    dsn = db_dsn_map[advanced.endpoint].format(
        user=quote_plus(db_user),
        password=quote_plus(db_password),
    )
    client = adminapi.AuthenticatedClient(
        base_url=env_map[advanced.endpoint],
        token="fake",
    )
    with connect(dsn) as conn:
        storage = PgStorage(conn)
        run_upload_redshift(
            storage,
            f.table_name.strip(),
            f.z_column.strip(),
            advanced.batch_size,
            client,
            write=advanced.write,
            z_error=f.z_error,
            report_func=report_func,
        )
