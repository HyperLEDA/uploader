from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import app.report as report
from app.endpoints import db_dsn_map
from uploader.credentials import save_credentials


class AuthenticateForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    db_user: str = Field(..., title="Database user", description="PostgreSQL user for database access.")
    db_password: str = Field(..., title="Database password", json_schema_extra={"ui:widget": "password"})


def handle_authenticate(form: BaseModel, report_func: Callable[[report.Event], None]) -> None:
    f = cast(AuthenticateForm, form)
    dsn = db_dsn_map[f.endpoint].format(
        user=quote_plus(f.db_user),
        password=quote_plus(f.db_password),
    )
    report_func(report.LogEvent(message="Testing database credentials..."))
    with connect(dsn) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
    report_func(report.LogEvent(message="Database credentials are valid."))
    report_func(report.ProgressEvent(percent=100))
    save_credentials(f.db_user, f.db_password)
    report_func(report.DoneEvent(message="Database credentials saved."))
