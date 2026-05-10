from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import login
from uploader.clients.gen.client.adminapi.models.api_ok_response_login_response import APIOkResponseLoginResponse
from uploader.clients.gen.client.adminapi.models.login_request import LoginRequest
from uploader.credentials import save_credentials, save_token


class AuthenticateForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    db_user: str = Field(..., title="Database user", description="PostgreSQL user for database access.")
    db_password: str = Field(..., title="Database password", json_schema_extra={"ui:widget": "password"})
    backend_user: str = Field(..., title="Backend user", description="Username for the HyperLEDA admin API.")
    backend_password: str = Field(..., title="Backend password", json_schema_extra={"ui:widget": "password"})


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
    save_credentials(f.db_user, f.db_password)

    report_func(report.LogEvent(message="Logging in to backend..."))
    client = adminapi.Client(base_url=env_map[f.endpoint])
    response = login.sync(
        client=client,
        body=LoginRequest(username=f.backend_user, password=f.backend_password),
    )
    if not isinstance(response, APIOkResponseLoginResponse):
        raise RuntimeError(f"Backend login failed: {response}")
    save_token(response.data.token)
    report_func(report.LogEvent(message="Backend login successful."))

    report_func(report.ProgressEvent(percent=100))
    report_func(report.DoneEvent(message="Credentials saved."))
