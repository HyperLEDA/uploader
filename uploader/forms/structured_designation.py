from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.app.storage import PgStorage
from uploader.app.structured.designations import upload_designations as run_upload_designations
from uploader.clients.gen.client import adminapi
from uploader.credentials import load_credentials


class StructuredDesignationAdvancedSettings(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    print_unmatched: bool = Field(
        default=False,
        title="Log unmatched names",
        description="Append each unmatched name to the log stream.",
    )


class StructuredDesignationForm(BaseModel):
    table_name: str = Field(..., title="Name of the table")
    column_name: str = Field(
        ...,
        title="Object name column",
        description="Name of the column that represents object designation in the table.",
    )
    write: bool = Field(
        default=False,
        title="Upload results?",
        description="If enabled, upload results; otherwise dry-run (statistics only).",
    )
    advanced: StructuredDesignationAdvancedSettings = Field(
        default_factory=StructuredDesignationAdvancedSettings,
        title="Advanced settings",
    )


def handle_structured_designation(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(StructuredDesignationForm, form)
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
        run_upload_designations(
            storage,
            f.table_name.strip(),
            f.column_name.strip(),
            advanced.batch_size,
            client,
            write=f.write,
            print_unmatched=advanced.print_unmatched,
            report_func=report_func,
        )
