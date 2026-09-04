from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.app.lib.formula import ExpressionStr, expression_json_schema_extra
from uploader.app.storage import PgStorage
from uploader.app.structured.designations import upload_designations as run_upload_designations
from uploader.clients.gen.client import adminapi
from uploader.credentials import load_credentials, load_token


class StructuredDesignationAdvancedSettings(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    output_file: str = Field(
        default="",
        title="Output file",
        description="If set, write id and designation pairs to this file path.",
    )


class StructuredDesignationForm(BaseModel):
    table_name: str = Field(..., title="Name of the table")
    expression: ExpressionStr = Field(
        ...,
        title="Designation expression",
        description=(
            "Expression yielding the object designation per row. "
            'Examples: col("designation"), col("weird name"), col("prefix") + " " + col("number").'
        ),
        json_schema_extra=expression_json_schema_extra(),
    )
    write: bool = Field(
        default=False,
        title="Upload results?",
        description="If enabled, upload results; otherwise dry-run.",
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
        token=load_token(),
    )
    with connect(dsn) as conn:
        storage = PgStorage(conn)
        run_upload_designations(
            storage,
            f.table_name.strip(),
            f.expression.strip(),
            advanced.batch_size,
            client,
            write=f.write,
            output_file=advanced.output_file.strip(),
            report_func=report_func,
        )
