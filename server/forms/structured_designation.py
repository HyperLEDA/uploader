from collections.abc import Callable
from typing import Any, Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

from app.endpoints import db_dsn_map, env_map
from app.gen.client import adminapi
from app.storage import PgStorage
from app.structured.designations import upload_designations as run_upload_designations
from server.tasks import TaskDefinition, register_task


class StructuredDesignationForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    db_user: str = Field(..., title="Database user", description="PostgreSQL user for read access to rawdata.")
    db_password: str = Field(..., title="Database password", json_schema_extra={"ui:widget": "password"})
    table_name: str = Field(..., title="Rawdata table name")
    column_name: str = Field(..., title="Name column", description="Column containing the object name.")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)
    write: bool = Field(
        default=False,
        title="Write to API",
        description="If enabled, upload results; otherwise dry-run (statistics only).",
    )
    print_unmatched: bool = Field(
        default=False,
        title="Log unmatched names",
        description="Append each unmatched name to the log stream.",
    )


def _handle_structured_designation(
    form: BaseModel,
    report: Callable[[dict[str, Any]], None],
) -> None:
    f = cast(StructuredDesignationForm, form)
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
        total = run_upload_designations(
            storage,
            f.table_name.strip(),
            f.column_name.strip(),
            f.batch_size,
            client,
            write=f.write,
            print_unmatched=f.print_unmatched,
            report=report,
        )
    report({"type": "done", "total_rows": total})


register_task(
    TaskDefinition(
        id="upload-structured-designation",
        title="Designations",
        description="Upload object designations (names) from a raw table to the structured level.",
        form_model=StructuredDesignationForm,
        handler=_handle_structured_designation,
        group="Upload structured",
    ),
)
