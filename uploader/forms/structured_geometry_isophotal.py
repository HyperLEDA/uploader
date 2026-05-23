from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.app.lib.expression import NAMED_CONSTANTS
from uploader.app.storage import PgStorage
from uploader.app.structured.geometry import upload_geometry_isophotal
from uploader.clients.gen.client import adminapi
from uploader.credentials import load_credentials, load_token

_EXPRESSION_HELP = (
    "Bare identifiers refer to rawdata column names. "
    "Identifiers starting with const_ refer to predefined constants. "
    "Operators: + - * /. Functions: sin(x), cos(x) (argument must be an angle). "
    "Numbers are dimensionless. "
    f"Available constants: {', '.join(sorted(NAMED_CONSTANTS))}."
)


class StructuredGeometryIsophotalAdvancedSettings(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)


class StructuredGeometryIsophotalForm(BaseModel):
    table_name: str = Field(..., title="Rawdata table name")
    band: str = Field(..., title="Band", description="Calibrated passband id.")
    a: str = Field(..., title="a", description=_EXPRESSION_HELP)
    e_a: str = Field(..., title="e_a", description=_EXPRESSION_HELP)
    b: str = Field(..., title="b", description=_EXPRESSION_HELP)
    e_b: str = Field(..., title="e_b", description=_EXPRESSION_HELP)
    pa: str = Field(..., title="pa", description=_EXPRESSION_HELP)
    e_pa: str = Field(..., title="e_pa", description=_EXPRESSION_HELP)
    isophote: str = Field(..., title="isophote", description=_EXPRESSION_HELP)
    write: bool = Field(
        default=False,
        title="Write to API",
        description="If enabled, upload results; otherwise dry-run (statistics only).",
    )
    advanced: StructuredGeometryIsophotalAdvancedSettings = Field(
        default_factory=StructuredGeometryIsophotalAdvancedSettings,
        title="Advanced settings",
    )


def handle_structured_geometry_isophotal(
    form: BaseModel,
    report_func: Callable[[report.Event], None],
) -> None:
    f = cast(StructuredGeometryIsophotalForm, form)
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
    expressions = {
        "a": f.a.strip(),
        "e_a": f.e_a.strip(),
        "b": f.b.strip(),
        "e_b": f.e_b.strip(),
        "pa": f.pa.strip(),
        "e_pa": f.e_pa.strip(),
        "isophote": f.isophote.strip(),
    }
    with connect(dsn) as conn:
        storage = PgStorage(conn)
        upload_geometry_isophotal(
            storage,
            f.table_name.strip(),
            f.band.strip(),
            expressions,
            advanced.batch_size,
            client,
            write=f.write,
            report_func=report_func,
        )
