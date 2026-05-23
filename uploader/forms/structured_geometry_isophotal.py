from collections.abc import Callable
from typing import Literal, cast
from urllib.parse import quote_plus

from psycopg import connect
from pydantic import BaseModel, Field

import uploader.app.report as report
from uploader.app.endpoints import db_dsn_map, env_map
from uploader.app.storage import PgStorage
from uploader.app.structured.geometry import upload_geometry_isophotal
from uploader.clients.gen.client import adminapi
from uploader.credentials import load_credentials, load_token


class StructuredGeometryIsophotalAdvancedSettings(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    batch_size: int = Field(default=10000, title="Batch size", ge=1, le=500_000)


class StructuredGeometryIsophotalForm(BaseModel):
    table_name: str = Field(..., title="Rawdata table name")
    band: str = Field(..., title="Band", description="Calibrated passband id.")
    a: str = Field(..., title="a", description="Expression. Semi-major axis length.")
    e_a: str = Field(..., title="e_a", description="Expression. Error on semi-major axis.")
    b: str = Field(..., title="b", description="Expression. Semi-minor axis length.")
    e_b: str = Field(..., title="e_b", description="Expression. Error on semi-minor axis.")
    pa: str = Field(
        default="", title="pa", description="Expression. Position angle (east of north). Leave empty to store null."
    )
    e_pa: str = Field(
        default="", title="e_pa", description="Expression. Error on position angle. Leave empty to store null."
    )
    isophote: str = Field(..., title="isophote", description="Expression. Surface brightness level.")
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
    expressions: dict[str, str] = {
        "a": f.a.strip(),
        "e_a": f.e_a.strip(),
        "b": f.b.strip(),
        "e_b": f.e_b.strip(),
        "isophote": f.isophote.strip(),
    }
    if f.pa.strip():
        expressions["pa"] = f.pa.strip()
    if f.e_pa.strip():
        expressions["e_pa"] = f.e_pa.strip()
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
