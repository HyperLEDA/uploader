from collections.abc import Callable
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field

import uploader.app.report as report
import uploader.forms.common as common
from uploader.app.endpoints import env_map
from uploader.app.sources.fits import FITSSource
from uploader.app.upload import upload_for_web
from uploader.clients.gen.client import adminapi


class UploadFitsAdvancedSettings(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    dry_run: bool = Field(
        default=False,
        title="Dry run",
        description="Show schema and process rows without creating table or uploading.",
    )
    hdu_index: int = Field(default=1, title="HDU index", ge=0)


class UploadFitsForm(BaseModel):
    table_name: str = common.TableNameField()
    table_description: str = common.TableDescriptionField()
    has_bibcode: bool = Field(
        default=True,
        title="Use bibcode",
        description="If enabled, provide a NASA ADS bibcode; otherwise provide manual source metadata.",
    )
    bibcode: str = common.BibcodeField()
    pub_name: str = Field(default="", title="Source name")
    pub_authors: list[str] = Field(
        default_factory=list,
        title="Authors",
        description="One author per entry when not using bibcode.",
    )
    pub_year: int = Field(default=0, title="Publication year")
    table_type: common.TableType = common.TableTypeField()
    filename: str = Field(..., title="FITS file path")
    advanced: UploadFitsAdvancedSettings = Field(
        default_factory=UploadFitsAdvancedSettings,
        title="Advanced settings",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "if": {"properties": {"has_bibcode": {"const": True}}},
            "then": {"required": ["bibcode"]},
            "else": {"required": ["pub_name", "pub_authors", "pub_year"]},
        },
    )


def handle_upload_fits(form: BaseModel, report_func: Callable[[report.Event], None]) -> None:
    f = cast(UploadFitsForm, form)
    advanced = f.advanced
    client = adminapi.AuthenticatedClient(
        base_url=env_map[advanced.endpoint],
        token="fake",
    )
    source = FITSSource(f.filename, advanced.hdu_index)
    bibcode = f.bibcode.strip() if f.has_bibcode else ""
    pub_name = f.pub_name.strip()
    pub_authors = list(f.pub_authors)
    pub_year = f.pub_year
    table_type = (f.table_type or "regular").upper()

    upload_for_web(
        source,
        client,
        f.table_name.strip(),
        f.table_description.strip(),
        bibcode,
        pub_name,
        pub_authors,
        pub_year,
        table_type,
        dry_run=advanced.dry_run,
        report_func=report_func,
    )
