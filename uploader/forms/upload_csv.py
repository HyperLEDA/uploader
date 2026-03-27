from collections.abc import Callable
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field

import uploader.app.report as report
import uploader.forms.common as common
from uploader.app.endpoints import env_map
from uploader.app.sources.csv import CSVSource
from uploader.app.upload import upload_for_web
from uploader.clients.gen.client import adminapi


class UploadCsvAdvancedSettings(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    batch_size: int = Field(default=1000000, title="Batch size", ge=1)
    dry_run: bool = Field(
        default=False,
        title="Dry run",
        description="Show schema and process rows without creating table or uploading.",
    )


class UploadCsvPublicationParameters(BaseModel):
    pub_name: str = Field(default="", title="Source name")
    pub_authors: list[str] = Field(
        default_factory=list,
        title="Authors",
        description="List of authors",
    )
    pub_year: int = Field(default=0, title="Publication year")


class UploadCsvForm(BaseModel):
    table_name: str = common.TableNameField()
    table_description: str = common.TableDescriptionField()
    has_bibcode: bool = Field(
        default=True,
        title="Use bibcode",
        description="If enabled, provide a NASA ADS bibcode; otherwise provide manual source metadata.",
    )
    bibcode: str = common.BibcodeField()
    publication_params: UploadCsvPublicationParameters = Field(
        default_factory=UploadCsvPublicationParameters,
        title="Parameters of publication",
        description="If bibcode is not provided, will use these parameters for bibliography.",
    )
    table_type: common.TableType = common.TableTypeField()
    filename: str = Field(..., title="CSV file path")
    advanced: UploadCsvAdvancedSettings = Field(
        default_factory=UploadCsvAdvancedSettings,
        title="Advanced settings",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "if": {"properties": {"has_bibcode": {"const": True}}},
            "then": {"required": ["bibcode"]},
            "else": {"required": ["pub_name", "pub_authors", "pub_year"]},
        },
    )


def handle_upload_csv(form: BaseModel, report_func: Callable[[report.Event], None]) -> None:
    f = cast(UploadCsvForm, form)
    advanced = f.advanced
    client = adminapi.AuthenticatedClient(
        base_url=env_map[advanced.endpoint],
        token="fake",
    )
    source = CSVSource(f.filename, chunk_size=advanced.batch_size)
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
