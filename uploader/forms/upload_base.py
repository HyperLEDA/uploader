from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

import uploader.forms.common as common


class UploadBaseForm(BaseModel):
    endpoint: Literal["dev", "test", "prod"] = Field(default="prod", title="API endpoint")
    table_name: str = common.TableNameField()
    table_description: str = common.TableDescriptionField()
    has_bibcode: bool = Field(
        default=True,
        title="Use bibcode",
        description="If enabled, provide a NASA ADS bibcode; otherwise provide manual source metadata.",
    )
    bibcode: str = Field(default="", title="Bibcode")
    pub_name: str = Field(default="", title="Source name")
    pub_authors: list[str] = Field(
        default_factory=list,
        title="Authors",
        description="One author per entry when not using bibcode.",
    )
    pub_year: int = Field(default=0, title="Publication year")
    table_type: str = Field(
        default="regular",
        title="Table type",
        description="regular or COMPILATION (uppercased on submit).",
    )
    dry_run: bool = Field(
        default=False,
        title="Dry run",
        description="Show schema and process rows without creating table or uploading.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "if": {"properties": {"has_bibcode": {"const": True}}},
            "then": {"required": ["bibcode"]},
            "else": {"required": ["pub_name", "pub_authors", "pub_year"]},
        },
    )
