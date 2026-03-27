from typing import Literal

from pydantic import Field
from pydantic.fields import FieldInfo

type TableType = Literal["regular", "compilation"]


class BaseTextField:
    base_description: str = ""
    title: str = ""

    @classmethod
    def build_description(cls, additional_description: str) -> str:
        if additional_description:
            return f"{cls.base_description} {additional_description}"
        return cls.base_description


class TableNameField(BaseTextField):
    base_description = "Machine-readable table id in HyperLEDA (e.g. sdss_dr12)."
    title = "Table name"

    def __new__(cls, *, default: object = ..., additional_description: str = "") -> FieldInfo:
        description = cls.build_description(additional_description)
        return Field(default, title=cls.title, description=description)


class TableDescriptionField(BaseTextField):
    base_description = "Human-readable description for search and display."
    title = "Table description"

    def __new__(cls, *, default: str = "", additional_description: str = "") -> FieldInfo:
        description = cls.build_description(additional_description)
        return Field(default=default, title=cls.title, description=description)


class TableTypeField:
    def __new__(cls, *, default: TableType = "regular") -> FieldInfo:
        return Field(
            default=default,
            title="Table type",
            description="Type of data that table represents.",
        )


class BibcodeField(BaseTextField):
    base_description = "NASA ADS bibcode."
    title = "Bibcode"

    def __new__(cls, *, default: str = "", additional_description: str = "") -> FieldInfo:
        description = cls.build_description(additional_description)
        return Field(
            default=default,
            title=cls.title,
            description=description,
            pattern=r"^$|^\d{4}[A-Za-z0-9.&]{14}[A-Za-z0-9]$",
        )
