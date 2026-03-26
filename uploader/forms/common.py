from pydantic import Field
from pydantic.fields import FieldInfo


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
