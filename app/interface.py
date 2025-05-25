import abc
from typing import Any

import hyperleda
import pandas


class UploaderPlugin[OffsetType: Any](abc.ABC):
    @classmethod
    @abc.abstractmethod
    def name(cls) -> str:
        pass

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def get_schema(self) -> list[hyperleda.ColumnDescription]:
        pass

    @abc.abstractmethod
    def get_data(
        self, offset: OffsetType | None = None
    ) -> tuple[pandas.DataFrame, OffsetType] | pandas.DataFrame:
        pass

    @abc.abstractmethod
    def stop(self):
        pass
