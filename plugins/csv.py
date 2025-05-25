from typing import final

import hyperleda
import pandas
import app


@final
class CSVPlugin(app.UploaderPlugin):
    def __init__(self, filename: str) -> None:
        self.filename = filename

    @classmethod
    def name(cls) -> str:
        return "csv"

    def start(self):
        self.file = open(self.filename, "r")

    def stop(self):
        self.file.close()

    def get_schema(self) -> hyperleda.CreateTableRequestSchema:
        raise NotImplementedError

    def get_data(
        self, offset: int | None = None
    ) -> tuple[pandas.DataFrame, int] | pandas.DataFrame:
        raise NotImplementedError


plugin = CSVPlugin
