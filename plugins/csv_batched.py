import pathlib
from typing import Generator, final

import hyperleda
import pandas
import app

type_map = {
    "object": hyperleda.DataType.string,
    "string": hyperleda.DataType.string,
    "int64": hyperleda.DataType.integer,
    "int32": hyperleda.DataType.integer,
    "float64": hyperleda.DataType.double,
    "float32": hyperleda.DataType.double,
}


@final
class CSVPlugin(app.UploaderPlugin, app.DefaultTableNamer):
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self._chunk_size = 1000
        self._current_chunk = 0
        self._total_chunks = 0
        self._reader = None
        self._schema = None

    def prepare(self) -> None:
        self._reader = pandas.read_csv(self.filename, chunksize=self._chunk_size)
        first_chunk = next(self._reader)
        self._schema = first_chunk.dtypes

        self._reader = pandas.read_csv(self.filename, chunksize=self._chunk_size)

        with open(self.filename, "r") as f:
            self._total_chunks = sum(1 for _ in f) - 1

        self._total_chunks = (
            self._total_chunks + self._chunk_size - 1
        ) // self._chunk_size
        self._current_chunk = 0

    def get_schema(self) -> list[hyperleda.ColumnDescription]:
        if self._schema is None:
            raise RuntimeError("Plugin not prepared. Call prepare() first.")

        columns = []
        for col, dtype in self._schema.items():
            data_type = type_map.get(str(dtype), hyperleda.DataType.string)

            columns.append(
                hyperleda.ColumnDescription(
                    name=str(col),
                    data_type=data_type,
                )
            )
        return columns

    def get_data(self) -> Generator[tuple[pandas.DataFrame, float], None, None]:
        if self._reader is None:
            raise RuntimeError("Plugin not prepared. Call prepare() first.")

        for chunk in self._reader:
            self._current_chunk += 1
            progress = self._current_chunk / self._total_chunks
            yield chunk, progress

    def stop(self) -> None:
        pass

    def get_table_name(self) -> str:
        return pathlib.Path(self.filename).stem


plugin = CSVPlugin
name = "csv"
