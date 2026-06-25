import pathlib
from collections.abc import Generator
from typing import final

import pandas

import uploader.app as app
from uploader.clients.gen.client.adminapi import models

type_map = {
    "object": models.DatatypeEnum.STRING,
    "string": models.DatatypeEnum.STRING,
    "int64": models.DatatypeEnum.INTEGER,
    "int32": models.DatatypeEnum.INTEGER,
    "float64": models.DatatypeEnum.DOUBLE,
    "float32": models.DatatypeEnum.DOUBLE,
}


@final
class CSVSource(app.UploaderSource, app.DefaultTableNamer):
    def __init__(self, filename: str, *, chunk_size: int = 10000) -> None:
        self.filename = filename
        self._chunk_size = chunk_size
        self._current_chunk = 0
        self._total_chunks = 0
        self._total_rows = 0
        self._reader = None
        self._schema = None

    def prepare(self) -> None:
        self._reader = pandas.read_csv(self.filename, chunksize=self._chunk_size)
        first_chunk = next(self._reader)
        self._schema = first_chunk.dtypes

        self._reader = pandas.read_csv(self.filename, chunksize=self._chunk_size)

        with pathlib.Path(self.filename).open() as f:
            self._total_rows = sum(1 for _ in f) - 1

        self._total_chunks = (self._total_rows + self._chunk_size - 1) // self._chunk_size
        self._current_chunk = 0

    def get_schema(self) -> list[models.ColumnDescription]:
        if self._schema is None:
            raise RuntimeError("Plugin not prepared. Call prepare() first.")

        columns = []
        for col, dtype in self._schema.items():
            data_type = type_map.get(str(dtype), models.DatatypeEnum.STRING)

            columns.append(
                models.ColumnDescription(
                    name=str(col),
                    data_type=data_type,
                )
            )
        return columns

    def get_data(self) -> Generator[tuple[pandas.DataFrame, int, int]]:
        if self._reader is None:
            raise RuntimeError("Plugin not prepared. Call prepare() first.")

        for chunk in self._reader:
            self._current_chunk += 1
            yield chunk, self._current_chunk, self._total_chunks

    def stop(self) -> None:
        pass

    def get_total_rows(self) -> int:
        return self._total_rows

    def get_table_name(self) -> str:
        return pathlib.Path(self.filename).stem
