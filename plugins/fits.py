import pathlib
from collections.abc import Generator
from typing import final

import pandas
from astropy.io import fits

import app
from app.gen.client.adminapi import models

type_map = {
    "object": models.DatatypeEnum.STRING,
    "string": models.DatatypeEnum.STRING,
    "int64": models.DatatypeEnum.INTEGER,
    "int32": models.DatatypeEnum.INTEGER,
    "float64": models.DatatypeEnum.DOUBLE,
    "float32": models.DatatypeEnum.DOUBLE,
}


@final
class FITSPlugin(app.UploaderPlugin, app.DefaultTableNamer):
    def __init__(self, filename: str, hdu_index: int = 1) -> None:
        self.filename = filename
        self.hdu_index = hdu_index
        self._hdu = None
        self._table = None
        self._schema = None
        self._batch_size = 100
        self._current_batch = 0
        self._total_batches = 0

    def prepare(self) -> None:
        self._hdu = fits.open(self.filename)
        self._table = self._hdu[self.hdu_index]

        if not isinstance(self._table, fits.BinTableHDU):
            raise ValueError(f"HDU {self.hdu_index} is not a binary table")

        self._schema = self._table.columns
        self._total_batches = (len(self._table.data) + self._batch_size - 1) // self._batch_size
        self._current_batch = 0

    def get_schema(self) -> list[models.ColumnDescription]:
        if self._schema is None:
            raise RuntimeError("Plugin not prepared. Call prepare() first.")

        columns = []
        for col in self._schema:
            data_type = type_map.get(str(col.format), models.DatatypeEnum.STRING)

            columns.append(
                models.ColumnDescription(
                    name=str(col.name),
                    data_type=data_type,
                    unit=str(col.unit) if col.unit else None,
                ),
            )
        return columns

    def get_data(self) -> Generator[tuple[pandas.DataFrame, float]]:
        if self._table is None:
            raise RuntimeError("Plugin not prepared. Call prepare() first.")

        table_data = self._table.data

        for i in range(0, len(table_data), self._batch_size):
            end_idx = min(i + self._batch_size, len(table_data))
            batch_data = table_data[i:end_idx]

            df = pandas.DataFrame(batch_data)
            self._current_batch += 1
            progress = self._current_batch / self._total_batches

            yield df, progress

    def stop(self) -> None:
        if self._hdu is not None:
            self._hdu.close()

    def get_table_name(self) -> str:
        return pathlib.Path(self.filename).stem


plugin = FITSPlugin
name = "fits"
