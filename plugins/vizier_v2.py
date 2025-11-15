import itertools
import pathlib
from collections.abc import Generator
from typing import final

import numpy as np
import pandas
from astropy import table
from astroquery import vizier

import app
from app.gen.client.adminapi import models, types


def _sanitize_filename(string: str) -> str:
    return (
        string.replace("/", "_")
        .replace("&", "_and_")
        .replace(">", "_gt_")
        .replace("<", "_lt_")
        .replace("=", "_eq_")
        .replace(" ", "_")
        .replace("!", "_not_")
    )


def dtype_to_datatype(dtype: str | np.dtype) -> models.DatatypeEnum:
    dtype_str = str(dtype).lower()

    if any(dtype_str.startswith(x) for x in ("str", "unicode", "<u", "|s", "<U", "object", "bytes")):
        return models.DatatypeEnum.STRING

    if any(
        dtype_str.startswith(x)
        for x in (
            "int8",
            "int16",
            "uint8",
            "uint16",
        )
    ):
        return models.DatatypeEnum.INTEGER

    if any(dtype_str.startswith(x) for x in ("int32", "int64", "uint32", "uint64")):
        return models.DatatypeEnum.LONG

    if any(dtype_str.startswith(x) for x in ("float", "float16", "float32", "float64", "double", "float128")):
        return models.DatatypeEnum.DOUBLE
    return models.DatatypeEnum.STRING


@final
class VizierV2Plugin(
    app.UploaderPlugin,
    app.DefaultTableNamer,
    app.BibcodeProvider,
    app.DescriptionProvider,
):
    def __init__(
        self,
        catalog_name: str,
        table_name: str,
        *constraints: str,
        cache_path: str = ".vizier_cache/",
        batch_size: int = 10,
    ):
        if len(constraints) % 3 != 0:
            raise ValueError("constraints must be provided in groups of three (column, sign, value)")
        self.constraints: list[app.Constraint] = []
        for i in range(0, len(constraints), 3):
            self.constraints.append(
                app.Constraint(column=constraints[i], operator=constraints[i + 1], value=constraints[i + 2])
            )
        self.catalog_name = catalog_name
        self.table_name = table_name
        self.batch_size = batch_size
        self.repo = app.TAPRepository()

    def prepare(self) -> None:
        pass

    def get_table_name(self) -> str:
        return _sanitize_filename(self.table_name)

    def get_bibcode(self) -> str:
        resp = vizier.Vizier().get_catalog_metadata(catalog=self.catalog_name)
        return resp["origin_article"][0]

    def get_description(self) -> str:
        resp = vizier.Vizier().get_catalog_metadata(catalog=self.catalog_name)
        return resp["title"][0]

    def get_schema(self) -> list[models.ColumnDescription]:
        t = self.repo.query(self.table_name, limit=1)
        result = []

        for _, col in t.columns.items():
            result.append(
                models.ColumnDescription(
                    name=col.name,
                    data_type=dtype_to_datatype(col.dtype),
                    ucd=col.meta.get("ucd", types.UNSET),
                    description=col.description,
                    unit=str(col.unit) if col.unit else types.UNSET,
                )
            )

        return result

    def get_data(self) -> Generator[tuple[pandas.DataFrame, float]]:
        constraints = self.constraints if self.constraints else None
        t = self.repo.query(self.table_name, constraints=constraints)

        total_rows = len(t)
        app.logger.info("uploading table", total_rows=total_rows)

        offset = 0
        for batch in itertools.batched(t, self.batch_size, strict=False):  # pyright: ignore[reportArgumentType]
            offset += len(batch)

            rows = []
            for row in batch:
                row_dict = {k: v for k, v in dict(row).items() if v != "--"}
                rows.append(row_dict)

            yield pandas.DataFrame(rows), offset / total_rows

    def stop(self) -> None:
        pass


plugin = VizierV2Plugin
name = "vizier-v2"
