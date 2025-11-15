import itertools
import pathlib
from collections.abc import Generator
from typing import final

import numpy as np
import pandas
from astropy import table
from astroquery import utils, vizier

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


class CachedVizierClient:
    def __init__(self, cache_path: str = ".vizier_cache/"):
        self.cache_path = cache_path
        self._client = vizier.Vizier()
        self._client.ROW_LIMIT = -1

    def _obtain_cache_path(
        self, catalog_name: str, row_num: int | None = None, constraints: dict[str, str] | None = None
    ) -> pathlib.Path:
        filename = f"{catalog_name}.vot"
        if row_num is not None:
            filename = f"{catalog_name}_rows_{row_num}.vot"
        if constraints:
            sorted_constraints = sorted(constraints.items())
            constraint_str = "_".join(f"{k}_{v}" for k, v in sorted_constraints)
            filename = f"{catalog_name}_constraints_{constraint_str}.vot"

        filename = _sanitize_filename(filename)
        path = pathlib.Path(self.cache_path) / "catalogs" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _write_catalog_cache(
        self, catalog_name: str, row_num: int | None = None, constraints: dict[str, str] | None = None
    ) -> None:
        app.logger.info(
            "downloading catalog from Vizier",
            catalog_name=catalog_name,
            row_num=row_num,
            constraints=constraints,
        )
        client = self._client
        if row_num is not None:
            client = vizier.Vizier()
            client.ROW_LIMIT = row_num
        query_kwargs = {"catalog": catalog_name}
        if constraints:
            query_kwargs.update(constraints)
        catalogs: utils.TableList = client.query_constraints(**query_kwargs)  # pyright: ignore[reportAttributeAccessIssue]

        if not catalogs:
            raise ValueError("catalog not found")

        cache_filename = self._obtain_cache_path(catalog_name, row_num, constraints)
        catalogs[0].write(str(cache_filename), format="votable")
        app.logger.debug("wrote catalog cache", location=str(cache_filename))

    def get_table(
        self, catalog_name: str, row_num: int | None = None, constraints: dict[str, str] | None = None
    ) -> table.Table:
        cache_path = self._obtain_cache_path(catalog_name, row_num, constraints)
        if not cache_path.exists():
            app.logger.debug("did not hit cache for the catalog, downloading")
            self._write_catalog_cache(catalog_name, row_num, constraints)

        return table.Table.read(cache_path, format="votable")

    def get_catalog_metadata(self, catalog: str) -> dict:
        return self._client.get_catalog_metadata(catalog=catalog)


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
        if len(constraints) % 2 != 0:
            raise ValueError("constraints must be provided in pairs (column, constraint_value)")
        self.constraints: dict[str, str] = {}
        for i in range(0, len(constraints), 2):
            self.constraints[constraints[i]] = constraints[i + 1]
        self.catalog_name = catalog_name
        self.table_name = table_name
        self.batch_size = batch_size
        self.client = CachedVizierClient(cache_path=cache_path)

    def prepare(self) -> None:
        pass

    def get_table_name(self) -> str:
        return _sanitize_filename(self.table_name)

    def get_bibcode(self) -> str:
        resp = self.client.get_catalog_metadata(catalog=self.catalog_name)
        return resp["origin_article"][0]

    def get_description(self) -> str:
        resp = self.client.get_catalog_metadata(catalog=self.catalog_name)
        return resp["title"][0]

    def get_schema(self) -> list[models.ColumnDescription]:
        t = self.client.get_table(self.table_name)
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
        t = self.client.get_table(self.table_name, constraints=constraints)

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
