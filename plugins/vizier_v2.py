import itertools
import pathlib
from collections.abc import Generator
from typing import final

import pandas
from astropy.io import votable
from astropy.io.votable import tree
from astroquery import utils, vizier

import app
from app.gen.client.adminapi import models, types


def _sanitize_filename(string: str) -> str:
    return string.replace("/", "_")


def _map_votable_datatype(datatype: str) -> models.DatatypeEnum:
    datatype_lower = datatype.lower() if datatype else ""
    if datatype_lower in ("char", "unicodechar", "string", "text"):
        return models.DatatypeEnum.STRING
    if datatype_lower in ("short", "int", "long", "integer", "smallint"):
        return models.DatatypeEnum.INTEGER
    if datatype_lower in ("float", "double", "real", "doubleprecision"):
        return models.DatatypeEnum.DOUBLE
    return models.DatatypeEnum.STRING


class CachedVizierClient:
    def __init__(self, cache_path: str = ".vizier_cache/"):
        self.cache_path = cache_path
        self._client = vizier.Vizier()

    def _obtain_cache_path(self, catalog_name: str, row_num: int | None = None) -> pathlib.Path:
        filename = f"{_sanitize_filename(catalog_name)}.vot"
        if row_num is not None:
            filename = f"{_sanitize_filename(catalog_name)}_rows_{row_num}.vot"
        path = pathlib.Path(self.cache_path) / "catalogs" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _write_catalog_cache(self, catalog_name: str, row_num: int | None = None) -> None:
        app.logger.info(
            "downloading catalog from Vizier",
            catalog_name=catalog_name,
            row_num=row_num,
        )
        client = self._client
        if row_num is not None:
            client = vizier.Vizier(row_limit=row_num)
        catalogs: utils.TableList = client.get_catalogs(catalog_name)  # pyright: ignore[reportAttributeAccessIssue]

        if not catalogs:
            raise ValueError("catalog not found")

        cache_filename = self._obtain_cache_path(catalog_name, row_num)
        catalogs[0].write(str(cache_filename), format="votable")
        app.logger.debug("wrote catalog cache", location=str(cache_filename))

    def get_table(self, catalog_name: str, row_num: int | None = None) -> tree.TableElement:
        cache_path = self._obtain_cache_path(catalog_name, row_num)
        if not cache_path.exists():
            app.logger.debug("did not hit cache for the catalog, downloading")
            self._write_catalog_cache(catalog_name, row_num)

        return votable.parse(str(cache_path)).get_first_table()

    def get_catalog_metadata(self, catalog: str) -> dict:
        return self._client.get_catalog_metadata(catalog=catalog)


@final
class VizierV2Plugin(
    app.UploaderPlugin,
    app.DefaultTableNamer,
    app.BibcodeProvider,
    app.DescriptionProvider,
):
    def __init__(self, cache_path: str = ".vizier_cache/", batch_size: int = 500):
        self.catalog_name = "J/ApJ/788/39"
        self.table_name = "J/ApJ/788/39/stars"
        self.batch_size = batch_size
        self.client = CachedVizierClient(cache_path=cache_path)

    def prepare(self) -> None:
        pass

    def get_table_name(self) -> str:
        t = self.client.get_table(self.table_name, row_num=1)
        return str(t.ID)

    def get_bibcode(self) -> str:
        resp = self.client.get_catalog_metadata(catalog=self.catalog_name)
        return resp["origin_article"][0]

    def get_description(self) -> str:
        resp = self.client.get_catalog_metadata(catalog=self.catalog_name)
        return resp["title"][0]

    def get_schema(self) -> list[models.ColumnDescription]:
        t = self.client.get_table(self.table_name)
        return [
            models.ColumnDescription(
                name=field.ID,
                data_type=_map_votable_datatype(str(field.datatype)),
                ucd=field.ucd,
                description=field.description,
                unit=str(field.unit) if field.unit else types.UNSET,
            )
            for field in t.fields
        ]

    def get_data(self) -> Generator[tuple[pandas.DataFrame, float]]:
        t = self.client.get_table(self.table_name)

        total_rows = int(t.nrows)  # pyright: ignore[reportArgumentType]
        app.logger.info("uploading table", total_rows=total_rows)

        table_rows = list(t.array)
        offset = 0
        for batch in itertools.batched(table_rows, self.batch_size, strict=False):
            offset += len(batch)

            rows = []
            for row in batch:
                row_dict = {k.ID: v for k, v in zip(t.fields, row, strict=False) if v != "--"}
                rows.append(row_dict)

            yield pandas.DataFrame(rows), offset / total_rows

    def stop(self) -> None:
        pass


plugin = VizierV2Plugin
name = "vizier-v2"
