import itertools
import pathlib
from collections.abc import Generator
from typing import final

import pandas
from astropy import table
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
        self.cache_path = cache_path
        self.batch_size = batch_size
        self.client = vizier.Vizier()

    def _obtain_cache_path(self, catalog_name: str) -> pathlib.Path:
        filename = f"{_sanitize_filename(catalog_name)}.vot"
        path = pathlib.Path(self.cache_path) / "catalogs" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _write_catalog_cache(self, catalog_name: str) -> None:
        app.logger.info(
            "downloading catalog from Vizier",
            catalog_name=catalog_name,
        )
        catalogs: utils.TableList = self.client.get_catalogs(catalog_name)  # pyright: ignore[reportAttributeAccessIssue]

        if not catalogs:
            raise ValueError("catalog not found")
        cache_filename = self._obtain_cache_path(catalog_name)
        catalogs[0].write(str(cache_filename), format="votable")
        app.logger.debug("wrote catalog cache", location=str(cache_filename))

    def _get_catalog_from_cache(self, catalog_name: str) -> table.Table:
        cache_filename = self._obtain_cache_path(catalog_name)
        return table.Table.read(str(cache_filename), format="votable")

    def _get_votable_from_cache(self, catalog_name: str) -> tree.VOTableFile:
        cache_filename = self._obtain_cache_path(catalog_name)
        return votable.parse(str(cache_filename))

    def prepare(self) -> None:
        pass

    def get_table_name(self) -> str:
        if not self._obtain_cache_path(self.table_name).exists():
            app.logger.debug("did not hit cache for the catalog, downloading")
            self._write_catalog_cache(self.table_name)

        t = self._get_catalog_from_cache(self.table_name)

        if hasattr(t, "meta") and t.meta is not None:
            return _sanitize_filename(t.meta["name"])

        raise RuntimeError("Unable to get table name")

    def get_bibcode(self) -> str:
        resp = self.client.get_catalog_metadata(catalog=self.catalog_name)
        return resp["origin_article"][0]

    def get_description(self) -> str:
        resp = self.client.get_catalog_metadata(catalog=self.catalog_name)
        return resp["title"][0]

    def get_schema(self) -> list[models.ColumnDescription]:
        if not self._obtain_cache_path(self.table_name).exists():
            app.logger.debug("did not hit cache for the catalog, downloading")
            self._write_catalog_cache(self.table_name)

        schema = self._get_votable_from_cache(self.table_name)
        table = schema.get_first_table()
        return [
            models.ColumnDescription(
                name=field.ID,
                data_type=_map_votable_datatype(str(field.datatype)),
                ucd=field.ucd,
                description=field.description,
                unit=str(field.unit) if field.unit else types.UNSET,
            )
            for field in table.fields
        ]

    def get_data(self) -> Generator[tuple[pandas.DataFrame, float]]:
        if not self._obtain_cache_path(self.table_name).exists():
            app.logger.debug("did not hit cache for the catalog, downloading")
            self._write_catalog_cache(self.table_name)

        table = self._get_catalog_from_cache(self.table_name)

        total_rows = len(table)
        app.logger.info("uploading table", total_rows=total_rows)

        table_rows = list(table)  # pyright: ignore[reportArgumentType]
        offset = 0
        for batch in itertools.batched(table_rows, self.batch_size, strict=False):
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
