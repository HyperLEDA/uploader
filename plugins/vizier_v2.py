import pathlib
from collections.abc import Generator
from typing import final

import pandas
from astropy import table
from astropy.io import votable
from astropy.io.votable import tree
from astroquery import utils, vizier

import app
from app.gen.client.adminapi import models


def _sanitize_filename(string: str) -> str:
    return string.replace("/", "_")


@final
class VizierV2Plugin(
    app.UploaderPlugin,
    app.DefaultTableNamer,
    app.DescriptionProvider,
):
    def __init__(self, cache_path: str = ".vizier_cache/"):
        self.catalog_name = "J/ApJ/788/39/stars"
        self.cache_path = cache_path
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
        if not self._obtain_cache_path(self.catalog_name).exists():
            app.logger.debug("did not hit cache for the catalog, downloading")
            self._write_catalog_cache(self.catalog_name)

        t = self._get_catalog_from_cache(self.catalog_name)

        if hasattr(t, "meta") and t.meta is not None:
            return _sanitize_filename(t.meta["name"])

        raise RuntimeError("Unable to get table name")

    def get_description(self) -> str:
        if not self._obtain_cache_path(self.catalog_name).exists():
            app.logger.debug("did not hit cache for the catalog, downloading")
            self._write_catalog_cache(self.catalog_name)

        t = self._get_catalog_from_cache(self.catalog_name)
        if hasattr(t, "meta") and t.meta is not None:
            return str(t.meta["description"])

        raise RuntimeError("Unable to get table description")

    def get_schema(self) -> list[models.ColumnDescription]:
        stars: utils.TableList = self.client.get_catalogs("J/ApJ/788/39/stars")  # pyright: ignore[reportAttributeAccessIssue]

        print(stars)

        return []

    def get_data(self) -> Generator[tuple[pandas.DataFrame, float]]:
        yield pandas.DataFrame(), 1.0

    def stop(self) -> None:
        pass


plugin = VizierV2Plugin
name = "vizier-v2"
