import http
import itertools
import pathlib
from typing import Generator, final

import astropy
import hyperleda
import pandas
import requests
import app
from astroquery import vizier
from astropy.io import votable
from astropy.io.votable import tree

VIZIER_URL = "https://vizier.cds.unistra.fr/viz-bin/votable/-tsv"


@final
class VizierPlugin(
    app.UploaderPlugin,
    app.DefaultTableNamer,
    app.BibcodeProvider,
    app.DescriptionProvider,
):
    def __init__(
        self,
        catalog_name: str,
        table_name: str,
        cache_path: str = ".vizier_cache/",
        batch_size: int = 500,
    ):
        self.cache_path = cache_path
        self.catalog_name = catalog_name
        self.table_name = table_name
        self.batch_size = batch_size

    def _write_table_cache(self, catalog_name: str, table_name: str):
        app.logger.info(
            "downloading table from Vizier",
            catalog_name=catalog_name,
            table_name=table_name,
        )

        vizier_client = vizier.VizierClass(row_limit=-1)
        catalogs = vizier_client.get_catalogs(catalog_name)  # type: ignore

        table = next((cat for cat in catalogs if cat.meta["name"] == table_name), None)
        if not table:
            raise ValueError("table not found in the catalog")

        cache_filename = self._obtain_cache_path("tables", catalog_name, table_name)
        table.write(cache_filename, format="votable")

        app.logger.debug("wrote table cache", location=str(cache_filename))

    def _write_schema_cache(self, catalog_name: str, table_name: str):
        vizier_client = vizier.VizierClass(row_limit=5)
        columns = _get_columns(vizier_client, catalog_name, table_name)
        raw_header = _download_table(table_name, columns, max_rows=10)

        cache_filename = self._obtain_cache_path("schemas", catalog_name, table_name)
        cache_filename.write_text(raw_header)

        app.logger.debug("wrote cache", location=str(cache_filename))

    def _obtain_cache_path(
        self, type_path: str, catalog_name: str, table_name: str
    ) -> pathlib.Path:
        filename = f"{_get_filename(catalog_name, table_name)}.vot"
        path = pathlib.Path(self.cache_path) / type_path / filename

        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _get_schema_from_cache(
        self, catalog_name: str, table_name: str
    ) -> tree.VOTableFile:
        cache_filename = self._obtain_cache_path("schemas", catalog_name, table_name)
        return votable.parse(str(cache_filename))

    def _get_table_from_cache(
        self, catalog_name: str, table_name: str
    ) -> astropy.table.Table:
        cache_filename = self._obtain_cache_path("tables", catalog_name, table_name)
        return astropy.table.Table.read(cache_filename, format="votable")

    def prepare(self) -> None:
        pass

    def get_schema(self) -> list[hyperleda.ColumnDescription]:
        if not self._obtain_cache_path(
            "schemas", self.catalog_name, self.table_name
        ).exists():
            app.logger.debug("did not hit cache for the schema, downloading")
            self._write_schema_cache(self.catalog_name, self.table_name)

        schema = self._get_schema_from_cache(self.catalog_name, self.table_name)

        table = schema.get_first_table()
        return [
            hyperleda.ColumnDescription(
                name=field.ID,
                data_type=field.datatype,
                ucd=field.ucd,
                description=field.description,
                unit=field.unit,
            )
            for field in table.fields
        ]

    def get_data(self) -> Generator[tuple[pandas.DataFrame, float], None, None]:
        if not self._obtain_cache_path(
            "tables", self.catalog_name, self.table_name
        ).exists():
            app.logger.debug("did not hit cache for the table, downloading")
            self._write_table_cache(self.catalog_name, self.table_name)

        table = self._get_table_from_cache(self.catalog_name, self.table_name)

        total_rows = len(table)
        app.logger.info("uploading table", total_rows=total_rows)

        table_rows = list(table)
        offset = 0
        for batch in itertools.batched(table_rows, self.batch_size):
            offset += len(batch)

            rows = []
            for row in batch:
                row_dict = {k: v for k, v in dict(row).items() if v != "--"}
                rows.append(row_dict)

            yield pandas.DataFrame(rows), offset / total_rows

    def stop(self) -> None:
        pass

    def get_table_name(self) -> str:
        return _get_filename(self.catalog_name, self.table_name)

    def get_bibcode(self) -> str:
        self.get_schema()
        schema = self._get_schema_from_cache(self.catalog_name, self.table_name)
        bibcode_info = next(
            filter(lambda info: info.name == "cites", schema.resources[0].infos)
        )
        return bibcode_info.value.split(":")[1]

    def get_description(self) -> str:
        self.get_schema()
        schema = self._get_schema_from_cache(self.catalog_name, self.table_name)
        return schema.resources[0].description


def _sanitize_filename(string: str) -> str:
    return string.replace("/", "_")


def _get_filename(catalog_name: str, table_name: str) -> str:
    return f"{_sanitize_filename(catalog_name)}_{_sanitize_filename(table_name)}"


def _get_columns(
    client: vizier.VizierClass, catalog_name: str, table_name: str
) -> list[str]:
    catalogs = client.get_catalogs(catalog_name)  # type: ignore

    meta = None
    for cat in catalogs:
        if cat.meta["name"] == table_name:
            meta = cat
            break

    if not meta:
        raise ValueError("table not found in the catalog")

    return meta.colnames


def _download_table(
    table_name: str, columns: list[str], max_rows: int | None = None
) -> str:
    out_max = "unlimited" if max_rows is None else max_rows

    payload = [
        "-oc.form=dec",
        f"-out.max={out_max}",
        "-sort=_r",
        "-order=I",
        f"-out.src={table_name}",
        "-c.eq=J2000",
        "-c.r=++2",
        "-c.u=arcmin",
        "-c.geom=r",
        f"-source={table_name}",
    ]

    columns = [f"-out={column}" for column in columns]

    data = "&".join(payload + columns)

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.request(
        http.HTTPMethod.POST, VIZIER_URL, data=data, headers=headers
    )

    return response.text


plugin = VizierPlugin
name = "vizier"
