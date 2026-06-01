import http
from collections.abc import Generator
from typing import Any
from unittest.mock import Mock, patch

import pandas
import pytest

from uploader.app.interface import UploaderSource
from uploader.app.sources.csv import CSVSource
from uploader.app.upload import upload
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi import models, types


class StubPlugin(UploaderSource):
    def __init__(self, should_raise: bool = False):
        self.should_raise = should_raise
        self.stop_called = False

    def prepare(self) -> None:
        pass

    def get_schema(self) -> list[models.ColumnDescription]:
        return []

    def get_data(self) -> Generator[tuple[pandas.DataFrame, int, int]]:
        if self.should_raise:
            raise Exception("Test error")
        return None

    def get_total_rows(self) -> int:
        return 0

    def stop(self) -> None:
        self.stop_called = True


@pytest.fixture
def mock_client() -> Mock:
    return Mock(spec=adminapi.AuthenticatedClient)


def mock_response[T: Any](resp: T) -> types.Response[T]:
    return types.Response(
        status_code=http.HTTPStatus.OK,
        content=b"",
        headers={},
        parsed=resp,
    )


@patch("uploader.app.upload.create_source")
@patch("uploader.app.upload.create_table")
@patch("uploader.app.upload.add_data")
def test_upload_with_csv_plugin(mock_add_data, mock_create_table, mock_create_source, mock_client):
    mock_create_source_response = models.APIOkResponseCreateSourceResponse(
        data=models.CreateSourceResponse(code="test_bibcode")
    )
    mock_create_source.sync.return_value = mock_create_source_response

    mock_create_table_response = models.APIOkResponseCreateTableResponse(data=models.CreateTableResponse(id=1))
    mock_create_table.sync_detailed.return_value = mock_response(mock_create_table_response)

    mock_add_data_response = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())
    mock_add_data.sync_detailed.return_value = mock_response(mock_add_data_response)

    plugin = CSVSource("tests/test_csv.csv")

    upload(
        plugin=plugin,
        client=mock_client,
        table_name="test_table",
        table_description="Test table description",
        bibcode="",
        pub_name="Test Publication",
        pub_authors=["Test Author"],
        pub_year=2024,
        table_type="REGULAR",
        report_func=lambda _: None,
    )

    mock_create_source.sync_detailed.assert_called_once()
    mock_create_table.sync_detailed.assert_called_once()
    mock_add_data.sync_detailed.assert_called_once()


@patch("uploader.app.upload.create_source")
@patch("uploader.app.upload.create_table")
def test_plugin_stop_called_on_error(mock_create_table, mock_create_source, mock_client):
    mock_create_source_response = models.APIOkResponseCreateSourceResponse(
        data=models.CreateSourceResponse(code="test_bibcode")
    )
    mock_create_source.sync_detailed.return_value = mock_response(mock_create_source_response)

    mock_create_table_response = models.APIOkResponseCreateTableResponse(data=models.CreateTableResponse(id=1))
    mock_create_table.sync_detailed.return_value = mock_response(mock_create_table_response)

    plugin = StubPlugin(should_raise=True)

    upload(
        plugin=plugin,
        client=mock_client,
        table_name="test_table",
        table_description="Test table description",
        bibcode="",
        pub_name="Test Publication",
        pub_authors=["Test Author"],
        pub_year=2024,
        table_type="REGULAR",
        report_func=lambda _: None,
    )

    assert plugin.stop_called
