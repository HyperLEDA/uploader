from unittest.mock import Mock, patch

import pandas
import pytest

from app.gen.client import adminapi
from app.gen.client.adminapi import models
from app.interface import UploaderPlugin
from app.upload import upload
from plugins.csv_batched import CSVPlugin


class StubPlugin(UploaderPlugin):
    def __init__(self, should_raise: bool = False):
        self.should_raise = should_raise
        self.stop_called = False

    def prepare(self) -> None:
        pass

    def get_schema(self) -> list[models.ColumnDescription]:
        return []

    def get_data(self) -> tuple[pandas.DataFrame, float] | None:
        if self.should_raise:
            raise Exception("Test error")
        return None

    def stop(self) -> None:
        self.stop_called = True


@pytest.fixture
def mock_client() -> Mock:
    return Mock(spec=adminapi.AuthenticatedClient)


@patch("app.upload.create_source")
@patch("app.upload.create_table")
@patch("app.upload.add_data")
def test_upload_with_csv_plugin(mock_add_data, mock_create_table, mock_create_source, mock_client):
    mock_create_source_response = models.APIOkResponseCreateSourceResponse(
        data=models.CreateSourceResponse(code="test_bibcode")
    )
    mock_create_source.sync.return_value = mock_create_source_response

    mock_create_table_response = models.APIOkResponseCreateTableResponse(data=models.CreateTableResponse(id=1))
    mock_create_table.sync.return_value = mock_create_table_response

    mock_add_data_response = models.APIOkResponseAddDataResponse(data=models.AddDataResponse())
    mock_add_data.sync.return_value = mock_add_data_response

    plugin = CSVPlugin("tests/test_csv.csv")

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
    )

    mock_create_source.sync.assert_called_once()
    mock_create_table.sync.assert_called_once()
    mock_add_data.sync.assert_called_once()


@patch("app.upload.create_source")
@patch("app.upload.create_table")
@patch("app.upload.add_data")
def test_plugin_stop_called_on_error(mock_add_data, mock_create_table, mock_create_source, mock_client):
    mock_create_source_response = models.APIOkResponseCreateSourceResponse(
        data=models.CreateSourceResponse(code="test_bibcode")
    )
    mock_create_source.sync.return_value = mock_create_source_response

    mock_create_table_response = models.APIOkResponseCreateTableResponse(data=models.CreateTableResponse(id=1))
    mock_create_table.sync.return_value = mock_create_table_response

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
    )

    assert plugin.stop_called
