import pytest
from unittest.mock import Mock
import hyperleda
from app.upload import upload
from plugins.csv_batched import CSVPlugin
from app.interface import UploaderPlugin
import pandas


class StubPlugin(UploaderPlugin):
    def __init__(self, should_raise: bool = False):
        self.should_raise = should_raise
        self.stop_called = False

    def prepare(self) -> None:
        pass

    def get_schema(self) -> list[hyperleda.ColumnDescription]:
        return []

    def get_data(self) -> tuple[pandas.DataFrame, float] | None:
        if self.should_raise:
            raise Exception("Test error")
        return None

    def stop(self) -> None:
        self.stop_called = True


@pytest.fixture
def mock_client() -> Mock:
    client = Mock(spec=hyperleda.HyperLedaClient)
    client.create_internal_source.return_value = "test_bibcode"
    client.create_table.return_value = "test_table_id"
    return client


def test_upload_with_csv_plugin(mock_client):
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
        table_type="regular",
    )

    mock_client.create_internal_source.assert_called_once_with(
        "Test Publication", ["Test Author"], 2024
    )
    mock_client.create_table.assert_called_once()
    mock_client.add_data.assert_called_once()


def test_plugin_stop_called_on_error(mock_client):
    plugin = StubPlugin(should_raise=True)

    # Call upload function
    upload(
        plugin=plugin,
        client=mock_client,
        table_name="test_table",
        table_description="Test table description",
        bibcode="",
        pub_name="Test Publication",
        pub_authors=["Test Author"],
        pub_year=2024,
        table_type="regular",
    )

    # Verify that stop was called despite the error
    assert plugin.stop_called
