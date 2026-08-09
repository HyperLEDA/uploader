from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from uploader.app.structured.designations.upload import upload_designations
from uploader.clients.gen.client import adminapi


def _mock_storage(total: int = 1) -> Mock:
    storage = Mock()
    storage.query.return_value = [{"cnt": total}]
    return storage


def _mock_client() -> Mock:
    return Mock(spec=adminapi.AuthenticatedClient)


@patch("uploader.app.structured.designations.upload.rawdata_batches")
@patch("uploader.app.structured.designations.upload.fetch_column_units")
def test_simple_column_expression(
    mock_fetch_column_units: Mock,
    mock_rawdata_batches: Mock,
) -> None:
    mock_fetch_column_units.return_value = ({"name"}, {"name": ""})
    mock_rawdata_batches.return_value = iter(
        [[{"hyperleda_internal_id": "1", "name": "NGC 123"}]],
    )

    total = upload_designations(
        _mock_storage(),
        "test_table",
        'col("name")',
        100,
        _mock_client(),
        report_func=lambda _: None,
    )

    assert total == 1
    mock_rawdata_batches.assert_called_once()
    assert mock_rawdata_batches.call_args.args[2] == ["name"]


@patch("uploader.app.structured.designations.upload.rawdata_batches")
@patch("uploader.app.structured.designations.upload.fetch_column_units")
def test_composed_string_expression(
    mock_fetch_column_units: Mock,
    mock_rawdata_batches: Mock,
) -> None:
    mock_fetch_column_units.return_value = ({"prefix", "number"}, {"prefix": "", "number": ""})
    mock_rawdata_batches.return_value = iter(
        [[{"hyperleda_internal_id": "1", "prefix": "NGC", "number": "123"}]],
    )

    total = upload_designations(
        _mock_storage(),
        "test_table",
        'col("prefix") + " " + col("number")',
        100,
        _mock_client(),
        report_func=lambda _: None,
    )

    assert total == 1


@patch("uploader.app.structured.designations.upload.fetch_column_units")
def test_missing_referenced_columns(mock_fetch_column_units: Mock) -> None:
    mock_fetch_column_units.return_value = ({"other"}, {})

    with pytest.raises(RuntimeError, match="has no column\\(s\\): \\['name'\\]"):
        upload_designations(
            _mock_storage(),
            "test_table",
            'col("name")',
            100,
            _mock_client(),
            report_func=lambda _: None,
        )


@patch("uploader.app.structured.designations.upload.rawdata_batches")
@patch("uploader.app.structured.designations.upload.fetch_column_units")
def test_null_referenced_values_counted_as_unmatched(
    mock_fetch_column_units: Mock,
    mock_rawdata_batches: Mock,
) -> None:
    mock_fetch_column_units.return_value = ({"name"}, {"name": ""})
    mock_rawdata_batches.return_value = iter(
        [[{"hyperleda_internal_id": "1", "name": None}]],
    )

    total = upload_designations(
        _mock_storage(),
        "test_table",
        'col("name")',
        100,
        _mock_client(),
        report_func=lambda _: None,
    )

    assert total == 1


@patch("uploader.app.structured.designations.upload.rawdata_batches")
@patch("uploader.app.structured.designations.upload.fetch_column_units")
def test_expression_evaluation_error_becomes_runtime_error(
    mock_fetch_column_units: Mock,
    mock_rawdata_batches: Mock,
) -> None:
    mock_fetch_column_units.return_value = ({"text_col", "num_col"}, {"text_col": "", "num_col": ""})
    mock_rawdata_batches.return_value = iter(
        [[{"hyperleda_internal_id": "1", "text_col": "NGC 123", "num_col": 1.5}]],
    )

    with pytest.raises(RuntimeError, match="failed to evaluate expression for row 1"):
        upload_designations(
            _mock_storage(),
            "test_table",
            'col("text_col") + col("num_col")',
            100,
            _mock_client(),
            report_func=lambda _: None,
        )


@patch("uploader.app.structured.designations.upload.rawdata_batches")
@patch("uploader.app.structured.designations.upload.fetch_column_units")
def test_output_file_writes_id_designation_pairs(
    mock_fetch_column_units: Mock,
    mock_rawdata_batches: Mock,
    tmp_path: Path,
) -> None:
    mock_fetch_column_units.return_value = ({"name"}, {"name": ""})
    mock_rawdata_batches.return_value = iter(
        [[{"hyperleda_internal_id": "id-1", "name": "NGC 123"}]],
    )
    output_file = tmp_path / "designations.csv"

    upload_designations(
        _mock_storage(),
        "test_table",
        'col("name")',
        100,
        _mock_client(),
        output_file=str(output_file),
        report_func=lambda _: None,
    )

    assert output_file.read_text().splitlines() == ["id,designation", "id-1,NGC 123"]
