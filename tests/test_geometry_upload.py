from unittest.mock import Mock, patch

import pytest

from uploader.app.structured.geometry.upload import upload_geometry_isophotal
from uploader.clients.gen.client import adminapi


def _mock_storage(total: int = 1) -> Mock:
    storage = Mock()
    storage.query.return_value = [{"cnt": total}]
    return storage


def _mock_client() -> Mock:
    return Mock(spec=adminapi.AuthenticatedClient)


def _base_expressions() -> dict[str, str]:
    return {
        "a": '3 * 10 ** col("logd25") * arcsec',
        "e_a": '3 * 10 ** col("logd25") * 2.302585093 * e_logd25 * arcsec',
        "b": '3 * 10 ** (col("logd25") - col("logr25")) * arcsec',
        "e_b": (
            '3 * 10 ** (col("logd25") - col("logr25")) * 2.302585093 '
            '* (col("e_logd25") ** 2 + col("e_logr25") ** 2) ** 0.5 * arcsec'
        ),
        "isophote": 'col("bri25")',
    }


@patch("uploader.app.structured.geometry.upload.rawdata_batches")
@patch("uploader.app.structured.geometry.upload._fetch_column_units")
def test_isophote_unit_conversion_error_includes_field_details(
    mock_fetch_column_units: Mock,
    mock_rawdata_batches: Mock,
) -> None:
    mock_fetch_column_units.return_value = (
        {"logd25", "logr25", "e_logd25", "e_logr25", "bri25"},
        {
            "logd25": "",
            "logr25": "",
            "e_logd25": "",
            "e_logr25": "",
            "bri25": "",
        },
    )
    mock_rawdata_batches.return_value = iter(
        [
            [
                {
                    "hyperleda_internal_id": "000079ce-5ffd-82c6-3f75-3a083f0fde80",
                    "logd25": 1.5,
                    "logr25": 0.3,
                    "e_logd25": 0.05,
                    "e_logr25": 0.04,
                    "bri25": 25.0,
                },
            ],
        ],
    )

    with pytest.raises(RuntimeError, match="failed to evaluate expressions for row") as exc_info:
        upload_geometry_isophotal(
            _mock_storage(),
            "test_table",
            "B",
            _base_expressions(),
            100,
            _mock_client(),
            report_func=lambda _: None,
        )

    message = str(exc_info.value)
    assert "isophote" in message
    assert 'col("bri25")' in message
    assert "mag/arcmin2" in message
    assert "columns: bri25=''" in message


@patch("uploader.app.structured.geometry.upload.rawdata_batches")
@patch("uploader.app.structured.geometry.upload._fetch_column_units")
def test_constant_isophote_unit_error_omits_empty_columns(
    mock_fetch_column_units: Mock,
    mock_rawdata_batches: Mock,
) -> None:
    mock_fetch_column_units.return_value = (
        {"logd25", "logr25", "e_logd25", "e_logr25"},
        {
            "logd25": "",
            "logr25": "",
            "e_logd25": "",
            "e_logr25": "",
        },
    )
    mock_rawdata_batches.return_value = iter(
        [
            [
                {
                    "hyperleda_internal_id": "000079ce-5ffd-82c6-3f75-3a083f0fde80",
                    "logd25": 1.5,
                    "logr25": 0.3,
                    "e_logd25": 0.05,
                    "e_logr25": 0.04,
                },
            ],
        ],
    )
    expressions = _base_expressions()
    expressions["isophote"] = "22"

    with pytest.raises(RuntimeError, match="failed to evaluate expressions for row") as exc_info:
        upload_geometry_isophotal(
            _mock_storage(),
            "test_table",
            "B",
            expressions,
            100,
            _mock_client(),
            report_func=lambda _: None,
        )

    message = str(exc_info.value)
    assert "isophote" in message
    assert "('22')" in message
    assert "from dimensionless to mag/arcmin2" in message
    assert "columns:" not in message
