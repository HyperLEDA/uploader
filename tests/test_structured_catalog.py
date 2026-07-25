from unittest.mock import Mock, patch

import pytest

from uploader.app.lib.formula import expression_syntax_help
from uploader.app.structured.generic.upload import is_numeric_datatype, upload_catalog_columns
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.models.catalog_field import CatalogField
from uploader.clients.gen.client.adminapi.models.catalog_schema import CatalogSchema
from uploader.clients.gen.client.adminapi.models.datatype_enum import DatatypeEnum
from uploader.forms.structured_catalog import _task_description, build_catalog_form


def _mock_storage(total: int = 1) -> Mock:
    storage = Mock()
    storage.query.return_value = [{"cnt": total}]
    return storage


def _mock_client() -> Mock:
    return Mock(spec=adminapi.AuthenticatedClient)


def _sample_schema() -> CatalogSchema:
    return CatalogSchema(
        catalog="demo",
        title="Demo catalog",
        description="Demo structured catalog.",
        fields=[
            CatalogField(name="label", data_type=DatatypeEnum.STRING, description="Object label"),
            CatalogField(name="mag", data_type=DatatypeEnum.FLOAT, unit="mag", description="Magnitude"),
            CatalogField(name="n", data_type=DatatypeEnum.INTEGER, required=False),
        ],
    )


def test_is_numeric_datatype() -> None:
    assert is_numeric_datatype(DatatypeEnum.FLOAT)
    assert is_numeric_datatype(DatatypeEnum.INTEGER)
    assert not is_numeric_datatype(DatatypeEnum.STRING)
    assert not is_numeric_datatype(DatatypeEnum.TIMESTAMP_WITHOUT_TIME_ZONE)


def test_build_catalog_form_numeric_fields_are_expressions() -> None:
    form_model = build_catalog_form(_sample_schema())
    fields = form_model.model_fields

    assert fields["label"].title == "label column"
    assert fields["label"].description == "Object label"

    assert fields["mag"].title == "mag"
    assert fields["mag"].description == "Expression. Magnitude"

    assert fields["n"].title == "n"
    assert fields["n"].description == "Expression. Value for n."
    assert fields["n"].is_required() is False


def test_task_description_includes_expression_help() -> None:
    description = _task_description(_sample_schema())
    assert "Demo structured catalog." in description
    assert expression_syntax_help() in description


@patch("uploader.app.structured.generic.upload.handle_call")
@patch("uploader.app.structured.generic.upload.save_structured_data.sync_detailed")
@patch("uploader.app.structured.generic.upload.rawdata_batches")
@patch("uploader.app.structured.generic.upload._fetch_column_units")
def test_upload_evaluates_numeric_expressions(
    mock_fetch_column_units: Mock,
    mock_rawdata_batches: Mock,
    mock_sync_detailed: Mock,
    mock_handle_call: Mock,
) -> None:
    mock_fetch_column_units.return_value = (
        {"name", "e_ra", "count"},
        {"name": "", "e_ra": "arcsec", "count": ""},
    )
    mock_rawdata_batches.return_value = iter(
        [
            [
                {
                    "hyperleda_internal_id": "000079ce-5ffd-82c6-3f75-3a083f0fde80",
                    "name": "NGC 1",
                    "e_ra": 0.5,
                    "count": 3,
                },
            ],
        ],
    )

    total = upload_catalog_columns(
        _mock_storage(),
        "test_table",
        "demo",
        column_map={"label": "name"},
        expressions={
            "mag": 'col("e_ra")',
            "n": 'col("count") * 2',
        },
        field_types={
            "label": DatatypeEnum.STRING,
            "mag": DatatypeEnum.FLOAT,
            "n": DatatypeEnum.INTEGER,
        },
        field_units={"mag": "arcsec"},
        columns=["label", "mag", "n"],
        batch_size=100,
        client=_mock_client(),
        write=True,
        report_func=lambda _: None,
    )

    assert total == 1
    mock_rawdata_batches.assert_called_once()
    fetch_columns = mock_rawdata_batches.call_args.args[2]
    assert set(fetch_columns) == {"name", "e_ra", "count"}
    body = mock_sync_detailed.call_args.kwargs["body"]
    assert body.catalog == "demo"
    assert body.columns == ["label", "mag", "n"]
    assert body.ids == ["000079ce-5ffd-82c6-3f75-3a083f0fde80"]
    assert body.data == [["NGC 1", 0.5, 6]]


@patch("uploader.app.structured.generic.upload.rawdata_batches")
@patch("uploader.app.structured.generic.upload._fetch_column_units")
def test_upload_unit_conversion_error_includes_field_details(
    mock_fetch_column_units: Mock,
    mock_rawdata_batches: Mock,
) -> None:
    mock_fetch_column_units.return_value = (
        {"bt"},
        {"bt": ""},
    )
    mock_rawdata_batches.return_value = iter(
        [
            [
                {
                    "hyperleda_internal_id": "000079ce-5ffd-82c6-3f75-3a083f0fde80",
                    "bt": 12.5,
                },
            ],
        ],
    )

    with pytest.raises(RuntimeError, match="failed to evaluate expressions for row") as exc_info:
        upload_catalog_columns(
            _mock_storage(),
            "test_table",
            "demo",
            column_map={},
            expressions={"mag": 'col("bt")'},
            field_types={"mag": DatatypeEnum.FLOAT},
            field_units={"mag": "arcsec"},
            columns=["mag"],
            batch_size=100,
            client=_mock_client(),
            write=False,
            report_func=lambda _: None,
        )

    message = str(exc_info.value)
    assert "mag" in message
    assert 'col("bt")' in message
    assert "arcsec" in message
