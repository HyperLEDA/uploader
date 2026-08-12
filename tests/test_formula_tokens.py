from uploader.app.lib.formula import expression_json_schema_extra, expression_tokens
from uploader.forms.structured_designation import StructuredDesignationForm


def test_expression_tokens_include_language_names() -> None:
    labels = {token["label"] for token in expression_tokens()}
    assert labels >= {"col", "sin", "cos", "str", "to_deg", "pi", "deg", "arcsec", "mag"}


def test_designation_form_marks_expression_widget() -> None:
    schema = StructuredDesignationForm.model_json_schema()
    properties = schema["properties"]
    assert isinstance(properties, dict)
    expression = properties["expression"]
    assert isinstance(expression, dict)
    extra = expression_json_schema_extra()
    assert expression["ui:widget"] == extra["ui:widget"]
    options = expression["ui:options"]
    extra_options = extra["ui:options"]
    assert isinstance(options, dict)
    assert isinstance(extra_options, dict)
    assert options["tokens"] == extra_options["tokens"]
