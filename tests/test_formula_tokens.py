from uploader.app.lib.formula import expression_json_schema_extra
from uploader.forms.structured_designation import StructuredDesignationForm


def test_designation_form_includes_expression_tokens() -> None:
    schema = StructuredDesignationForm.model_json_schema()
    properties = schema["properties"]
    assert isinstance(properties, dict)
    expression = properties["expression"]
    assert isinstance(expression, dict)
    extra = expression_json_schema_extra()
    options = expression["ui:options"]
    extra_options = extra["ui:options"]
    assert isinstance(options, dict)
    assert isinstance(extra_options, dict)
    assert options["tokens"] == extra_options["tokens"]
