from server.forms.structured_designation import (
    StructuredDesignationForm,
    handle_structured_designation,
)
from server.forms.structured_icrs import StructuredIcrsForm, handle_structured_icrs
from server.forms.structured_nature import StructuredNatureForm, handle_structured_nature
from server.forms.structured_redshift import StructuredRedshiftForm, handle_structured_redshift
from server.forms.upload import UploadRawForm, handle_upload_raw
from server.tasks import TaskDefinition, register_task


def register_all_tasks() -> None:
    register_task(
        TaskDefinition(
            id="upload",
            title="Upload via plugin",
            description="Upload a raw data table to HyperLEDA using a plugin.",
            form_model=UploadRawForm,
            handler=handle_upload_raw,
            group="Upload",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-designation",
            title="Designations",
            description="Upload object designations (names) from a raw table to the structured level.",
            form_model=StructuredDesignationForm,
            handler=handle_structured_designation,
            group="Upload structured",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-icrs",
            title="ICRS",
            description="Upload ICRS coordinates from a raw table to the structured level.",
            form_model=StructuredIcrsForm,
            handler=handle_structured_icrs,
            group="Upload structured",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-nature",
            title="Nature",
            description="Upload object nature/type from a raw table to the structured level.",
            form_model=StructuredNatureForm,
            handler=handle_structured_nature,
            group="Upload structured",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-redshift",
            title="Redshift",
            description="Upload redshift (cz) from a raw z column to the structured level.",
            form_model=StructuredRedshiftForm,
            handler=handle_structured_redshift,
            group="Upload structured",
        ),
    )
