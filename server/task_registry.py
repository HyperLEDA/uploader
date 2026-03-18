from server.forms.structured_designation import (
    StructuredDesignationForm,
    handle_structured_designation,
)
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
