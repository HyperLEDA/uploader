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
            title="Upload raw table",
            description="Upload a raw data table to HyperLEDA using a plugin.",
            form_model=UploadRawForm,
            handler=handle_upload_raw,
            group="Raw tables",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-designation",
            title="Designations",
            description="Convert designations to common format and upload them to the database.",
            form_model=StructuredDesignationForm,
            handler=handle_structured_designation,
            group="Catalogs",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-icrs",
            title="ICRS",
            description="Upload ICRS coordinates to the database.",
            form_model=StructuredIcrsForm,
            handler=handle_structured_icrs,
            group="Catalogs",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-nature",
            title="Nature",
            description="Upload object type to the database.",
            form_model=StructuredNatureForm,
            handler=handle_structured_nature,
            group="Catalogs",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-redshift",
            title="Redshift",
            description="Upload redshift to the database.",
            form_model=StructuredRedshiftForm,
            handler=handle_structured_redshift,
            group="Catalogs",
        ),
    )
