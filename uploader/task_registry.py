from uploader.forms.authenticate import AuthenticateForm, handle_authenticate
from uploader.forms.crossmatch_default import CrossmatchDefaultForm, handle_crossmatch_default
from uploader.forms.crossmatch_layered import CrossmatchLayeredForm, handle_crossmatch_layered
from uploader.forms.crossmatch_two_radii import CrossmatchTwoRadiiForm, handle_crossmatch_two_radii
from uploader.forms.structured_designation import (
    StructuredDesignationForm,
    handle_structured_designation,
)
from uploader.forms.structured_geometry_isophotal import (
    StructuredGeometryIsophotalForm,
    handle_structured_geometry_isophotal,
)
from uploader.forms.structured_icrs import StructuredIcrsForm, handle_structured_icrs
from uploader.forms.structured_nature import StructuredNatureForm, handle_structured_nature
from uploader.forms.structured_note import StructuredNoteForm, handle_structured_note
from uploader.forms.structured_photometry_hyperleda import (
    StructuredPhotometryHyperledaForm,
    handle_structured_photometry_hyperleda,
)
from uploader.forms.structured_redshift import StructuredRedshiftForm, handle_structured_redshift
from uploader.forms.upload_csv import UploadCsvForm, handle_upload_csv
from uploader.forms.upload_fits import UploadFitsForm, handle_upload_fits
from uploader.forms.upload_vizier import UploadVizierForm, handle_upload_vizier
from uploader.tasks import TaskDefinition, register_task


def register_all_tasks() -> None:
    register_task(
        TaskDefinition(
            id="authenticate",
            title="Authenticate",
            description="Save database credentials for use by other tasks.",
            form_model=AuthenticateForm,
            handler=handle_authenticate,
            group="Settings",
            rerunnable=False,
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-csv",
            title="Upload from CSV",
            description="Upload a raw table from a CSV file.",
            form_model=UploadCsvForm,
            handler=handle_upload_csv,
            group="Raw tables",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-fits",
            title="Upload from FITS",
            description="Upload a raw table from a FITS file.",
            form_model=UploadFitsForm,
            handler=handle_upload_fits,
            group="Raw tables",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-vizier",
            title="Upload from Vizier",
            description="Upload a raw table from a VizieR catalog.",
            form_model=UploadVizierForm,
            handler=handle_upload_vizier,
            group="Raw tables",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-note",
            title="Note",
            description="Add free text note about a record.",
            form_model=StructuredNoteForm,
            handler=handle_structured_note,
            group="Catalogs",
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
    register_task(
        TaskDefinition(
            id="upload-structured-photometry-hyperleda",
            title="Photometry (HyperLEDA)",
            description="Upload U/B/V/I/K asymptotic magnitudes to photometry catalog.",
            form_model=StructuredPhotometryHyperledaForm,
            handler=handle_structured_photometry_hyperleda,
            group="Catalogs",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-structured-geometry-isophotal",
            title="Isophotal geometry",
            description="Upload isophotal ellipse geometry (a, b, pa, isophote) from rawdata columns via expressions.",
            form_model=StructuredGeometryIsophotalForm,
            handler=handle_structured_geometry_isophotal,
            group="Catalogs",
        ),
    )
    register_task(
        TaskDefinition(
            id="crossmatch-default",
            title="Crossmatch (default)",
            description="Cross-identify objects using a single search radius.",
            form_model=CrossmatchDefaultForm,
            handler=handle_crossmatch_default,
            group="Crossmatch",
        ),
    )
    register_task(
        TaskDefinition(
            id="crossmatch-two-radii",
            title="Crossmatch (two-radii)",
            description="Cross-identify objects using inner and outer search radii.",
            form_model=CrossmatchTwoRadiiForm,
            handler=handle_crossmatch_two_radii,
            group="Crossmatch",
        ),
    )
    register_task(
        TaskDefinition(
            id="crossmatch-layered",
            title="Crossmatch (layered)",
            description="Cross-identify objects using layered ICRS then name resolution.",
            form_model=CrossmatchLayeredForm,
            handler=handle_crossmatch_layered,
            group="Crossmatch",
        ),
    )
