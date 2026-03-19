from server.forms.authenticate import AuthenticateForm, handle_authenticate
from server.forms.crossmatch_default import CrossmatchDefaultForm, handle_crossmatch_default
from server.forms.crossmatch_layered import CrossmatchLayeredForm, handle_crossmatch_layered
from server.forms.crossmatch_two_radii import CrossmatchTwoRadiiForm, handle_crossmatch_two_radii
from server.forms.structured_designation import (
    StructuredDesignationForm,
    handle_structured_designation,
)
from server.forms.structured_icrs import StructuredIcrsForm, handle_structured_icrs
from server.forms.structured_nature import StructuredNatureForm, handle_structured_nature
from server.forms.structured_photometry_hyperleda import (
    StructuredPhotometryHyperledaForm,
    handle_structured_photometry_hyperleda,
)
from server.forms.structured_redshift import StructuredRedshiftForm, handle_structured_redshift
from server.forms.upload_csv import UploadCsvForm, handle_upload_csv
from server.forms.upload_fits import UploadFitsForm, handle_upload_fits
from server.forms.upload_vizier import UploadVizierForm, handle_upload_vizier
from server.forms.upload_vizier_v2 import UploadVizierV2Form, handle_upload_vizier_v2
from server.tasks import TaskDefinition, register_task


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
            title="Upload CSV",
            description="Upload a raw table from a CSV file.",
            form_model=UploadCsvForm,
            handler=handle_upload_csv,
            group="Raw tables",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-fits",
            title="Upload FITS",
            description="Upload a raw table from a FITS file.",
            form_model=UploadFitsForm,
            handler=handle_upload_fits,
            group="Raw tables",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-vizier",
            title="Upload Vizier",
            description="Upload a raw table from a VizieR catalog.",
            form_model=UploadVizierForm,
            handler=handle_upload_vizier,
            group="Raw tables",
        ),
    )
    register_task(
        TaskDefinition(
            id="upload-vizier-v2",
            title="Upload Vizier V2",
            description="Upload a raw table from VizieR using TAP queries.",
            form_model=UploadVizierV2Form,
            handler=handle_upload_vizier_v2,
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
