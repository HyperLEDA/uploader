from uploader.app.endpoints import env_map
from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import get_catalogs
from uploader.clients.gen.client.adminapi.models.catalog_schema import CatalogSchema


def fetch_catalogs(*, base_url: str | None = None) -> list[CatalogSchema]:
    client = adminapi.Client(base_url=base_url or env_map["prod"])
    response = handle_call(get_catalogs.sync_detailed(client=client))
    return list(response.data.catalogs)
