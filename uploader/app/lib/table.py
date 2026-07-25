from uploader.app.upload import handle_call
from uploader.clients.gen.client import adminapi
from uploader.clients.gen.client.adminapi.api.default import get_table


def fetch_column_units(
    client: adminapi.AuthenticatedClient,
    table_name: str,
) -> tuple[set[str], dict[str, str]]:
    resp = handle_call(get_table.sync_detailed(client=client, table_name=table_name))
    column_names: set[str] = set()
    column_units: dict[str, str] = {}
    for col in resp.data.column_info:
        column_names.add(col.name)
        if isinstance(col.unit, str):
            column_units[col.name] = col.unit
    return column_names, column_units
