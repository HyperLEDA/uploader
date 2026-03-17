import inspect
from typing import Any

import app


def get_plugin_instance(
    plugin_name: str,
    plugins: dict[str, type[app.UploaderPlugin]],
    args: list[Any],
) -> app.UploaderPlugin:
    plugin_class = plugins[plugin_name]

    try:
        return plugin_class(*args)
    except TypeError:
        pass

    s = inspect.signature(plugin_class)
    required_args = []

    for arg_name, arg in s.parameters.items():
        if arg.default is inspect.Parameter.empty:
            required_args.append(arg_name)

    raise RuntimeError(
        f"Plugin {plugin_name} has {len(required_args)} required arguments ({required_args}). {len(args)} were given."
    )
