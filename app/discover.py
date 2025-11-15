import importlib.util
import sys
from pathlib import Path

from app import interface, log


def discover_plugins(dir: str) -> dict[str, type[interface.UploaderPlugin]]:
    plugins: dict[str, type[interface.UploaderPlugin]] = {}

    py_files = Path(dir).glob("*.py")

    for file_path in py_files:
        module_name = file_path.stem
        spec = importlib.util.spec_from_file_location(module_name, str(file_path))
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        if not hasattr(module, "plugin"):
            log.logger.warn("python file has no declared plugin", filename=str(file_path))
            continue

        plugin_class = getattr(module, "plugin")

        if not hasattr(module, "name"):
            log.logger.warn(
                "python file has no declared plugin name",
                filename=str(file_path),
                plugin=type(plugin_class),
            )
            continue

        plugin_name = getattr(module, "name")

        if not issubclass(plugin_class, interface.UploaderPlugin):
            log.logger.warn(
                "plugin is declared but does not satisfy the required specification",
                filename=str(file_path),
            )
            continue

        plugins[plugin_name] = plugin_class

        log.logger.info("discovered plugin", name=plugin_name)

    return plugins
