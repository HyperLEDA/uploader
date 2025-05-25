import importlib.util
import sys
from pathlib import Path

import structlog
from app import interface


def discover_plugins(dir: str) -> dict[str, type[interface.UploaderPlugin]]:
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()

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
            logger.warn("python file has no declared plugin", filename=str(file_path))
            continue

        plugin_class = getattr(module, "plugin")

        if not issubclass(plugin_class, interface.UploaderPlugin):
            logger.warn(
                "plugin is declared but does not satisfy the required specification",
                filename=str(file_path),
            )
            continue

        plugin_name = plugin_class.name()
        plugins[plugin_name] = plugin_class

        structlog.get_logger().info("discovered plugin", name=plugin_name)

    return plugins
