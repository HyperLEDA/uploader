import subprocess
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict) -> None:
        frontend_dir = Path(self.root) / "frontend"
        subprocess.run(
            ["yarn", "install", "--frozen-lockfile"],
            cwd=frontend_dir,
            check=True,
        )
        subprocess.run(
            ["yarn", "build"],
            cwd=frontend_dir,
            check=True,
        )
