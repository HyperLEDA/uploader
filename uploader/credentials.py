import base64
import pathlib

import dotenv

ENV_PATH = pathlib.Path(".env")


def save_credentials(db_user: str, db_password: str) -> None:
    ENV_PATH.touch(exist_ok=True)
    dotenv.set_key(str(ENV_PATH), "DB_USER", db_user)
    dotenv.set_key(str(ENV_PATH), "DB_PASSWORD", db_password)


def load_credentials() -> tuple[str, str]:
    values = dotenv.dotenv_values(ENV_PATH)
    db_user = values.get("DB_USER", "")
    db_password = values.get("DB_PASSWORD", "")
    if not db_user or not db_password:
        raise RuntimeError("Database credentials not configured. Run the 'Authenticate' task first.")
    return str(db_user), str(db_password)


def save_backend_credentials(backend_user: str, backend_password: str) -> None:
    ENV_PATH.touch(exist_ok=True)
    dotenv.set_key(str(ENV_PATH), "BACKEND_USER", backend_user)
    dotenv.set_key(str(ENV_PATH), "BACKEND_PASSWORD", backend_password)


def load_backend_credentials() -> tuple[str, str]:
    values = dotenv.dotenv_values(ENV_PATH)
    backend_user = values.get("BACKEND_USER", "")
    backend_password = values.get("BACKEND_PASSWORD", "")
    if not backend_user or not backend_password:
        raise RuntimeError("Backend credentials not configured. Run the 'Authenticate' task first.")
    return str(backend_user), str(backend_password)


def load_backend_basic_token() -> str:
    backend_user, backend_password = load_backend_credentials()
    raw = f"{backend_user}:{backend_password}".encode()
    return base64.b64encode(raw).decode()
