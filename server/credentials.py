import dotenv

import server.paths as paths

ENV_PATH = paths.DATA_DIR / ".env"


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
