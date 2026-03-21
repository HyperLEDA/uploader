env_map = {
    "dev": "http://localhost:8080",
    "test": "https://leda.kraysent.dev",
    "prod": "https://leda.sao.ru",
}

db_dsn_map = {
    "dev": "postgresql://{user}:{password}@localhost:5432/hyperleda",
    "test": "postgresql://{user}:{password}@leda.kraysent.dev:5433/hyperleda",
    "prod": "postgresql://{user}:{password}@database.leda.sao.ru:5432/hyperleda",
}
