upgrade:
	uv sync --all-extras --upgrade

check:
	uvx ruff check
	uvx ruff format --check . 
	uv run pytest --config-file=pyproject.toml

fix:
	uvx ruff format .

test:
	pytest tests
