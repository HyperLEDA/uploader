upgrade:
	uv sync --all-extras --upgrade

check:
	uvx ruff check
	uvx ruff format --check . 
	uv run pytest --config-file=pyproject.toml

fix:
	uvx ruff format .

test:
	uvx pytest tests

gen:
	uv run openapi-python-client generate \
		--output-path app/gen/client \
		--overwrite \
		--meta uv \
		--config openapigen.yaml \
		--url https://leda.sao.ru/admin/api/openapi.json
