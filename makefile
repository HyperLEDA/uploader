install:
	uv sync

install-dev:
	uv sync --all-extras

check:
	@find . \
		-name "*.py" \
		-not -path "./.venv/*" \
		-not -path "./.git/*" \
		-exec uv run python -m py_compile {} +
	@uvx ruff format \
		--quiet \
		--config=pyproject.toml \
		--check
	@uvx ruff check \
		--quiet \
		--config=pyproject.toml
	@uv run pytest \
		--quiet \
		--config-file=pyproject.toml

fix:
	@uvx ruff format \
		--quiet \
		--config=pyproject.toml
	@uvx ruff check \
		--quiet \
		--config=pyproject.toml \
		--fix

# only for mac as this is faster
build:
	docker build . \
		--platform linux/arm64

update-template:
	copier update \
		--skip-answered \
		--conflict inline \
		--answers-file .template.yaml

upgrade:
	uv sync --all-extras --upgrade

gen:
	uv run openapi-python-client generate \
		--output-path app/gen/client \
		--overwrite \
		--meta uv \
		--config openapigen.yaml \
		--url https://leda.sao.ru/admin/api/openapi.json
