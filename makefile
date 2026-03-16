install:
	uv sync

install-dev:
	uv sync --all-extras

check:
	@output=$$(copier check-update --answers-file .template.yaml 2>&1) || true; \
	if echo "$$output" | grep -q "up-to-date"; then \
		true; \
	elif echo "$$output" | grep -q "New template version"; then \
		echo "Template update available, run make update-template"; \
	else \
		echo "$$output"; \
	fi
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

new-branch:
	@read -p "Branch name: " branch_name && \
	branch_name=$${branch_name// /-} && \
	base=$$(git remote show origin | sed -n '/HEAD branch/s/.*: //p') && \
	echo "Selecting $$base branch as default" && \
	git fetch origin $$base && \
	git checkout -b $$branch_name origin/$$base && \
	git push -u origin $$branch_name

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
