install:
	uv sync

install-frontend:
	cd frontend && yarn install --frozen-lockfile

install-dev:
	uv sync --all-extras

install-dev-frontend:
	cd frontend && yarn install

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
	@echo "Compilation ok."

	@uv run ruff format \
		--quiet \
		--config=pyproject.toml \
		--check
	@echo "Formatting ok."

	@uv run ruff check \
		--quiet \
		--config=pyproject.toml
	@echo "Linter ok."

	@output=$$(uv run basedpyright 2>&1); exit_code=$$?; \
	if [ $$exit_code -ne 0 ]; then echo "$$output"; fi; \
	exit $$exit_code
	@echo "Typechecking ok."

	@uv run pytest \
		--quiet \
		--config-file=pyproject.toml
	@echo "Testing ok."

check-frontend:
	@output=$$(cd frontend && yarn run --silent prettier --check src 2>&1) || { echo "$$output"; exit 1; }
	@output=$$(cd frontend && yarn run --silent eslint src 2>&1) || { echo "$$output"; exit 1; }
	@output=$$(cd frontend && yarn run --silent test:run 2>&1) || { echo "$$output"; exit 1; }
	@cd frontend && yarn build

fix:
	@uv run ruff format \
		--quiet \
		--config=pyproject.toml

	@uv run ruff check \
		--quiet \
		--config=pyproject.toml \
		--fix

fix-frontend:
	@output=$$(cd frontend && yarn run --silent prettier --write src 2>&1) || { echo "$$output"; exit 1; }
	@output=$$(cd frontend && yarn run --silent eslint --fix src 2>&1) || { echo "$$output"; exit 1; }

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
	mkdir -p uploader/clients/gen
	uv run openapi-python-client generate \
		--output-path uploader/clients/gen/client \
		--overwrite \
		--meta uv \
		--config openapigen.yaml \
		--url https://leda.sao.ru/admin/api/openapi.json

.PHONY: serve frontend dev check-frontend fix-frontend install-frontend install-dev-frontend

serve:
	uv run uvicorn uploader.cli:app --reload --port 8000

frontend:
	cd frontend && yarn dev

dev:
	@set -e; \
	trap 'kill $$backend_pid $$frontend_pid 2>/dev/null || true; wait $$backend_pid $$frontend_pid 2>/dev/null || true' INT TERM EXIT; \
	$(MAKE) --no-print-directory serve & \
	backend_pid=$$!; \
	$(MAKE) --no-print-directory frontend & \
	frontend_pid=$$!; \
	wait $$backend_pid $$frontend_pid
