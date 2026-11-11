COMPOSE := docker compose -f docker-compose.yml


# Test database URIs
SCYLLA_URI ?= 127.0.0.2:9042
SCYLLA_URI2 ?= 127.0.0.3:9042
SCYLLA_URI3 ?= 127.0.0.4:9042

# Export for tests
export SCYLLA_URI
export SCYLLA_URI2
export SCYLLA_URI3

.PHONY: all
all: test

.PHONY: ci
ci: static test

.PHONY: static
static: static-rust static-python

.PHONY: static-rust
static-rust: fmt-check-rust check-rust clippy
.PHONY: static-python
static-python: fmt-check-python check-python type-check

.PHONY: fmt
fmt: fmt-rust fmt-python

.PHONY: fmt-rust
fmt-rust:
	cargo fmt --all

.PHONY: fmt-check-rust
fmt-check-rust:
	cargo fmt --all -- --check

.PHONY: check-rust
check-rust:
	cargo check --all-targets

.PHONY: clippy
clippy:
	RUSTFLAGS="-Dwarnings --cfg scylla_unstable" cargo clippy --all-targets --features python-extension,openssl-010,rust-tests

.PHONY: fmt-python
fmt-python:
	uv run ruff format .

.PHONY: fmt-check-python
fmt-check-python:
	uv run ruff format --check

.PHONY: check-python
check-python:
	uv run ruff check

.PHONY: lint
lint:
	uv run ruff check --fix .

.PHONY: type-check
type-check:
	$(MAKE) -C docs setup
	uv run basedpyright
	uv run basedpyright -p docs

.PHONY: requires_db
requires_db:
	uv run pytest -v -m requires_db

.PHONY: test
test:
	uv run pytest -v

.PHONY: test-rust
test-rust:
	cargo test --no-default-features --features rust-tests

.PHONY: coverage
coverage:
	scripts/coverage.sh

.PHONY: build
build: build-rust build-python

.PHONY: build-rust
build-rust:
	cargo build

.PHONY: build-python
build-python:
	uv build

.PHONY: up
up:
	$(COMPOSE) up -d --wait
	@echo
	@echo "ScyllaDB cluster is running in the background. Use 'make down' to stop it."
	@echo

.PHONY: down
down:
	$(COMPOSE) down --remove-orphans

.PHONY: logs
logs:
	$(COMPOSE) logs -f

.PHONY: cqlsh
cqlsh:
	$(COMPOSE) exec scylla1 cqlsh -u cassandra -p cassandra

.PHONY: shell
shell:
	$(COMPOSE) exec scylla1 bash

.PHONY: clean
clean: down
	cargo clean
	uv clean
	rm -rf docs/book
	$(MAKE) docs-clean

.PHONY: docs
docs:
	$(MAKE) -C docs dirhtml

.PHONY: docs-preview
docs-preview:
	$(MAKE) -C docs preview

.PHONY: docs-clean
docs-clean:
	$(MAKE) -C docs clean

.PHONY: docs-test
docs-test:
	$(MAKE) -C docs test
