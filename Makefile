.DEFAULT_GOAL := help
.PHONY: help setup up down restart logs ps psql seed reseed reset clean lint \
        traffic inject failures watch reset-schema dagster-dev ingest-once \
        ac1 ac1-full ac1-json ac3 ac3-ci dbt-build \
        evals eval-ac1 eval-ac2 eval-ac3 eval-defects

SHELL := /bin/bash
COMPOSE := docker compose

# Dagster needs a home for the instance (run/event storage) so successive
# ingest runs are genuinely incremental (watermark read from prior run).
export DAGSTER_HOME ?= $(CURDIR)/.dagster_home

# The top-level package is named `platform` (per the locked contract), which
# collides with the stdlib. The dagster CLI's loader does not put the repo root
# on sys.path, so `dagster dev -m platform.ingestion...` cannot find the package
# unless we export the repo root explicitly. `python -m` does this implicitly;
# the dagster CLI does not.
export PYTHONPATH := $(CURDIR)$(if $(PYTHONPATH),:$(PYTHONPATH),)

ifneq (,$(wildcard .env))
include .env
export
endif
RUN := uv run python -m src.seed.seed
GEN := uv run python -m src.gen.cli

CUSTOMERS ?= 500
PRODUCTS  ?= 200
ORDERS    ?= 5000
SEED      ?= 42
FAILURE   ?= schema_drift
TRAFFIC   ?= 200

help:
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Install Python dependencies with uv
	uv sync

up: ## Start PostgreSQL (schema auto-applied on first boot)
	$(COMPOSE) up -d
	@echo "Waiting for PostgreSQL to be healthy..."
	@until [ "$$($(COMPOSE) ps -q postgres | xargs docker inspect -f '{{.State.Health.Status}}')" = "healthy" ]; do sleep 1; done
	@echo "PostgreSQL is ready."

down: ## Stop containers (keep data)
	$(COMPOSE) down

restart: down up ## Restart the stack

logs: ## Tail PostgreSQL logs
	$(COMPOSE) logs -f postgres

ps: ## Show container status
	$(COMPOSE) ps

psql: ## Open a psql shell against the source database
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-postgres} -d $${POSTGRES_DB:-ecommerce}

seed: ## Generate clean correlated data (CUSTOMERS/PRODUCTS/ORDERS/SEED overridable)
	$(RUN) --customers $(CUSTOMERS) --products $(PRODUCTS) --orders $(ORDERS) --seed $(SEED)

reseed: ## Truncate then regenerate a fresh clean dataset
	$(RUN) --customers $(CUSTOMERS) --products $(PRODUCTS) --orders $(ORDERS) --seed $(SEED) --truncate

reset: down clean up ## Destroy data volume and recreate an empty database

clean: ## Remove containers and the data volume
	$(COMPOSE) down -v

failures: ## List available failure modes
	$(GEN) list

traffic: ## Insert normal orders (TRAFFIC=count)
	$(GEN) traffic --orders $(TRAFFIC)

inject: ## Inject one failure mode (FAILURE=schema_drift)
	$(GEN) inject $(FAILURE)

reset-schema: ## Revert schema drift (user_id -> customer_id)
	$(GEN) reset-schema

watch: ## Stream traffic and inject random failures (Ctrl-C to stop)
	$(GEN) watch

dagster-dev: ## Launch the Dagster UI for C2 ingestion (http://localhost:3000)
	@mkdir -p $(DAGSTER_HOME)
	uv run dagster dev -m platform.ingestion.definitions

ingest-once: ## Run C2 raw ingestion once (Postgres -> DuckDB raw.raw_*), incremental
	@mkdir -p $(DAGSTER_HOME)
	uv run python -m platform.ingestion.run --persistent

ac1: ## C4h AC-1 gate: fast smoke (peak load + analytics, exit!=0 on breach)
	uv run python -m platform.harness.cli --profile smoke

ac1-full: ## C4h AC-1 gate: full 75k/day-equiv, 60s window (exit!=0 on breach)
	uv run python -m platform.harness.cli --profile full

ac1-json: ## C4h AC-1 gate (full) as JSON for evals/dashboards
	uv run python -m platform.harness.cli --profile full --json

dbt-build: ## Run the C3 dbt medallion build (bronze->silver->gold) into the warehouse
	@mkdir -p $(DAGSTER_HOME)
	cd platform/transform && DUCKDB_DATABASE=$(CURDIR)/platform/warehouse/warehouse.duckdb \
		uv run dbt build --profiles-dir profiles

ac3: ## C8 AC-3 gate: median source->gold freshness lag <= 5min (3 samples)
	uv run python -m platform.probe.cli --samples 3

ac3-ci: ## C8 AC-3 gate: single-shot CI mode (1 sample, JSON, exit!=0 on breach)
	uv run python -m platform.probe.cli --ci

evals: ## Run ALL acceptance-criteria evals and print a verdict table (scaled-down)
	bash platform/evals/run_evals.sh

eval-ac1: ## Eval AC-1 peak isolation (PROFILE=smoke|full; default smoke)
	bash platform/evals/eval_ac1.sh $(PROFILE)

eval-ac2: ## Eval AC-2 gold query p95 <= 5s (REPS overridable)
	bash platform/evals/eval_ac2.sh $(REPS)

eval-ac3: ## Eval AC-3 source->gold freshness <= 5min (SAMPLES overridable)
	bash platform/evals/eval_ac3.sh $(SAMPLES)

eval-defects: ## Eval defect survival: inject -> caught in *_rejects, absent from gold
	bash platform/evals/eval_defect_survival.sh

lint: ## Lint the Python code with ruff
	uv run ruff check src platform tests
