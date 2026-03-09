PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
OPDATA := $(VENV)/bin/opdata
PYTEST := $(VENV)/bin/pytest

.PHONY: venv install demo dashboard test

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -e .[dev]

demo:
	$(OPDATA) demo-data --out data/raw --seed 42 --days 365 --orders 55000
	$(OPDATA) build-warehouse --raw data/raw --db data/warehouse.duckdb
	$(OPDATA) transform --db data/warehouse.duckdb
	$(OPDATA) metrics --db data/warehouse.duckdb --out artifacts/metrics
	$(OPDATA) quality --db data/warehouse.duckdb --out artifacts/quality --config config.example.yml

dashboard:
	$(OPDATA) dashboard --db data/warehouse.duckdb

test:
	$(PYTEST) -q
