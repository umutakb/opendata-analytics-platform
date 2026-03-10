# OpenData Analytics Platform

A clean, local-first analytics platform that implements a mini modern data stack for e-commerce data:

- Ingestion (deterministic synthetic data)
- Warehouse (DuckDB)
- SQL transforms (staging + marts)
- Metrics layer (SQL-driven exports)
- Data quality checks (JSON + HTML report)
- Streamlit dashboard
- CI with pytest

## Architecture

```text
+---------------------+      +------------------------+      +------------------------+
| Synthetic Generator | ---> | DuckDB Raw Tables      | ---> | SQL Staging Models     |
| (CSV in data/raw)   |      | customers/products/... |      | stg_* views            |
+---------------------+      +------------------------+      +------------------------+
                                                                  |
                                                                  v
                                   +------------------------+      +------------------------+
                                   | SQL Mart Models        | ---> | Metrics SQL            |
                                   | dim_*/fct_* tables     |      | CSV + JSON artifacts   |
                                   +------------------------+      +------------------------+
                                                |
                                                v
                                   +------------------------+      +------------------------+
                                   | Data Quality Checks    | ---> | Streamlit Dashboard    |
                                   | report.json / report.html     | KPIs, trends, cohorts |
                                   +------------------------+      +------------------------+
                                                |
                                                v
                                       +----------------+
                                       | GitHub Actions |
                                       | pytest -q      |
                                       +----------------+
```

## Quickstart

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .[dev]
make demo
make dashboard
```

## CLI Commands

```bash
opdata demo-data --out data/raw --seed 42 --days 365
opdata build-warehouse --raw data/raw --db data/warehouse.duckdb
opdata transform --db data/warehouse.duckdb
opdata metrics --db data/warehouse.duckdb --out artifacts/metrics
opdata quality --db data/warehouse.duckdb --out artifacts/quality --config config.example.yml
opdata dashboard --db data/warehouse.duckdb
```

`opdata dashboard` prints the Streamlit command. Use `opdata dashboard --db data/warehouse.duckdb --run` to launch directly.

## Metrics Selection (Config-Driven)

You can control which metrics are executed by editing `config.example.yml`:

```yaml
metrics:
  eval_days: 90
  enabled:
    - m_gmv_daily
    - m_orders_daily
    - m_aov_daily
    - m_active_customers_daily
    - m_retention_cohort
    - m_net_gmv_daily
  disabled:
    - m_net_gmv_daily
```

- If `metrics.enabled` is present, only listed metrics run.
- `metrics.disabled` always applies as blacklist.
- If `metrics.enabled` is missing or empty, all discovered `sql/metrics/*.sql` files run except disabled ones.

## Folder Structure

```text
opendata-analytics-platform/
  README.md
  LICENSE
  Makefile
  pyproject.toml
  config.example.yml
  src/opendata_platform/
    __init__.py
    config.py
    cli.py
    ingest/
      generate_data.py
      download_open_data.py
    warehouse/
      build_db.py
    transform/
      run_sql.py
    quality/
      dq_checks.py
      report.py
    metrics/
      metric_runner.py
    dashboard/
      app.py
  sql/
    staging/
    marts/
    metrics/
  tests/
  .github/workflows/ci.yml
  data/
    raw/
    staging/
    marts/
  artifacts/
    quality/
    metrics/
    logs/
```

## Outputs

Running `make demo` generates:

- `data/raw/*.csv`
- `data/staging/*.csv`
- `data/marts/*.csv`
- `artifacts/metrics/*.csv` and `*.json`
- `artifacts/quality/report.json`
- `artifacts/quality/report.html`

## Add a New Metric

1. Add a SQL file under `sql/metrics/` (example: `m_new_metric.sql`).
2. Ensure the SQL returns a result table.
3. Run `opdata metrics --db data/warehouse.duckdb --out artifacts/metrics`.
4. The runner auto-exports `m_new_metric.csv` and `m_new_metric.json`.

## Add a New Quality Check

1. Add rule configuration to `config.example.yml` if needed.
2. Implement check logic in `src/opendata_platform/quality/dq_checks.py`.
3. Include it in `run_quality_checks`.
4. Re-run: `opdata quality --db data/warehouse.duckdb --out artifacts/quality --config config.example.yml`.

## Optional Open Dataset Download

Core pipeline works fully offline with synthetic data. If internet is available, you can optionally fetch an open dataset:

```bash
python -m opendata_platform.ingest.download_open_data --out data/raw_open
```

## Screenshot Placeholders

- `docs/screenshots


## License

MIT
