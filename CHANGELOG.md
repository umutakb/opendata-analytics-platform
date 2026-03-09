# Changelog

All notable changes to this project will be documented in this file.

## [1.2.0] - 2026-03-09
### Added
- `opdata run-all` command for end-to-end execution
- Config-driven metric selection via `metrics.enabled`
- Run manifests and per-run logs under `artifacts/runs/<timestamp>/`
- CI E2E flow coverage using `run-all --small`

## [1.1.0] - 2026-03-09
### Added
- Dashboard filters: date range, country, category
- Filter-aware KPI cards, daily GMV/orders charts, and Top Categories chart
- Net GMV daily metric (`m_net_gmv_daily.sql`)
- Timestamped default artifact runs under `artifacts/runs/<timestamp>/...`
- Latest artifact pointers via `artifacts/latest/` and `artifacts/latest_run.txt`
- Data quality referential integrity checks with missing count and sample rows

### Changed
- CLI `metrics` and `quality` commands now support optional `--out` with timestamped default
- E2E tests now resolve artifacts from `artifacts/runs/*` or `artifacts/latest`

## [1.0.0]
### Added
- End-to-end local analytics platform (DuckDB warehouse)
- Synthetic e-commerce dataset generator (offline)
- SQL staging + marts transformations
- Metrics layer (GMV, Orders, AOV, Active Customers, Cohort Retention)
- Data quality checks + JSON/HTML reports
- Streamlit dashboard (KPI overview, trends, cohorts, DQ summary)
- GitHub Actions CI with pytest
