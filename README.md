# OpenData Analytics Platform

Local-first, DuckDB tabanlı mini modern data stack örneği.

- Ingestion (synthetic + optional open dataset)
- Warehouse (DuckDB)
- SQL transforms (staging + marts)
- Metrics layer (SQL auto-discovery)
- Data quality checks (plugin discovery)
- Streamlit dashboard (multi-page)
- Data contracts (schema validation)
- CI (pytest)

## Architecture

```text
+----------------------------+
| opdata ingest              |
| - source=synthetic (default)|
| - source=open (optional)   |
+-------------+--------------+
              |
              v
+----------------------------+      +-----------------------------+
| data/raw/*.csv             | ---> | opdata build-warehouse      |
+----------------------------+      | DuckDB (data/warehouse.duckdb)
                                    +---------------+-------------+
                                                    |
                                                    v
                                    +-----------------------------+
                                    | opdata transform            |
                                    | sql/staging + sql/marts     |
                                    +---------------+-------------+
                                                    |
                          +-------------------------+-------------------------+
                          v                                                   v
            +-----------------------------+                     +-----------------------------+
            | opdata metrics              |                     | opdata quality              |
            | sql/metrics/*.sql           |                     | quality/checks/* plugins    |
            | artifacts/runs/<ts>/metrics |                     | artifacts/runs/<ts>/quality |
            +-----------------------------+                     +-----------------------------+
                          \                                                   /
                           \                                                 /
                            v                                               v
                           +-----------------------------------------------+
                           | Streamlit Dashboard (Overview/Cohorts/Quality)|
                           +-----------------------------------------------+
```

## Quickstart

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .[dev]
opdata run-all --small
opdata dashboard --db data/warehouse.duckdb --run
```

## Core Commands

```bash
opdata ingest --source synthetic --out data/raw --days 365 --seed 42
opdata ingest --source open --out data/raw --dataset uci_online_retail
opdata build-warehouse --raw data/raw --db data/warehouse.duckdb
opdata transform --db data/warehouse.duckdb
opdata metrics --db data/warehouse.duckdb --config config.example.yml
opdata quality --db data/warehouse.duckdb --config config.example.yml
opdata validate-contract --db data/warehouse.duckdb --contract contracts/schema_v1.yml
opdata run-all --small
```

Not: `--source open` internet gerektirir. İndirme/parsing başarısız olursa ingest otomatik olarak synthetic veriye fallback yapar.

## run-all

`opdata run-all` adımları sırayla çalıştırır:

1. ingest
2. build-warehouse
3. transform
4. metrics
5. quality
6. (opsiyonel) validate-contract

Örnek:

```bash
opdata run-all --small
opdata run-all --source open --dataset uci_online_retail --validate-contract
```

Her çalıştırma için timestamp’li çıktı üretilir:

- `artifacts/runs/<YYYY-MM-DD_HHMMSS>/metrics`
- `artifacts/runs/<YYYY-MM-DD_HHMMSS>/quality`
- `artifacts/runs/<YYYY-MM-DD_HHMMSS>/logs/run.log`
- `artifacts/runs/<YYYY-MM-DD_HHMMSS>/run_manifest.json`
- `artifacts/latest_run.txt`

## Config

`config.example.yml` içinde:

- `schema_version: 1`
- `warehouse.db_path`
- quality kuralları
- metrics seçimi

Örnek metrics seçimi:

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

Davranış:

- `enabled` varsa yalnızca listedekiler çalışır.
- `disabled` her zaman blacklist olarak uygulanır.
- `enabled` yoksa `sql/metrics/*.sql` altındaki tüm metric SQL’leri (disabled hariç) çalışır.

## Dashboard (Multi-page)

`opdata dashboard --db data/warehouse.duckdb --run`

Sekmeler:

- `Overview`: KPI, GMV/Orders trendleri (MA7), Top Categories, Net GMV
- `Cohorts`: retention cohort tablosu
- `Quality`: PASS/WARN/FAIL özeti, failed checks, son çalıştırma bilgisi

## Data Contracts

Contract dosyası: `contracts/schema_v1.yml`

Doğrulananlar:

- required relation exists
- required column exists
- (varsa) basic type checks

Çalıştırma:

```bash
opdata validate-contract --db data/warehouse.duckdb --contract contracts/schema_v1.yml
```

## Test ve CI

```bash
pytest -q
```

CI (`.github/workflows/ci.yml`) testleri çalıştırır.

## Make Targets

```bash
make install
make demo
make run-all
make dashboard
make test
```

## Folder Structure

```text
opendata-analytics-platform/
  contracts/
    schema_v1.yml
  sql/
    staging/
    marts/
    metrics/
  src/opendata_platform/
    cli.py
    ingest/
    warehouse/
    transform/
    metrics/
    quality/
      checks/
    contracts/
    dashboard/
  tests/
  artifacts/
  data/
```

## Breaking Changes Policy

- `schema_version` artırımı potansiyel breaking change olarak değerlendirilir.
- Yeni major sürümlerde contract ve metric adlarında uyumsuz değişiklikler olabilir.
- Minor sürümler geriye uyumluluğu korumayı hedefler.

## Screenshots (Placeholders)

- `docs/screenshots/overview.png`
- `docs/screenshots/cohorts.png`
- `docs/screenshots/quality.png`

## License

MIT
