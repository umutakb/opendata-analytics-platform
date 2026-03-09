# Roadmap — OpenData Analytics Platform

Bu doküman, projenin sürüm bazlı gelişim planını içerir.

## v1.0 (Current)
- DuckDB warehouse (local)
- Synthetic e-commerce data generation (offline)
- SQL transforms (staging/marts)
- Metrics layer (GMV, Orders, AOV, Active Customers, Cohort Retention)
- Data quality report (JSON + HTML)
- Streamlit dashboard (KPI + charts + cohort + DQ summary)
- CI (pytest)

## v1.1 (Polish + UX)
- Dashboard filtreleri: tarih aralığı, country, category
- Net GMV: refunds/canceled ayrımı, net_gmv metriği
- Artifacts: run timestamp klasörleme (artifacts/runs/YYYY-MM-DD_HHMM)
- DQ: ek kontroller (ör. referential integrity: orders.customer_id exists)

## v1.2 (Operationalization)
- `opdata run-all` komutu (ingest + build + transform + metrics + quality)
- Config-driven metrics: config ile hangi metric’ler çalışsın seçimi
- Daha iyi loglama (artifacts/logs/run.log)
- CI’da “küçük demo pipeline” koşumu (daha sağlam)

## v2.0 (Extensibility)
- Multi-source: synthetic + optional open dataset (offline fallback her zaman var)
- Plugin yapısı:
  - new metric = new SQL file + auto-discovery
  - new DQ check = new python check module
- Dashboard sayfaları modüler (multi-page)
- Data contract: schema versioning + breaking changes policy

## v3.0 (Automation + Sharing)
- GitHub Actions scheduled run (nightly) + artifacts upload
- One-click demo: release assets + quick demo dataset
- (Optional) Streamlit Cloud deployment guide
