# observability

Event stream (the measurement of record, Arch §7.3) plus the context sources (Arch §9). Context
and trend only -- never a pass/fail source (Arch §7.1).

- `event_stream.py` -- the run's events; `subscribe()` lets an observer see them, never at the cost of the record
- `observer.py` -- the run's link to the environment's Prometheus + Grafana (env profile `observability:`):
  markers for the run's events, the run window's metrics copied into `metrics.json`, the dashboard link in the report
- `metrics.py` -- the series every run keeps; `dashboard.py` builds the Grafana dashboard from the same list
- `scrape_config.py` -- Prometheus's targets, generated from the env profiles
- `prometheus.py`, `grafana.py`, `http_client.py` -- the clients (standard library only)
- `alerts.py` -- the alert source interface (MTTD; none connected yet)

Setup of the stack itself: `infra/monitoring/README.md`.
