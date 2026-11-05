# Grafana dashboard

`pumasi-dashboard.json` shows a Pumasi cluster from its Prometheus metrics:
nodes online, the queue by priority, runs per minute, each node's CPU,
memory, disk, network, temperature and round trip, GPUs, CU balances and
the file store.

1. Start the control plane with `--metrics-addr` and scrape it with
   Prometheus (see [`../prometheus.yml`](../prometheus.yml)).
2. In Grafana, Dashboards > New > Import, upload `pumasi-dashboard.json`
   and pick your Prometheus data source.
