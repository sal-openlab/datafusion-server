---
title: Metrics Information
weight: 30
---

{{< toc >}}

## Settings

Tracking and exposing metrics information is enabled by the `telemetry` feature flag.
Please refer to this [documentation]({{< ref "/installation/using-crate#feature-flags" >}})
for more information about feature flags.

The metrics information is intended to be handled by [Prometheus](https://prometheus.io/) {{< icon "external-link" >}}.
Therefore, in the config.toml file, the entry `server.metrics_address` should be configured
to listen for access from the Prometheus server. The listening address is restricted by default
to access from localhost.

```toml
[server]
metrics_address = "127.0.0.1"
metrics_port = 9100
```

In this case, we configure it to be accessible from anywhere for testing purposes,
but in a production environment, it’s advisable to restrict access to only the Prometheus server.

```toml
[server]
metrics_address = "0.0.0.0"
metrics_port = 9100
```

## Testing Configuration

After starting the DataFusion Server and making API accesses,
try accessing the `/metrics` endpoint on the configured server port.

```shell
curl http://127.0.0.1:9100/metrics
```

You should receive a response like this.

```
# TYPE http_requests_total counter
http_requests_total{method="GET",path="/session/create",status="200"} 1
http_requests_total{method="POST",path="/session/:session_id/datasource",status="204"} 1

# TYPE flight_requests_total counter
flight_requests_total{method="get_schema",status="ok"} 1
flight_requests_total{method="do_get",status="error"} 1
flight_requests_total{method="do_get",status="ok"} 1

# TYPE flight_requests_duration_seconds summary
flight_requests_duration_seconds{method="do_get",status="error",quantile="0"} 0.000506459
flight_requests_duration_seconds{method="do_get",status="error",quantile="0.5"} 0.0005064423655961616
flight_requests_duration_seconds{method="do_get",status="error",quantile="0.9"} 0.0005064423655961616
flight_requests_duration_seconds{method="do_get",status="error",quantile="0.95"} 0.0005064423655961616
flight_requests_duration_seconds{method="do_get",status="error",quantile="0.99"} 0.0005064423655961616
flight_requests_duration_seconds{method="do_get",status="error",quantile="0.999"} 0.0005064423655961616
flight_requests_duration_seconds{method="do_get",status="error",quantile="1"} 0.000506459
```

## Metric Names and Labels

Currently, following metric names and labels are exposed:

| metric name                      | value                                         | labels               |
|----------------------------------|-----------------------------------------------|----------------------|
| http_requests_total              | Total number of HTTP requests received        | method, path, status |
| http_requests_duration_seconds   | Latency from request to response              | method, path, status |
| flight_requests_total            | Total number of Flight gRPC requests received | method, status       |
| flight_requests_duration_seconds | Latency from request to response              | method, status       |
| session_contexts_total           | Total number of contexts created              |                      |
| session_context_duration_seconds | Lifetime per session                          |                      |
| data_source_registrations_total  | Total number of data source loaded            | scheme, format       |

## Configuring Prometheus

Detailed configuration information about Prometheus is not provided here.
There are shell scripts and configuration file in the `example/prometheus-docker` directory
for starting and stopping Prometheus with Docker, so let them a try.

* https://github.com/sal-openlab/datafusion-server/tree/main/example/prometheus-docker
