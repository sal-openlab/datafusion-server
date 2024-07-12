---
title: Using crate for your project
weight: 20
---

{{< toc >}}

## Pre-require (MSRV)

* Rust Toolchain 1.74+ (Edition 2021) from https://www.rust-lang.org {{< icon "external-link" >}}

## Add DataFusion Server crate to your Cargo.toml

If you do not need the plugin feature, simply use `cargo add datafusion-server` or edit Cargo.toml manually.

```toml
[dependencies]
datafusion-server = "x.y.z"
```

or enables Python plugin feature:

```toml
[dependencies]
datafusion-server = { version = "x.y.z", features = ["plugin"] }
```

Refer to the next section for the configurable feature flags.

## Feature flags

| flags     | feature                                           |
|-----------|---------------------------------------------------|
| plugin    | Data source connector and post processor plugin   |
| flight    | Arrow Flight RPC client / server                  |
| avro      | Apache Avro format for using data source          |
| webdav    | HTTP extended WebDAV store                        |
| deltalake | Delta Lake integration                            |
| telemetry | Track and expose metric information to Prometheus |

## Example of call the DataFusion Server entry function

Configuration programmatically.

```rust
use datafusion_server::settings::Settings;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // making configuration for the DataFusion Server
    let mut settings = Settings::new()?;
    settings.server.port = 80;
    settings.log.level = "debug".to_string();

    // executing the DataFusion Server with custom configuration
    datafusion_server::execute(settings)?;

    Ok(())
}
```

Configuration from file.

```rust
use datafusion_server::settings::Settings;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // making configuration from file
    let config_file = std::path::PathBuf::from("./config.toml");
    let settings = Settings::new_with_file(&config_file)?;

    // executing the DataFusion Server
    datafusion_server::execute(settings)?;

    Ok(())
}
```

Example configuration file (config.toml).

```toml
[server]
port = 80
flight_grpc_port = 50051
metrics_address = "0.0.0.0"
metrics_port = 9100
base_url = "/"
data_dir = "./data"
plugin_dir = "./plugins"

[session]
default_keep_alive = 3600 # 1 hour
upload_limit_size = 2000 # 2GB

[log]
level = "debug"
```

### Configuration parameters

| Parameter                          | Description                                                    | Default     |
|------------------------------------|----------------------------------------------------------------|-------------|
| `server.address`                   | Acceptable host address                                        | `0.0.0.0`   |
| `server.port`                      | Listening port for HTTP                                        | `4000`      |
| `server.flight_address`            | Acceptable host address for Flight gRPC                        | `0.0.0.0`   |       
| `server.flight_grpc_port`          | Listening port for Flight gRPC                                 | `50051`     |
| `server.metrics_address`           | Acceptable host address for metrics information for Prometheus | `127.0.0.1` |       
| `server.metrics_port`              | Listening port for metrics information for Prometheus          | `9100`      |
| `server.base_url`                  | URL prefix                                                     | `/`         |
| `server.data_dir`                  | Static data source directory                                   | `./data`    |
| `server.plugin_dir`                | Python plugin directory                                        | `./plugin`  |
| `server.disable_stateful_features` | Disables stateful endpoints, like a `/session/create`          | `false`     |                          
| `session.default_keep_alive`       | Default session timeout value in seconds                       | `3600`      |
| `session.upload_limit_size`        | Size limit in MB for `/session/:id/datasource/upload` endpoint | `20`        |
| `log.level`                        | Logging level (`trace`, `debug`, `info`, `warn`, `error`)      | `info`      |
