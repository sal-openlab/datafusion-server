---
title: Using crate for your project
weight: 20
---

{{< toc >}}

## Pre-require (MSRV)

* Rust Toolchain 1.76+ (Edition 2021) from https://www.rust-lang.org

## Add DataFusion Server crate to your Cargo.toml

If you do not need the plugin feature, simply use `cargo add datafusion-server` or edit Cargo.toml manually.

```toml
[dependencies]
datafusion-server = "0.9.2"
```

or enables Python plugin feature:

```toml
[dependencies]
datafusion-server = { version = "0.9.2", features = ["plugin"] }
```

## Example of call the DataFusion Server entry function

Configuration programatically.

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
port = 4000
base_url = "/"
data_dir = "./data"
plugin_dir = "./plugins"

[session]
default_keep_alive = 3600

[log]
level = "debug"
```

### Configuration parameters

| Parameter           | Description                  | Default    |
| -- | -- | -- |
| `server.address`    | Acceptable host address      | `0.0.0.0`  |
| `server.port`       | Listening port for HTTP      | `4000`     |
| `server.flight_grpc_port` | Listening port for Flight gRPC | `50051` |
| `server.base_url`   | URL prefix                   | `/`        |
| `server.data_dir`   | Static data source directory | `./data`   |
| `server.plugin_dir` | Python plugin directory      | `./plugin` |
| `session.default_keep_alive` | Default session timeout value in seconds | `3600` |
| `log.level` | Logging level (`trace`, `debug`, `info`, `warn`, `error`) | `info` |



