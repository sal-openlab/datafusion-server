# datafusion-server crate

Multiple session, variety of data sources query server implemented by Rust.

* Asynchronous architecture used by [Tokio](https://tokio.rs/) ecosystem
* Apache Arrow with DataFusion
  + Supports multiple data source with SQL queries
* Python plugin feature for data source connector and post processor

## License

License under the [MIT](LICENSE)

Copyright (c) 2022 - 2024 SAL Ltd. - https://sal.co.jp

## Supported environment

* Linux
* BSD based Unix incl. macOS 10.6+
* SVR4 based Unix
* Windows 10+ incl. WSL 2

and other [LLVM](https://llvm.org/) supported environment.

## Build at local environment

### Pre-require

* Rust Toolchain 1.73+ (Edition 2021) from https://www.rust-lang.org
* _or_ the Docker official container image from https://hub.docker.com/_/rust

### How to run

```sh
$ cargo init server-executer
$ cd server-executer
```

#### Example of Cargo.toml

```toml
[package]
name = "server-executer"
version = "0.1.0"
edition = "2021"

[dependencies]
datafusion-server = "0.8.13"
```

#### Example of src/main.rs

```rust
fn main() {
    datafusion_server::execute("path/to/config.toml");
}
```

#### Example of config.toml

```toml
# Configuration file of datafusion-server

[server]
port = 4000
base_url = "/"
data_dir = "./data"
plugin_dir = "./plugins"

[session]
default_keep_alive = 3600 # in seconds

[log]
# trace, debug, info, warn, error
level = "debug"
```

#### Debug build and run

```sh
$ cargo run
```

## datafusion-server with Python plugins feature

Require Python interpreter v3.7+

### How to run

#### Example of Cargo.toml

```toml
[package]
name = "server-executor"
version = "0.1.0"
edition = "2021"

[dependencies]
datafusion-server = { version = "0.8.13", features = ["plugin"] }
```

#### Debug build and run

```sh
$ cargo run
```

### Release build with full optimization

#### Example of Cargo.toml

```toml
[package]
name = "server-executor"
version = "0.1.0"
edition = "2021"

[profile.release]
opt-level = 'z'
strip = true
lto = "fat"
codegen-units = 1

[dependencies]
datafusion-server = { version = "0.8.13", features = ["plugin"] }
```

#### Build for release

```sh
$ cargo build --release
```

### Clean workspace

```sh
$ cargo clean
```

## Usage

### Multiple data sources with SQL query

* Can be used many kind of data source format (Parquet, JSON, ndJSON, CSV, ...).
* Data can be retrieved from the local file system and from external REST services.
  + Processing by JSONPath can be performed if necessary.
* Query execution across multiple data sources.
  + SQL query engine uses Arrow DataFusion.
    - Details https://arrow.apache.org/datafusion/user-guide/sql/index.html for more information.
* Arrow, JSON and CSV formats to response.

```sh
$ curl -X "POST" "http://localhost:4000/dataframe/query" \
     -H 'Content-Type: application/json' \
     -d $'
{
  "dataSources": [
    {
      "options": {
        "inferSchemaRows": 100,
        "hasHeader": true
      },
      "name": "sales",
      "location": "superstore.csv",
      "format": "csv"
    }
  ],
  "query": {
    "sql": "SELECT * FROM sales"
  },
  "response": {
    "format": "json"
  }
}'
```
