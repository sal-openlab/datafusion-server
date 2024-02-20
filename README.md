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

## Using pre-built Docker image (Currently available amd64 architecture only)

### Pre-require

* Docker-ce/ee v20+

### Pull container image from GitHub container registry

```sh
$ docker pull ghcr.io/sal-openlab/datafusion-server/datafusion-server:latest
```

or built without Python plugin version.

```sh
$ docker pull ghcr.io/sal-openlab/datafusion-server/datafusion-server-without-plugin:latest
```

### Executing container

```sh
$ docker run -d --rm \
    -p 4000:4000 \
    -v ./data:/var/datafusion-server/data \
    --name datafusion-server \
    ghcr.io/sal-openlab/datafusion-server/datafusion-server:latest
```

If you are only using sample data in a container, omit the `-v ./data:/var/xapi-server/data`.

## Build container your self

### Pre-require

* Docker-ce/ee v20+

### Build two containers, datafusion-server and datafusion-server-without-plugin

```sh
$ cd <repository-root-dir>
$ ./make-containers.sh
```

### Executing container

```sh
$ docker run -d --rm \
    -p 4000:4000 \
    -v ./bin/data:/var/datafusion-server/data \
    --name datafusion-server \
    datafusion-server:0.8.15
```

If you are only using sample data in a container, omit the `-v ./bin/data:/var/xapi-server/data`.

## Build from source code for use in your project

### Pre-require

* Rust Toolchain 1.76+ (Edition 2021) from https://www.rust-lang.org
* _or_ the Rust official container from https://hub.docker.com/_/rust

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
datafusion-server = "0.8.15"
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
datafusion-server = { version = "0.8.15", features = ["plugin"] }
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
datafusion-server = { version = "0.8.15", features = ["plugin"] }
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

#### Example (local file)

```sh
$ curl -X "POST" "http://localhost:4000/dataframe/query" \
     -H 'Content-Type: application/json' \
     -d $'
{
  "dataSources": [
    {
      "format": "csv",
      "name": "sales",
      "location": "file:///superstore.csv",
      "options": {
        "inferSchemaRows": 100,
        "hasHeader": true
      }
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

#### Example (remote REST API)

```sh
$ curl -X "POST" "http://localhost:4000/dataframe/query" \
     -H 'Content-Type: application/json' \
     -d $'
{
  "dataSources": [
    {
      "format": "json",
      "name": "entry",
      "location": "https://api.publicapis.org/entries",
      "options": {
        "jsonPath": "$.entries[*]"
      }
    }
  ],
  "query": {
    "sql": "SELECT * FROM entry WHERE \"Category\"='Animals'"
  },
  "response": {
    "format": "json"
  }
}'
```

#### Example (Python datasource connector)

```sh
$ curl -X "POST" "http://localhost:4000/dataframe/query" \
     -H 'Content-Type: application/json' \
     -d $'
{
  "dataSources": [
    {
      "format": "arrow",
      "name": "example",
      "location": "excel://example-workbook.xlsx/Sheet1",
      "options": {
        "skipRows": 2
      }
    }
  ],
  "query": {
    "sql": "SELECT * FROM example"
  },
  "response": {
    "format": "json"
  }
}'
```
