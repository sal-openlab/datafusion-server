# datafusion-server

A Simple Arrow server implemented by Rust.

* Asynchronous architecture used by [Tokio](https://tokio.rs/) ecosystem
* Apache Arrow with DataFusion
  + Supports multiple data source with SQL queries
* Python plugin feature for data source connector

## License

Copyright (c) 2022 - 2023 SAL Ltd. - https://sal.co.jp

## Supported environment

* Linux with or without Docker
* BSD based Unix incl. macOS 10.6+
* SVR4 based Unix
* Windows 10+ incl. WSL 2

and other [LLVM](https://llvm.org/) supported environment.

## Build at local environment

### Pre-require

* Rust Toolchain 1.70+ (Edition 2021) from https://www.rust-lang.org
* _or_ the Docker official container image from https://hub.docker.com/_/rust

### Debug build and run instantly

```sh
$ cargo run
```

### Release build

```sh
$ cargo build --release
```

Creating executable binary at `target/release/datafusion-server`. Size of the executable file which does not depend on any shared libraries at about only 33 MB.

## datafusion-server with Python plugins feature

Require Python interpreter v3.7+

### Debug build and run instantly

```sh
$ cargo run --features plugin
```

### Release build

```sh
$ cargo build --release --features plugin
```

### Clean workspace

```sh
$ cargo clean
```

## Build to standalone Docker image

### Building container

```sh
$ docker build -t datafusion-server:0.x.x .
```

The container image size is 40 MB, including OS and all required packages.

```sh
$ docker images
REPOSITORY      TAG     IMAGE ID       CREATED         SIZE
datafusion-server     0.x.x   de4a87a6a9b1   3 minutes ago   38.9MB
```

### Running container

```sh
$ docker run --rm \
    -p 4000:4000 \
    -v ./data:/var/datafusion-server/data \
    --name datafusion-server \
    datafusion-server:0.x.x
```

Embedded plugin placed at `/usr/local/datafusion-server/plugins` directory if the plugin feature has enabled.

```sh
$ docker run --rm \
    -p 4000:4000 \
    -v ./data:/var/datafusion-server/data \
    -v ./plugins:/usr/local/datafusion-server/plugins \
    --name datafusion-server \
    datafusion-server:0.x.x
```

## Usage

### Single data source

* CSV data source to Arrow response

```sh
$ curl http://localhost:4000/arrow/csv/superstore.csv
```

* Parquet to Arrow response

```sh
$ curl http://localhost:4000/arrow/parquet/superstore.parquet
```

### Multiple data sources with SQL query

* Can be used multiple kind of format
* Query execution across multiple data sources
  + SQL query engine uses Arrow DataFusion (See https://arrow.apache.org/datafusion/user-guide/sql/index.html for more information)
* Arrow and JSON both type of response format

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
