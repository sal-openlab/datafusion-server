---
title: PostgreSQL
weight: 10
---

{{< toc >}}

## Supported Versions

* [PostgreSQL](https://www.postgresql.org/) {{< icon "external-link" >}} v9.5+

## Configuration

### Configuration File (config.toml)

Minimum example

```toml
[[databases]]
type = "postgres"
namespace = "pg1"
user = "example"
password = "password"
host = "postgres-sever.local"
database = "example"
```

Full example

```toml
[[databases]]
type = "postgres"
namespace = "pg1" # default postgres
user = "example"
password = "password"
host = "postgres-server.local"
port = 5432 # default 5432
database = "example"
ssl_mode = "prefer" # disable, allow, prefer (default), require, verify-ca, verify-full
max_connections = 30 # default 10
enable_schema_cache = true # default false
description = "PostgreSQL"
```

By separating namespace, multiple definitions can be defined.

### Environment Variables

Minimum example for `docker run`

```shell
docker run -d --rm \
    -p 4000:4000 \
    -e POSTGRES_NAMESPACE=pg1 \
    -e POSTGRES_USER=example \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_HOST=postgres.local \
    -e POSTGRES_DATABASE=example \
    --name datafusion-server \
    datafusion-server:latest
```

In addition to this, the following parameters can be specified as needed.

* `POSTGRES_PORT`
* `POSTGRES_SSL_MODE`
* `POSTGRES_MAX_CONNECTIONS`
* `POSTGRES_ENABLE_SCHEMA_CACHE`

It can coexist with the configuration file, but namespace must be unique.
Additionally, only one definition can be made via environment variables.
