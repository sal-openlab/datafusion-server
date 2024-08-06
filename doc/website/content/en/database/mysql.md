---
title: MySQL / MariaDB
weight: 20
---

{{< toc >}}

## Supported Versions

* [MySQL](https://www.mysql.com/) {{< icon "external-link" >}} v5.6, v5.7, v8.0
* [MariaDB](https://mariadb.org/) {{< icon "external-link" >}} v10.1+

## Configuration

### Configuration File (config.toml)

Minimum example

```toml
[[databases]]
type = "mysql"
namespace = "my1"
user = "example"
password = "password"
host = "mysql-sever.local"
database = "example"
```

Full example

```toml
[[databases]]
type = "mysql"
namespace = "my1" # default mysql
user = "root"
password = "password"
host = "mysql-server.local"
port = 3306 # default 3306
database = "example"
ssl_mode = "preferred" # disabled (default), preferred（default 5.7.3+), required, verify-ca, verify-identity
max_connections = 30 # default 10
enable_schema_cache = true # default false
description = "MySQL / MariaDB"
```

By separating namespace, multiple definitions can be defined.

### Environment Variables

Minimum example for `docker run`

```shell
docker run -d --rm \
    -p 4000:4000 \
    -e MYSQL_NAMESPACE=my1 \
    -e MYSQL_USER=example \
    -e MYSQL_PASSWORD=password \
    -e MYSQL_HOST=postgres.local \
    -e MYSQL_DATABASE=example \
    --name datafusion-server \
    datafusion-server:latest
```

In addition to this, the following parameters can be specified as needed.

* `MYSQL_PORT`
* `MYSQL_SSL_MODE`
* `MYSQL_MAX_CONNECTIONS`
* `MYSQL_ENABLE_SCHEMA_CACHE`

It can coexist with the configuration file, but namespace must be unique.
Additionally, only one definition can be made via environment variables.
