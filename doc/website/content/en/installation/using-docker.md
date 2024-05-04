---
title: Using Docker
weight: 10
---

{{< toc >}}

## Pre-built Docker images

DataFusion Server has two container options: a full-featured container with Python plugin enabled and a compact container without Python plugin.

### Pull container image from GitHub container registry

{{< hint type=note >}}
GitHub container registry supports only the amd64 architecture.
{{< /hint >}}

Full-featured built container:

```shell
docker pull ghcr.io/sal-openlab/datafusion-server/datafusion-server:latest
```

or built without Python plugin container:

```shell
docker pull ghcr.io/sal-openlab/datafusion-server/datafusion-server-without-plugin:latest
```

### Executing container

Full-featured built container:

```shell
docker run -d --rm \
    -p 4000:4000 \
    -v ./data:/var/datafusion-server/data \
    --name datafusion-server \
    ghcr.io/sal-openlab/datafusion-server/datafusion-server:latest
```

or without Python plugin container:

```shell
docker run -d --rm \
    -p 4000:4000 \
    -v ./data:/var/datafusion-server/data \
    --name datafusion-server \
    ghcr.io/sal-openlab/datafusion-server/datafusion-server-without-plugin:latest
```

If you are only using sample data in a container, omit the `-v ./data:/var/xapi-server/data`.

```shell
docker run -d --rm \
    -p 4000:4000 \
    --name datafusion-server \
    ghcr.io/sal-openlab/datafusion-server/datafusion-server:latest
```

### Checking running logs and server statistics

Inspecting container logs.

```shell
docker logs datafusion-server
```

Call statistics endpoint by [cURL](https://curl.se/) {{< icon "external-link" >}}with [jq](https://jqlang.github.io/jq/) {{< icon "external-link" >}}formatter.

```shell
curl http://localhost:4000/sysinfo | jq
```

Results is like follows.

```json
{
  "name": "datafusion-server",
  "version": "0.9.1",
  "plugin": {
    "pythonInterpreter": "3.11.7 (main, Jan  9 2024, 06:52:32) [GCC 12.2.0]",
    "connectors": [
      {
        "module": "example",
        "version": "1.1.0"
      },
      {
        "module": "excel",
        "version": "1.0.0"
      }
    ],
    "processors": [
      {
        "module": "pivot-table",
        "version": "1.0.0"
      }
    ]
  },
  "statistics": {
    "runningTime": 1277
  }
}
```

### Stopping container

```shell
docker stop datafusion-server
```

## Building containers your self

{{< hint type=note title="Supported Environments" >}}
* Docker CE / EE [Supported platforms](https://docs.docker.com/engine/install/#supported-platforms) {{< icon "external-link" >}}
* Docker Desktop for [Windows](https://docs.docker.com/desktop/install/windows-install/) {{< icon "external-link" >}}/ [macOS](https://docs.docker.com/desktop/install/mac-install/) {{< icon "external-link" >}}/ [Linux](https://docs.docker.com/desktop/install/linux-install/) {{< icon "external-link" >}}
{{< /hint >}}

### Clone the source codes from GitHub

```shell
git clone https://github.com/sal-openlab/datafusion-server.git
cd datafusion-server
```

### Executes the bundled shell script

```shell
./make-containers.sh
```

This will build two containers: a full-feature container and a compact container with the without plugin feature. Additionally, Docker image files containing both containers are generated.

* datafusion-server-x.y.z.tar.gz
* datafusion-server-without-plugin-x.y.z.tar.gz

If the `--no-export` option is added to the `make-containers.sh` script, container image creation will not be performed.

### Executing container

Full-featured built container:

```shell
docker run -d --rm \
    -p 4000:4000 \
    -p 50051:50051 \
    -v ./data:/var/datafusion-server/data \
    --name datafusion-server \
    datafusion-server:x.y.z
```

or without Python plugin container:

```shell
docker run -d --rm \
    -p 4000:4000 \
    -p 50051:50051 \
    -v ./data:/var/datafusion-server/data \
    --name datafusion-server \
    datafusion-server-without-plugin:x.y.z
```

If you are only using sample data in a container, omit the `-v ./data:/var/xapi-server/data`.

```shell
docker run -d --rm \
    -p 4000:4000 \
    -p 50051:50051 \
    --name datafusion-server \
    datafusion-server:x.y.z
```
