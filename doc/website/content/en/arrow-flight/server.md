---
title: Arrow Flight Server (TBD)
weight: 10
---

{{< toc >}}

## Additional Setup

Add feature flag `flight`.

```toml
[dependencies]
datafusion-server = { version = "x.y.z", features = ["flight"] }
```

## Using Docker

Map gRPC listening ports. The default port is `51001`.

```shell
docker run -d --rm \
    -p 4000:4000 \
    -p 51001:51001 \
    -v ./data:/var/datafusion-server/data \
    --name datafusion-server \
    datafusion-server:x.y.z
```
