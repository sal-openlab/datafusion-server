---
title: Arrow Flight Client (TBD)
weight: 20
---

{{< toc >}}

## Data Source Definitions

```shell
curl -X POST http://192.168.1.32:4000/session/86783d10-f286-49c2-b64f-57bb2eb5285f/datasource \
     -H 'Content-Type: application/json' \
     -d $'
[
  {
    "format": "flight",
    "name": "store_from_flight",
    "location": "http://server2:50051/d7891b5f-9bc5-45ca-8722-d1931b09e90e/store"
  }
]'
```
