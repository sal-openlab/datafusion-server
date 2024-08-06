---
title: Seamless Query
weight: 30
---

{{< toc >}}

## Query

Seamlessly integrate with existing data sources.

### Session Based Query Example

1. Creating session context

```shell
$ curl http://127.0.0.1:4000/session/create?id=session1
```

2. Read the existing data from data source

```shell
curl -X POST http://127.0.0.1:4000/session/session1/datasource \
     -H 'Content-Type: application/json' \
     -d $'
[
  {
    "format": "parquet"
    "name": "superstore",
    "location": "file:///superstore.parquet"
  }
]'
```

3. Query with external database with existing table

When namespace is defined as `pg1`, `JOIN` queries can be seamlessly performed with tables
within the DataFusion context as shown below.

```sh
$ curl -X POST http://127.0.0.1:4000/session/session1/query \
     -H 'Content-Type: application/sql' \
     -H 'Accept: text/csv' \
     -d $'
SELECT M.city, M.population, SUM("superstore.Sales") AS sales
  FROM superstore, table1@pg1 M
 WHERE "superstore.City" = M.city
 GROUP BY "superstore.City"
'
```

### Session-less Query Example

```sh
$ curl -X POST http://127.0.0.1:4000/dataframe/query \
     -H 'Content-Type: application/json' \
     -H 'Accept: text/csv' \
     -d $'
{
  "dataSources": [
    {
      "format": "parquet",
      "name": "superstore",
      "location": "file:///superstore.parquet"
    }
  ]
  "query": {
    "sql": "SELECT M.city, M.population, SUM(\"superstore.Sales\") AS sales\n FROM superstore, table1@pg1 M\n WHERE \"superstore.City\" = M.city\n GROUP BY \"superstore.City\""
  }
}'
```
