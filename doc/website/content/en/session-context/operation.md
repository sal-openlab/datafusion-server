---
title: Operation
weight: 20
---

{{< toc >}}

## Create A New Session Context

To create a session context with the default keep-alive (`session.keep_alive` in setting), simply send a request to the `/session/create` endpoint by method `GET`.

```shell
curl http://127.0.0.1:4000/session/create
```

In the response, you can check the context ID, creation time, and remaining time to live. This context ID will be required for subsequent access to the session context.

```json
{
  "id": "281b509a-bc80-4afa-8b06-181d191c555b",
  "created": "2024-03-17T06:16:58.165Z",
  "ttl": 3600000
}
```

If the default settings have not been changed, the time-to-live (TTL) is 3600 seconds. However, a custom keep-alive duration can be set for each session as shown below. If the `keepAlive` value is set to `0`, the DataFusion Server will not automatically delete that session context.

```shell
curl http://127.0.0.1:4000/session/create?keepAlive=1800
```

The TTL is reset each time there is some access to the context. If left idle until the TTL reaches zero, the session context will be deleted.

## Inspecting A Existing Session Context

To check the current state of a session context, issue a request to the `/session/:id` endpoint by method `GET`.

```shell
curl http://127.0.0.1:4000/session/281b509a-bc80-4afa-8b06-181d191c555b
```

You will see the TTL value decreasing.

```json
{
  "id": "281b509a-bc80-4afa-8b06-181d191c555b",
  "created": "2024-03-17T06:16:58.165Z",
  "ttl": 2747231
}
```

## Delete A Existing Session Context

To delete a session context, send a request to the `/session/:id` endpoint by method `DELETE`.

```shell
curl -X DELETE http://127.0.0.1:4000/session/281b509a-bc80-4afa-8b06-181d191c555b
```

## Adding Tables from Data Sources

To load a table or tables into the context from data sources, request to the `/session/:id/datasource` endpoint by `POST`.

```shell
curl -X POST http://127.0.0.1:4000/session/b5307f5a-39a6-46e8-84c8-d441fef86897/datasource \
     -H 'Content-Type: application/json' \
     -d $'
[
  {
    "format": "csv",
    "name": "store",
    "location": "file:///superstore.csv",
    "options": {
      "inferSchema": 1000,
      "hasHeader": true
    }
  },
  {
    "format": "parquet"
    "name": "apis",
    "location": "file:///public-apis.parquet"
  }
]'
```

Refer to [Data Source Definition]({{< ref "/data-source" >}}) for details on the data source definition part. If the table is successfully created in the context from the data source, an HTTP status code of `204` indicating success will be returned.

## List of Existing Tables

To obtain a list of the tables currently existing in the context, send a `GET` request to the `/session/:id/datasource` endpoint.

```shell
curl http://127.0.0.1:4000/session/b5307f5a-39a6-46e8-84c8-d441fef86897/datasource
```

If a table was added in the previous example, the following response would be returned.

```json
[
  "store",
  "apis"
]
```

## Inspect A Detail of the Table

To obtain detailed information about a table, access the `/session/:id/datasource/:table` endpoint.

```shell
curl http://127.0.0.1:4000/session/b5307f5a-39a6-46e8-84c8-d441fef86897/datasource/store
```

Here, we are requesting detailed information about the `store` table loaded from the `superstore.csv` data source. The data types of the inferred columns will also be revealed here.

```json
{
  "name": "store",
  "location": "file:///superstore.csv",
  "schema": [
    {
      "name": "Category",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "City",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Container",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Customer ID",
      "dataType": "Int64",
      "nullable": true
    },
    {
      "name": "Customer Name",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Customer Segment",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Department",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Item ID",
      "dataType": "Int64",
      "nullable": true
    },
    {
      "name": "Item",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Order Date",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Order ID",
      "dataType": "Int64",
      "nullable": true
    },
    {
      "name": "Order Priority",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Postal Code",
      "dataType": "Int64",
      "nullable": true
    },
    {
      "name": "Region",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Row ID",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Ship Date",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Ship Mode",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "State",
      "dataType": "String",
      "nullable": true
    },
    {
      "name": "Discount",
      "dataType": "Float64",
      "nullable": true
    },
    {
      "name": "Order Quantity",
      "dataType": "Int64",
      "nullable": true
    },
    {
      "name": "Product Base Margin",
      "dataType": "Float64",
      "nullable": true
    },
    {
      "name": "Profit",
      "dataType": "Int64",
      "nullable": true
    },
    {
      "name": "Sales",
      "dataType": "Int64",
      "nullable": true
    },
    {
      "name": "Shipping Cost",
      "dataType": "Int64",
      "nullable": true
    },
    {
      "name": "Unit Price",
      "dataType": "Int64",
      "nullable": true
    }
  ]
}
```

If the inferred data types are not as intended, you should either increase the `inferSchemaRows` value or define the schema using `schema`. For example, `Int64` might be too large for "Postal Code" column.

## Remove A Table From Context

To remove a table from the context, send a request to the `/session/:id/datasource/:name` endpoint by the `DELETE` method.

```shell
curl -X DELETE http://127.0.0.1:4000/session/b5307f5a-39a6-46e8-84c8-d441fef86897/datasource/store
```

## Refresh A Existing Table

Reloading a table according to the data source definition can be useful, for example, when the data source points to a file that is updated in real-time, or when the data source is obtained from a REST API.

A refresh request is made to the `/session/:id/datasource/:name/refresh` endpoint by the `GET` method.


```shell
curl http://127.0.0.1:4000/session/b5307f5a-39a6-46e8-84c8-d441fef86897/datasource/apis/refresh
```

However, there are caveats to refreshing. It cannot be used on tables for which data source definitions have not been made, such as tables created within the context using DDL like `CREATE TABLE table1`.

## TBD...
