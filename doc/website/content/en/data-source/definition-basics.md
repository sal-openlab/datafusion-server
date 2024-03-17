---
title: Definition Basics
weight: 20
---

{{< toc >}}

## Data Source Definition Overview

The example shown below defines a data source for a CSV format file located at `settings.server.data_dir` directory with consisting of two columns and including a header row.

```json
[
  {
    "format": "csv",
    "name": "example",
    "location": "file:///example.csv",
    "options": {
      "hasHeader": true
    },
    "schema": [
      {
        "name": "id",
        "dataType": "Integer",
        "nullable": false
      },
      {
        "name": "name",
        "dataType": "String",
        "nullable": true
      }
    ]
  }
]
```

This example specifies schema information, but the schema can be inferred using the `inferSchemaRows` option.

```json
[
  {
    "format": "csv",
    "name": "example",
    "location": "file:///example.csv",
    "options": {
      "hasHeader": true,
      "inferSchemaRows": 100
    }
  }
]
```

This instructs to prefetch data up to the first 100 rows to determine the schema. Increasing the number of rows to infer can improve the accuracy of the schema information, but also raises the cost of inference.

If `schema` is not defined, the schema is determined unconditionally using the default value of `inferSchemaRows`, which is `100`. Therefore, the previous example can be simplified as follows.

```json
[
  {
    "format": "csv",
    "name": "example",
    "location": "file:///example.csv",
    "options": {
      "hasHeader": true
    }
  }
]
```

### Details of Schema

For `schema`, define the necessary number of columns according to their order (for CSV, it needs to match the data order in the file; for JSON, the keys must match the `name` attribute). If the schema definition has fewer columns than the actual data, those extra columns will be ignored.

Details of each key in the `schema` array:

| Key | Value | Required |
| -- | -- | -- |
| `name` | Column Name | True |
| `dataType` | Specifies the type of data that a column can hold, such as String, Integer, Float, Boolean, etc. Each data type determines the nature of the data, how much space it occupies, and how the system processes it. | True |
| `nullable` | Indicates whether the column allows null values or not. If true, the column can contain null values; if false, it cannot. If omitted, it as `false`. | False |

#### Supported Data Types

Although `dataType` can be set to data types supported by Apache Arrow, not all are applicable. Below is a list of data types supported by DataFusion Server.

| Data Type | Description |
| -- | -- |
| Boolean | |
| Int8    | |
| Int16   | |
| Int32   | |
| Int64   | |
| UInt8   | |
| UInt16  | |
| UInt32  | |
| UInt64  | |
| Integer | Alias for Int64 |
| Float16 | |
| Float32 | |
| Float64 | |
| Float   | Alias for Float64 |
| Decimal128 | 128-bit width decimal value with maximum precision and scale is 38 digits |
| Decimal256 | 256-bit width decimal value with maximum precision and scale is 72 digits |
| Decimal | Alias for Decimal256 |
| Timestamp       | Counting the milliseconds from 00:00:00.000 on 1 January 1970 as UTC |
| TimestampSecond | Counting the seconds from 00:00:00 on 1 January 1970 as UTC |
| TimestampMicro  | Counting the microseconds from 00:00:00.000000 on 1 January 1970 as UTC |
| TimestampNano   | Counting the nanoseconds from 00:00:00.000000000 on 1 January 1970 as UTC |
| Date            | Elapsed time since 00:00:00.000 on 1 January 1970 in milliseconds |
| Time            | Elapsed time since midnight in milliseconds |
| Duration        | Measure of elapsed time in milliseconds |
| DurationSecond  | Measure of elapsed time in seconds |
| DurationMicro   | Measure of elapsed time in microseconds |
| DurationNano    | Measure of elapsed time in nanoseconds |
| String          | variable length string in Unicode with UTF-8 encoding


### About Options

The parameters that can be specified in `options` vary depending on the `format` and the scheme of `location`. Refer to the respective data source page for the options available in each format. Here, we will discuss options common to all formats.

#### `overwrite` option

Specifies whether to overwrite the table specified by `name` if it already exists in the session context. The default is `false`. Refer to information about the [Session Context]({{< ref "/session-context" >}}).

#### `inferSchemaRows` option

Although the schema has already been discussed, there is one additional point to note. If the format is `parquet`, the schema is predefined, so any specifications via `inferSchemaRows` or `schema` are completely ignored.

## Example

While it's not necessary to present another example, it should be noted that the data source definition is an array. Therefore, multiple data sources can be defined at once.

Here, shows a complete query sample that does not use a session with multiple data source definitions.

```shell
curl -X POST http://127.0.0.1:4000/dataframe/query \
     -H 'Content-Type: application/json' \
     -H 'Accept: text/csv' \
     -d $'{
  "dataSources": [
    {
      "name": "table1",
      "format": "parquet",
      "location": "file:///table1.parquet"
    },
    {
      "name": "table2",
      "format": "json",
      "location": "file:///table2.json",
      "options": {
        "inferSchemaRows": 300
      }
    }
  ]
  "query": {
    "sql": "SELECT id, name, category FROM table1, table2 WHERE table1.category = table2.category ORDER BY id"
  }
}'
```
