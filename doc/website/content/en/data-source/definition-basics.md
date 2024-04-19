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
| `Boolean` | |
| `Int8`    | |
| `Int16`   | |
| `Int32`   | |
| `Int64`   | |
| `UInt8`   | |
| `UInt16`  | |
| `UInt32`  | |
| `UInt64`  | |
| `Integer` | Alias for `Int64` |
| `Float16` | |
| `Float32` | |
| `Float64` | |
| `Float`   | Alias for `Float64` |
| `Decimal128`      | 128-bit width decimal value with maximum precision and scale is 38 digits |
| `Decimal256`      | 256-bit width decimal value with maximum precision and scale is 72 digits |
| `Decimal`         | Alias for `Decimal256` |
| `Date32`          | Elapsed time since 1 January 1970 in days                         |
| `Date64`          | Elapsed time since 00:00:00.000 on 1 January 1970 in milliseconds |
| `Date`            | Alias for `Date64` |
| `Time32`          | Elapsed time since midnight in seconds or milliseconds     |
| `Time64`          | Elapsed time since midnight in microseconds or nanoseconds |
| `Time`            | Alias for `Time32` |
| `Timestamp`       | Counting the specific precisions from 00:00:00 on 1 January 1970 as UTC |
| `Duration`        | Measure of elapsed time in specific precisions |
| `Interval`        | “Calendar” based interval in `YearMonth`, `DayTime`, `MonthDayNano` |
| `String`          | variable length string in Unicode with UTF-8 encoding |
| `List`            | Array of some data type with variable length, Can be stored up to 2,147,483,647 elements |
| `LargeList`       | Array of some data type with variable length, Can be stored up to 9,223,372,036,854,775,807 elements |
| `Struct`          | Nested data types that contains a number of sub-fields |
| `Map`             | Map is a nestable key-value store |
| `Union`           | Can represent slots of differing types |

Data types removed in DataFusion Server v0.11.0
| Data Type | Description |
| -- | -- |
| `TimestampSecond` | Counting the seconds from 00:00:00 on 1 January 1970 as UTC |
| `TimestampMicro`  | Counting the microseconds from 00:00:00.000000 on 1 January 1970 as UTC |
| `TimestampNano`   | Counting the nanoseconds from 00:00:00.000000000 on 1 January 1970 as UTC |
| `DurationSecond`  | Measure of elapsed time in seconds |
| `DurationMicro`   | Measure of elapsed time in microseconds |
| `DurationNano`    | Measure of elapsed time in nanoseconds |

#### Simple Data Types

A simple data type, such as `Int64`, is defined as follows.

```json
{
  "schema": [
    {
      "name": "quantity",
      "dataType": "Int64",
      "nullable": true
    }
  ]
}
```

#### Complex Data Types

Complex data types have different definitions individually. Let's look at each one by one.

##### Decimal128, Decimal256

First, here are examples of `Decimal128` and `Decimal256`.

```json
{
  "schema": [
    {
      "name": "global_population",
      "dataType": {
        "Decimal256": {
          "precision": 8,
          "scale": 2
        }
      },
      "nullable": false
    }
  ]
}
```

* precision is the total number of digits
* scale is the number of digits past the decimal

In this example, it is possible to represent `123456.78`.

https://docs.rs/arrow-schema/51.0.0/arrow_schema/enum.DataType.html#variant.Decimal256

##### Timestamp

An example of a `Timestamp` would look as follows.

```json
{
  "schema": [
    {
      "name": "accessed_at",
      "dataType": {
        "Timestamp": {
          "unit": "Millisecond",
          "timezone": "UTC"
        }
      },
      "nullable": false
    }
  ]
}
```

The possible precisions for `unit` are `Second`, `Millisecond`, `Microsecond`, and `Nanosecond`. The `timezone` is optional, and the default value when omitted is `UTC`.

https://docs.rs/arrow-schema/51.0.0/arrow_schema/enum.DataType.html#variant.Timestamp

##### Duration

The definition of `Duration` is similar to that of `Timestamp`, but conceptually, it does not have a timezone.

```json
{
  "schema": [
    {
      "name": "processed_time",
      "dataType": {
        "Timestamp": {
          "unit": "Microsecond"
        }
      },
      "nullable": true
    }
  ]
}
```

https://docs.rs/arrow-schema/51.0.0/arrow_schema/enum.DataType.html#variant.Duration

##### Interval

`Interval` represents a period according to the calendar. The units that can be specified for `unit` are `YearMonth`, `DayTime`, and `MonthDayNano`.

```json
{
  "schema": [
    {
      "name": "construction_period",
      "dataType": {
        "Interval": {
          "unit": "YearMonth"
        }
      },
      "nullable": true
    }
  ]
}
```

https://docs.rs/arrow-schema/51.0.0/arrow_schema/enum.DataType.html#variant.Interval

##### List, LargeList

`List` and `LargeList` differ only in the maximum number of array elements they can hold.

```json
{
  "schema": [
    {
      "name": "purity_numbers",
      "dataType": {
        "List": "UInt64"
      },
      "nullable": true
    }
  ]
}
```

Elements can be nested, and there is no logical upper limit to the depth of nesting.

```json
{
  "schema": [
    {
      "name": "trend_by_years",
      "dataType": {
        "LargeList": {
          "List": "String"
        }
      },
      "nullable": true
    }
  ]
}
```

Nestable elements are not limited to `List`; `Map`, `Struct`, and any other combinations can also be defined.

https://docs.rs/arrow-schema/51.0.0/arrow_schema/enum.DataType.html#variant.List

##### Struct

A `Struct` is a composite of any combination of fields.

```json
{
  "schema": [
    {
      "name": "person",
      "dataType": {
        "Struct": [
          {
            "name": "name",
            "dataType": "String",
            "nullable": false
          },
          {
            "name": "age",
            "dataType": "UInt8",
            "nullable": true
          }
        ]
      },
      "nullable": true
    }
  ]
}
```

https://docs.rs/arrow-schema/51.0.0/arrow_schema/enum.DataType.html#variant.Struct

##### Map

A `Map` is a key-value store. Any data type can be specified for `key` and `value`, and nesting is also possible here.

```json
{
  "schema": [
    {
      "name": "revenue_by_area",
      "dataType": {
        "Map": {
          "key": "String",
          "value": "Int64",
          "ordered": false
        }
      },
      "nullable": true
    }
  ]
}
```

If `ordered` is set to `true`, the entries are stored sorted by key. `ordered` is optional, and the default value when omitted is `false`.

https://docs.rs/arrow-schema/51.0.0/arrow_schema/enum.DataType.html#variant.Map

##### Union

`Union` can represent slots of differing types.

```json
{
  "schema": [
    {
      "name": "age_or_name",
      "dataType": {
        "Union": {
          "types": [
            {"id": 1, "type": "Int32"},
            {"id": 2, "type": "String"}
          ],
          "mode": "Sparse"
        }
      },
      "nullable": true
    }
  ]
}
```

The `id` array is used to indicate which type of union each `Field` belongs to. The memory layout for `Union` differs depending on whether the type is `Dense` or `Sparse`. In a dense union, all elements are contiguously placed in memory, whereas in a sparse union, they are not. The `id` array serves as a mapping to show which field each piece of data belongs to.

https://docs.rs/arrow-schema/51.0.0/arrow_schema/enum.DataType.html#variant.Union

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
