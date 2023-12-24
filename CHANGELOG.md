# Changelog

## 0.8.12 (2023-12-24)

* Updates to DataFusion v34 and Arrow v49
  + https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/34.0.0.md
  + https://github.com/apache/arrow-rs/blob/49.0.0/CHANGELOG.md
    - Changed result value of `json::reader::infer_json_schema_from_seekable`.
* And other crates updated

## 0.8.11 (2023-11-17)

* Updates to DataFusion v33 and Arrow v48
  + https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/33.0.0.md
  + https://github.com/apache/arrow-rs/blob/48.0.0/CHANGELOG.md
    - `arrow_csv::WriterBuilder::has_headers` and `arrow_csv::ReaderBuilder::has_header` has been deprecated, use `with_header` instead.
* And other crates updated

## 0.8.10 (2023-10-14)

* Updates to DataFusion v32 and Arrow v47
  + https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/32.0.0.md
    - `SessionContext::with_config` has been deprecated, use `new_with_config` instead.
  + https://github.com/apache/arrow-rs/blob/47.0.0/CHANGELOG.md
* Updates to Tokio v1.33
  + https://github.com/tokio-rs/tokio/blob/master/tokio/CHANGELOG.md#1330-october-9-2023
* Addressed Clippy v0.1.73 warning: 'matching over () is more explicit'.

## 0.8.9 (2023-09-23)

* Prepared DataFusion Server to run in standalone and Docker environments

## 0.8.7 (2023-09-22)

* Internal version number under adjustment was skipped
* In the meantime, documentation and sample data were organized

## 0.8.4 (2023-09-16)

* Updates to DataFusion v31 and Arrow v46
  + https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/31.0.0.md
  + https://github.com/apache/arrow-rs/blob/46.0.0/CHANGELOG.md

## 0.8.3 (2023-09-03)

* Refactored CSV relatives
  + Drop deprecated date format parser options
  + Response by CSV format were supported

## 0.8.2 (2023-08-26)

* Updates to DataFusion v30 and Arrow v45
  + https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/30.0.0.md
  + https://github.com/apache/arrow-rs/blob/45.0.0/CHANGELOG.md
  + DataFusion v29 was not officially released, but all changes are applied in v30
    - https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/29.0.0.md
    - https://github.com/apache/arrow-rs/blob/44.0.0/CHANGELOG.md

## 0.8.1 (2023-08-13)

* Passing through Arrow schema structures to data source connector plugins when defined in data source definitions
  + If a schema is defined in the data source definition, Arrow schema structure is created on Rust and passed to plugin modules
  + See `plugins/datasource-plugin-example/plugin_main.py` for more detailed information

## 0.8.0 (2023-08-05) - Introducing built-in merge tables processor

* Built-in merge tables processor feature
  + Merges row and column direction from multiple tables
  + For more details, further reading openapi.v1.yaml

#### Row direction merge

Example tables

```
table1        table2        table3

 id title      id title      id title
--- --------  --- --------  --- --------
  1 title1     10 title11    11 title11
  2 title2     11 title12    12 title12
  3 title3
```

Request

```json
{
  "processor": {
    "mergeProcessors": [
      {
        "direction": "row",
        "baseTable": "table1",
        "targetTables": ["table2", "table3"]
      }
    ]
  }
}
```

After Processed

```
table1

 id title
--- --------
  1 title1
  2 title2
  3 title3
 10 title10
 11 title11
 11 title11
 12 title12
```

If the `distinct` option has enabled like follows

```json
{
  "processor": {
    "mergeProcessors": [
      {
        "direction": "row",
        "baseTable": "table1",
        "targetTables": ["table2", "table3"],
        "options": {
          "distinct": true
        }
      }
    ]
  }
}
```

Then,

```
table1

 id title
--- --------
  1 title1
  2 title2
  3 title3
 10 title10
 11 title11
 12 title12
```

Constraints

* All tables must have exactly the same schema

#### Column direction merge

Example tables

```
table1                   table2          table3

 id title    signal_id   id   signal     code signal
--- -------- ---------   ---- --------   ---- --------
  1 title1   S001        S001 signal1    S004 signal4
  2 title2   S002        S002 signal2
  3 title3   S003        S003 signal3
  4 title4   S004
  4 title5   S003
```

Request

```json
{
  "processor": {
    "mergeProcessors": [
      {
        "direction": "column",
        "baseTable": "table1",
        "targets": [
          {
            "table": "table2",
            "baseKeys": "signal_id",
            "targetKeys": "id"
          },
          {
            "table": "table3",
            "baseKeys": "signal_id",
            "targetKeys": "code"
          }
        ]
      }
    ]
  }
}
```

After Processed

```
table1

 id title    signal_id signal
--- -------- --------- ------------
  1 title1   S001      signal1
  2 title2   S002      signal2
  3 title3   S003      signal3
  4 title4   S004      signal4
  4 title5   S003      signal3
```

## 0.7.0 (2023-07-29)

* System information and statistics API
  + `/sysinfo` endpoint to results information
  + Details, further reading `endpoint.v1.yaml`

## 0.6.6 (2023-07-27)

* Updates to DataFusion v28 and Arrow v43
  + https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/28.0.0.md
  + https://github.com/apache/arrow-rs/blob/43.0.0/CHANGELOG.md

## 0.6.5 (2023-07-01)

* Updates to DataFusion v27 and Arrow v42
  + https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/27.0.0.md
  + https://github.com/apache/arrow-rs/blob/42.0.0/CHANGELOG.md
* Updates to PyO3 v0.19
  + https://pyo3.rs/main/changelog#0190---2023-05-31
* Updates Python interpreter to v3.11.4
  + https://docs.python.org/release/3.11.4/whatsnew/changelog.html#python-3-11-4

## 0.6.4 (2023-06-10)

* Updates to DataFusion v26 and Arrow v40
  + https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/26.0.0.md
  + https://github.com/apache/arrow-rs/blob/40.0.0/CHANGELOG.md
  + Change `RecordBatchWriter` related specifications in Arrow v40, currently mismatched with DataFusion v26
    - https://github.com/apache/arrow-rs/pull/4206

## 0.6.3 (2023-06-04)

* Can be passed multiple record batches to Python plugin

This query results could not be passed to the Python processor plugin. Because generated column(s) to separate multiple record batches.

```json
{
  "sql": "SELECT row_number() over() -1 AS month_offset, avg_temp FROM global_temp WHERE country='Japan'"
}
```

## 0.6.2 (2023-05-31)

* Preserves property order in JSON responses

Originally field order is `foo`, `bar` to sorts the dictionary order.

* Hopefully,

```json
{
  "foo": 1,
  "bar": 2
}
```

* Instead of

```json
{
  "bar": 2,
  "foo": 1
}
```

## 0.6.1 (2023-05-28)

* Refactored data source connector plugin relatives
  + Plugin can results the Arrow (record batch) format
    - Therefore, passing through pandas DataFrame to datafusion-server
    - Microsoft Excel data source via the pandas connector plugin included

```json
[
  {
    "format": "arrow",
    "name": "example",
    "location": "excel://example-workbook.xlsx/Sheet1",
    "pluginOptions": {
      "skipRows": 2
    }
  }
]
```

## 0.6.0 (2023-05-27) - Introducing processor module plugin feature

* Processor module plugin by Python feature
  + Plugin will be triggered after the SQL query
  + Plugin supported [pandas](https://pandas.pydata.org/) (+NumPy) for big data analysis
    - Example pivot-table processor plugin included
  + Multiple processor module can be chained

```json
{
  "sql": "SELECT * FROM Table1, Table2, Table3 WHERE ...",
  "postProcessors": [
    {
      "module": "module1",
      "pluginOptions": {
        "foo": "Options for processor plugin"
      }
    },
    {
      "module": "module2"
    }
  ]
}
```

```mermaid
---
title: Processor Module Chain
---
flowchart LR
  Table1["RecordBatch 1"]
  Table2["RecordBatch 2"]
  Table3["RecordBatch n"]

  Query["datafusion-server\nSQL query"]

  Table1 & Table2 & Table3 --> Query

  Module1["Processor\nModule 1"]
  Module2["Processor\nModule 2"]

  Response["datafusion-serve\nResponder"]

  Query--"RecordBatch"-->Module1

  subgraph pythonPlugin[Python Processor Plugin]
    Module1--"RecordBatch"-->Module2
  end

  Module2--"RecordBatch"-->Response

  classDef rustInstance fill:#08427b,stroke:#0b4884,color:#fff
  classDef pythonInterpreter fill:#1168bd,stroke:#0b4884,color:#fff
  style pythonPlugin fill:none,stroke:#ccc,stroke-width:2px,stroke-dasharray: 5 5,color:#666

  class Table1,Table2,Table3,Query,Response rustInstance
  class Module1,Module2 pythonInterpreter
```

## 0.5.5 (2023-05-24)

* Updates to DataFusion v25 and Arrow v39
  + Dropped specify the date format functionality to effected with CSV reader
     - Arrow CSV parser recognize the date with `^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d.\d{1,n}$`, if a trailing `z` is present it is not recognized as a date
  　　- Probably, **schema specification is a must be required for CSV reading**　　
  + https://github.com/apache/arrow-datafusion/blob/main/dev/changelog/25.0.0.md
  + https://github.com/apache/arrow-rs/blob/39.0.0/CHANGELOG.md

## 0.5.4 (2023-05-23) - Hot bugfix release

* Core dumps (Panic!) occurs when reading into the huge size of data frames
  + DataFusion's specification has changed the handling of RecordBatch size

## 0.5.3 (2023-05-13)

* Updates to DataFusion v24 and Arrow v38
* Recommended rustc to v1.69+
* Bundled Python v3.11.3 for Docker container

## 0.5.2 (2023-05-05)

* Completely re-write JSON, Raw JSON, CSV and Parquet serializer
  + Now can be saved to file system by native JSON
* Force over-write option in `DataSourceOption` to effects read/write data source operations

```json
 [
   {
     "format": "parquet",
     "name": "apis",
     "location": "file:///public-apis.parquet",
     "options": {
       "overwrite": true
     }
   }
]
```


## 0.5.1 (2023-05-03)

* Can not be accessed to data source details generated by DDL/DML

```json
{
  "sql": "CREATE TABLE table1 { column1 TEXT }"
}
```

```sh
$ curl http://localhost:4000/session/:id/datasource/table1
```

* Occurres `Can not found data source 'table1'`, but the following response should be obtained

```json
{
  "name": "table1",
  "schema": [
    {
      "name": "column1",
      "dataType": "String",
      "nullable": true
    }
  ]
}
```

## 0.5.0 (2023-05-01)

* Save selected data sources to local file system
  + Currently NOT supported native JSON, use raw JSON alternatively 

## 0.4.2 (2023-04-30)

* Updates latest module dependencies
  + Follows DataFusion v23.0
  + Arrow officially dropped native JSON support

## 0.4.1 (2023-03-28)

* Supports both native JSON and raw JSON response format
  + Arrow deprecates to native JSON from v34.0

## 0.4.0 (2023-02-24) - Introducing Python plugin feature

* Data source connector plugin by Python 3.7+
* FIELD system EventDatabase plugin (scheme: `fsdb://`)

## 0.3.0 (2023-01-19)

* TTL based session management APIs for multi-user context sessions
* Can be specified schema to data sources strictly

## 0.2.0 (2023-01-09) - Introducing DataFusion

* Multiple data source queries by DataFusion SQL engine
* Supports external REST server
* Can be used with JSONPath in JSON response

## 0.1.1 (2023-01-02)

* Supports JSON and Parquet in data source format and JSON responder

## 0.1.0 (2022-09-14) - Initial internal release

* Supports CSV to Apache Arrow responder
* Assumed data converter to pass to Finos/Perspective as main use case
