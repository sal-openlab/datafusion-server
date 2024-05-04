---
title: What's Data Sources
weight: 10
---

{{< toc >}}

## Data Source Definitions

Data sources are defining information from local files, object stores, API responses, etc. to be converted into the Arrow memory model of DataFusion's dataframe.

The dataset retrieved from the data source definition is stored in-memory as a RecordBatch in Arrow. And from the DataFusion Server's point of view, they are equivalent to SQL tables.

## Supported Format

The DataFusion Server supports the following standard data sources in the following formats.

* Arrow
* JSON
* ndJSON (new-line delimited JSON)
* CSV
* Parquet
* Avro
* Arrow Flight gRPC

## Data Source Connector Plugins

The data source can be easily extended by implementing the data source connector plugin in [Python](https://www.python.org/) {{< icon "external-link" >}}, details of which are explained in [Data Source Connector Plugin]({{< ref "/using-python-plugin/datasource-connector-plugin" >}}).

## Data Source Format and Location Matrix

| format \\ location | http(s)                   | grpc(+tls)                | file                           | plugin                    |
| -- | :--: | :--: | :-- | :--: |
| Arrow              |                           |                           |                                | {{< icon "fa-circle-check" >}} |
| JSON               | {{< icon "fa-circle-check" >}} |                           | {{< icon "fa-circle-check" >}} {{< icon "download" >}} | {{< icon "fa-circle-check" >}} |
| ndJSON             | {{< icon "fa-circle-check" >}} |                           | {{< icon "fa-circle-check" >}} {{< icon "download" >}} | {{< icon "fa-circle-check" >}} |
| CSV                | {{< icon "fa-circle-check" >}} |                           | {{< icon "fa-circle-check" >}} {{< icon "download" >}} | {{< icon "fa-circle-check" >}} |
| Parquet            | {{< icon "fa-circle-check" >}} |                           | {{< icon "fa-circle-check" >}} {{< icon "download" >}} | {{< icon "fa-circle-check" >}} |
| Avro               |                           |                           | {{< icon "fa-circle-check" >}}      |                           |
| Arrow Flight       |                           | {{< icon "fa-circle-check" >}} |                                |                           |

* {{< icon "fa-circle-check" >}} Supported
* {{< icon "download" >}} Save feature supported
