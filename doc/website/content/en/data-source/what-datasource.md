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

* JSON
* ndJSON (new-line delimited JSON)
* CSV
* Parquet
* Avro

For JSON format, it supports files stored on the local file system or JSON that can be obtained from the REST API.

## Data Source Connector Plugins

The data source can be easily extended by implementing the Connector Plugin in Python, details of which are explained in [Using Python Plugin Modules]({{< ref "/using-python-plugin" >}}).

