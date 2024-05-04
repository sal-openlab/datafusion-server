---
title: Data Source Connector Plugin
weight: 10
---

{{< toc >}}

## Data Source Connector Plugin Basics

Data source connector plugins are related to the `location` specified in the [data source definition]({{< ref "/data-source/definition-basics" >}}). DataFusion Server selects the data source connector plugin based on the scheme part of the `location`.

```json
{
  "dataSources": [
    {
      "format": "arrow",
      "name": "example",
      "location": "example://hello-world"
    }
  ]
}
```

In this example, if an `example` scheme exists as a data source connector plugin, the example data source connector plugin is invoked.

Upon startup, Data Fusion Server scans subdirectories from the root of the plugin directory (configured by `server.plugin_dir`), looking for directories where a `plugin_def.toml` file exists and `plugin_type = "datasource"` is defined.

The `bin/plugins/datasource-plugin-example` directory in the repository contains the following `plugin_def.toml` file as a sample.

```toml
# Plugin definition file for datafusion-server

[general]
title = "Example data source plugin (json, ndJson, arrow)"
version = "1.2.0"
plugin_type = "datasource"
scheme = "example"

[plugin]
file = "plugin_main.py"
entry = "main"
```
The file indicated by `plugin.file` is the implementation of the data source connector plugin, with the entry point being the function shown in `plugin.entry`. The plugin implementation can be split across multiple files; it simply needs to match the specified entry file.

plugin_main.py:
```python
"""
Simple example for datafusion-server data source connector plugin.
Request Example:
"""

import logging
import pyarrow as pa


def main(response_format: str, authority: str, path: str, schema: pa.Schema, **kwargs):
    """Plugin main function
    :param response_format: request format (json, ndJson, csv, ...)
    :param authority: authority in URI
    :param path: path in URI or None
    :param schema: schema definitions or None
    :param kwargs: option parameters or None
    :return: results to datafusion-server encoded by UTF-8 string
    """
    init_logging(kwargs.get("system_config"))

    logging.info("Starting: datasource-plugin-example")
    logging.debug(
        "response_format = %s, authority = %s, path = %s, kwargs = %s",
        response_format,
        authority,
        path,
        kwargs,
    )

    if response_format == "json":
        return (
            '[\n{"foo":"hello - json","bar":12345},\n{"foo":"world","bar":67890}\n]\n'
        )

    elif response_format == "ndJson":
        return '{"foo":"hello - ndJson","bar":12345}\n{"foo":"world","bar":67890}'

    elif response_format == "arrow":
        # Apache Arrow Python bindings - https://arrow.apache.org/docs/python/index.html

        # creates Schema when `schema` is `None`
        schema = (
            schema
            if schema is not None
            else (
                pa.schema(
                    [
                        pa.field("foo", pa.string()),
                        pa.field("bar", pa.int64()),
                        pa.field("col_vector", pa.list_(pa.int32())),
                        pa.field(
                            "col_map",
                            pa.map_(pa.string(), pa.float32()),
                        ),
                    ]
                )
            )
        )

        # prepares columnar data
        foo = pa.array(["hello - arrow", "world"])
        bar = pa.array([12345, 67890])
        col_vector = pa.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]])
        col_map = pa.array(
            [[{"key": "key1", "value": 123.4}], [{"key": "key2", "value": 567.8}]],
            type=pa.map_(pa.string(), pa.float32())
        )

        # creates RecordBatch and return
        return pa.record_batch([foo, bar, col_vector, col_map], schema=schema)

    else:
        raise ValueError("Unsupported format: " + response_format)


def init_logging(system_config):
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=(
            logging.DEBUG
            if system_config["log_level"] == "trace"
            or system_config["log_level"] == "debug"
            else logging.INFO
        ),
    )
```

The entry function receives the format specified in the data source definition, the authority and path parts of the URL, and, if a schema is specified in the data source definition, the Arrow Schema object generated on the Rust side. All contents of `pluginOptions` from the data source definition are passed as a Python dictionary in `kwargs`.

In this example, the response is generated based on the first argument, `response_format`. For `json` or `ndJson`, it simply returns the content as a string to the Rust side. For `arrow`, the example returns a Record Batch in Arrow format, which includes columns like `List` and `Map`, to Rust. While it is possible to pass schema and `pluginOptions`, they are ignored in this example.

The `system_config` key in `kwargs` is set with values such as the log level, Data Fusion Server version, `data_dir`, and `plugins_dir` on the Rust side, so refer to it as needed.

If the process cannot continue due to unsupported formats or other issues, raise a Python exception. On the Rust side, an error occurrence in the Python interpreter and the exception message will be responded.

## Real World Data Source Connector Plugin

The previous example (example plugin) might not be very meaningful as it does not actually fetch data from a data source. Here, we introduce an example where data is actually retrieved from a data source (Microsoft Excel format). Reading and parsing Excel format files are performed using the [pandas](https://pandas.pydata.org/) {{< icon "external-link" >}}library. While the use of pandas here might seem a bit basic, it is expected to be extensively utilized in [Post Processor Plugins]({{< ref "post-processor-plugin" >}}), so please add it to the Python library dependencies.

Begin by preparing a definition file. It looks something like this.

datasource-plugin-excel/plugin_def.toml
```toml
[general]
title = "Microsoft Excel 2003+ data source connector plugin"
version = "1.0.0"
plugin_type = "datasource"
scheme = "excel"

[plugin]
file = "plugin_main.py"
entry = "main"
```

And the implementation of the Data Source Connector Plugin goes like this.

plugin_main.py
```python
""" datafusion-server data source connector plugin - excel
"""

import os
import logging
import pyarrow as pa
import pandas as pd


def main(response_format: str, authority: str, path: str, schema: pa.Schema, **kwargs):
    """Plugin main function
    :param response_format: request format (arrow)
    :param authority: file path and name
    :param path: sheet name
    :param schema: schema definitions or None
    :param kwargs: option parameters
    :return: results to datafusion-server encoded by Arrow record batch
    """
    init_logging(kwargs.get("system_config"))

    logging.info("Starting: datasource-plugin-excel")
    logging.debug(
        "response_format = %s, authority = %s, path = %s, kwargs = %s",
        response_format,
        authority,
        path,
        kwargs,
    )

    if response_format != "arrow":
        raise ValueError("Unsupported format: " + response_format)

    file = os.path.join(kwargs.get("system_config")["data_dir"], authority)
    sheet = path.lstrip("/")
    skip_rows = valid_int(kwargs.get("skipRows"))
    num_rows = valid_int(kwargs.get("nRows"))
    header_row = valid_int(kwargs.get("headerRow")) if kwargs.get("headerRow") else 0
    has_header = True if kwargs.get("hasHeader") is None else kwargs.get("hasHeader")
    header_row = header_row if has_header else None

    df = pd.read_excel(
        file, sheet, skiprows=skip_rows, nrows=num_rows, header=header_row, dtype_backend="pyarrow"
    )

    logging.debug("Parsed dataframe >>\n%s", df)
    logging.info("Successfully completed: datasource-plugin-excel")

    return pa.RecordBatch.from_pandas(df)


def init_logging(system_config):
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=(
            logging.DEBUG
            if system_config["log_level"] == "trace"
            or system_config["log_level"] == "debug"
            else logging.INFO
        ),
    )


def valid_int(value: any) -> int | None:
    return int(value) if value is not None else None
```

This implementation does not support formats other than Arrow. Communications between the Plugin and Rust are most efficient when using Arrow (Record Batch), as it prevents issues with missing values and data type impedance mismatches. Therefore, it is recommended to use Arrow format for exchanges whenever possible.

The code is straightforward, but let's explain it step by step.

1. Initialize the Python logging system based on the log level set in `kwargs["system_config"]`.
2. If any value other than `arrow` is specified for the first argument `response_format`, throw an exception and terminate.
3. Retrieve the file path, file name, sheet name, presence of headers, and number of rows from the parameters passed in `kwargs`.
4. Pass parameters to the pandas `read_excel()` function to load and parse the Excel format file. The result will be a pandas DataFrame. From pandas v2.0.0 onward, Arrow can be used as the backend for DataFrames. This can be specified with `dtype_backend="pyarrow"`.
5. Convert the pandas DataFrame to an Arrow Record Batch using the PyArrow `RecordBatch.from_pandas()` function and return it to Rust. If using pandas v2.0.0 or later with `dtype_backend="pyarrow"` specified, the conversion is near the zero-cost.

The data source definition for using this data source connector plugin is as follows.

```json
{
  "dataSources": [
    {
      "format": "arrow",
      "name": "table1",
      "location": "excel://example-workbook.xlsx/Sheet1",
      "pluginOptions": {
        "skipRows": 2
      }
    }
  ]
}
```

All files, including information on the necessary Python libraries, are located in the `bin/plugins/datasource-plugin-excel` directory, and you are encouraged to refer to them.
