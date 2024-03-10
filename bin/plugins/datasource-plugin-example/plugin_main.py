# -*- coding: utf-8 -*-

"""
Simple example for datafusion-server data source connector plugin.
Request Example:

http://localhost:4000/dataframe/query

```json
{
  "dataSources": [
    {
      "format": "arrow",
      "name": "hello",
      "location": "example://hello.world",
      "pluginOptions": {
        "foo": {
          "bar": [1, 2, 3],
          "baz": {
            "value": "text1"
          }
        }
      }
      "schema": [
        {
          "name": "foo",
          "dataType": "String",
          "nullable": true
        },
        {
          "name": "bar",
          "dataType": "Int64",
          "nullable": true
        }
      ]
    }
  ],
  "query": {
    "sql": "SELECT * FROM hello"
  },
  "response": {
    "format": "json"
  }
}
```

Debug Message:
```
>> Python received arguments:
format, authority, path: arrow auth /path
schema: foo: string, bar: int64
kwargs: {'foo': {'bar': [1.0, 2.0, 3.0], 'baz': {'value': 'text1'}}}
```

Response:
```json
[
  {
    "foo": "hello - arrow",
    "bar": 12345
  },
  {
    "foo": "world",
    "bar": 67890
  }
]
```
"""

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
        return '[\n{"foo":"hello - json","bar":12345},\n{"foo":"world","bar":67890}\n]\n'

    elif response_format == "ndJson":
        return '{"foo":"hello - ndJson","bar":12345}\n{"foo":"world","bar":67890}'

    elif response_format == "arrow":
        # Apache Arrow Python bindings - https://arrow.apache.org/docs/python/index.html

        # creates Schema when `schema` is `None`
        schema = schema if schema is not None else (
            pa.schema(
                [
                    pa.field("foo", pa.string()),
                    pa.field("bar", pa.int64())
                ]
            )
        )

        # prepares columnar data
        foo = pa.array(["hello - arrow", "world"])
        bar = pa.array([12345, 67890])

        # creates RecordBatch and return
        return pa.record_batch([foo, bar], schema=schema)

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
