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
      "location": "simple://hello.world",
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
    :param response_format: request format (json, rawJson, csv, ...)
    :param authority: authority in URI
    :param path: path in URI or None
    :param schema: schema definitions or None
    :param kwargs: option parameters or None
    :return: results to datafusion-server encoded by UTF-8 string
    """
    print(">> Python received arguments:")
    print("format, authority, path:", response_format, authority, path)
    print("schema:", schema)
    print("kwargs:", kwargs)

    if response_format == "json":
        return '[\n{"foo":"hello - json","bar":12345},\n{"foo":"world","bar":67890}\n]\n'

    elif response_format == "rawJson":
        return '{"foo":"hello - rawJson","bar":12345}\n{"foo":"world","bar":67890}'

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
