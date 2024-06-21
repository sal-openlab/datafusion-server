---
title: Delta Lake
weight: 90
---

{{< toc >}}

## Data Source Definition

```json
{
  "format": "deltalake",
  "name": "example",
  "location": "s3://my-bucket/delta-table"
}
```

Specify `deltalake` in the `format` to indicate a Delta Lake table. And in the `location`,
specify the root directory of the Delta Lake table. The schemes that can be specified in
the `location` refer to supported
[Format and Location Matrix]({{< ref "/data-source/what-datasource#data-source-format-and-location-matrix" >}}).

## Options

### Specified Table Version (Delta Table Time Travel Feature)

Specify the version number of the Delta table in the `version` key of the `options` block.
The initial version is `0`. If omitted, the latest version will be used.

```json
{
  "format": "deltalake",
  "name": "example",
  "location": "file:///delta-tables/delta-table",
  "options": {
    "version": 0
  }
}
```

Here is the official documentation on Delta Lake’s ‘Time
Travel’: [Delta Lake Time Travel](https://delta.io/blog/2023-02-01-delta-lake-time-travel/) {{< icon "external-link" >}}

## Usage Example

```sh
$ curl -X POST http://127.0.0.1:4000/dataframe/query \
     -H 'Content-Type: application/json' \
     -d $'
{
  "dataSources": [
    {
      "format": "deltalake",
      "name": "example",
      "location": "file:///delta-table"
    }
  ]
  "query": {
    "sql": "SELECT * FROM example"
  }
}'
```

## Footnote

Accessing Delta Lake tables utilizes the
[delta-kernel](https://crates.io/crates/delta_kernel) {{< icon "external-link" >}}crate.
I’m filled with gratitude toward the members who have achieved high-quality results early on in
the [delta-kernel-rs](https://github.com/delta-incubator/delta-kernel-rs) {{< icon "external-link" >}}project.

At present, the functionality for reading Delta Lake tables has been implemented, but it is anticipated that in the near
future, functionalities such as writing operations and vacuuming will be implemented. Data Fusion Server also plans to
expand its capabilities accordingly.

Please refer to the [blog](https://delta.io/blog/delta-kernel/) {{< icon "external-link" >}}post for more information on
the Delta Kernel.
