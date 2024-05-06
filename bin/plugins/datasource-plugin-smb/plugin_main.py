# -*- coding: utf-8 -*-

"""
SMB/CIFS datafusion-server data source connector plugin.
using smbprotocol(smbclient) - https://pypi.org/project/smbprotocol

Request Example:

```json
{
  "dataSources": [
    {
      "format": "csv",
      "name": "example",
      "location": "smb://server/shared-folder/example.csv",
      "pluginOptions": {
        "user": "foo",
        "password": "password"
      }
    }
  ],
}
```

or

```json
{
  "dataSources": [
    {
      "format": "parquet",
      "name": "example",
      "location": "smb://user:password@server/shared-folder/example.parquet"
    }
  ],
}
```
"""

import logging
import re
import smbclient
from typing import Union, Any


def main(response_format: str, authority: str, path: str, schema: Any, **kwargs):
    """Plugin main function
    :param response_format: request format (json, ndJson, csv, ...)
    :param authority: authority in URI
    :param path: path in URI or None
    :param schema: schema definitions (ignores this plugin)
    :param kwargs: option parameters or None
    :return: results to specific file format
    """
    init_logging(kwargs.get("system_config"))

    logging.info("Starting: datasource-plugin-smb")
    logging.debug(
        "response_format = %s, authority = %s, path = %s, kwargs = %s",
        response_format,
        authority,
        path,
        kwargs,
    )

    if response_format == "arrow" or response_format == "avro":
        raise ValueError("Unsupported format: " + response_format)

    if path is None or path == "":
        raise ValueError("Must be required folder and filename")

    authority = parse_authority(authority)
    server = authority.get("server")
    port = valid_int(authority.get("port"), valid_int(kwargs.get("port"), 445))
    username = (
        authority.get("user")
        if authority.get("user") is not None
        else kwargs.get("user")
    )
    password = (
        authority.get("password")
        if authority.get("password") is not None
        else kwargs.get("password")
    )
    connection_timeout = valid_int(kwargs.get("timeout"), 60)
    encrypt = kwargs.get("encrypt", False)

    unc = r"\\" + server + path.replace("/", "\\")
    mode = "rb" if response_format == "parquet" else "r"

    if server is None or server == "":
        raise ValueError("Must be required server name or address")

    logging.debug(f"unc={unc}, mode={mode}, username={username}")

    with smbclient.open_file(
        unc,
        mode=mode,
        port=port,
        username=username,
        password=password,
        connection_timeout=connection_timeout,
        encrypt=encrypt,
    ) as fd:
        data = fd.read()

    smbclient.reset_connection_cache()
    logging.info("Successfully completed: datasource-plugin-smb")

    return data


def parse_authority(url):
    pattern = re.compile(
        r"(?:(?P<user>[^:]+):(?P<password>[^@]+)@)?(?P<server>[^:]+)(?::(?P<port>\d+))?"
    )
    match = pattern.match(url)

    if match:
        return match.groupdict()
    else:
        return None


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


def valid_int(value: any, default: Union[int, None] = None) -> Union[int, None]:
    if value is not None:
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"Cannot convert {value} to integer")
    else:
        return default
