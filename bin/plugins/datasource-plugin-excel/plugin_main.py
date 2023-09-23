# -*- coding: utf-8 -*-
""" datafusion-server data source connector plugin - excel
"""

import os
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

    system_config = kwargs.get("system_config")

    if is_debug(system_config):
        print(">> Python received arguments:", response_format, authority, path, schema, kwargs)

    if response_format != "arrow":
        raise ValueError("Unsupported format: " + response_format)

    file = os.path.join(system_config["data_dir"], authority)
    sheet = path.lstrip("/")
    skip_rows = valid_int(kwargs.get("skipRows"))
    num_rows = valid_int(kwargs.get("nRows"))
    header_row = valid_int(kwargs.get("headerRow")) if kwargs.get("headerRow") else 0
    has_header = True if kwargs.get("hasHeader") is None else kwargs.get("hasHeader")
    header_row = header_row if has_header else None

    df = pd.read_excel(
        file, sheet, skiprows=skip_rows, nrows=num_rows, header=header_row
    )

    if is_debug(system_config):
        print(df)

    return pa.RecordBatch.from_pandas(df)


def is_debug(system_config) -> bool:
    return (
        True
        if system_config["log_level"] == "trace"
        or system_config["log_level"] == "debug"
        else False
    )


def valid_int(value: any) -> int | None:
    return int(value) if value is not None else None
