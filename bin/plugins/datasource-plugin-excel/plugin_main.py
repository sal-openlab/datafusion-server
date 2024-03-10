# -*- coding: utf-8 -*-
""" datafusion-server data source connector plugin - excel
"""
from __future__ import annotations

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
        file, sheet, skiprows=skip_rows, nrows=num_rows, header=header_row
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
