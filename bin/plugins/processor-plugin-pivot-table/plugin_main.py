# -*- coding: utf-8 -*-
""" datafusion-server processor plugin - pivot table
"""

import logging
import pyarrow as pa
import pandas as pd


def main(source_df: pa.RecordBatch, **kwargs) -> pa.RecordBatch:
    """Plugin main function
    :param source_df: pyarrow dataframe
    :param kwargs: option parameters
    :return: results to datafusion-server processed arrow record batch
    """
    init_logging(kwargs.get("system_config"))

    logging.info("Starting: processor-plugin-pivot-table")
    logging.debug("plugin_main.py: main(), source_df=%s, kwargs=%s", source_df, kwargs)

    df: pd = source_df.to_pandas()

    logging.debug("pandas dataframe >>\n%s", df)

    pivot = df.pivot_table(
        values=kwargs.get("values"),
        index=kwargs.get("index"),
        columns=kwargs.get("columns"),
    )

    logging.debug("pandas pivot table >>\n%s", pivot)
    logging.info("Successfully completed: processor-plugin-pivot-table")

    return pa.RecordBatch.from_pandas(pivot)


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
