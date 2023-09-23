# -*- coding: utf-8 -*-
""" datafusion-server processor plugin - pivot table
"""

import pyarrow as pa
import pandas as pd


def main(source_df: pa.RecordBatch, **kwargs) -> pa.RecordBatch:
    """Plugin main function
    :param source_df: pyarrow dataframe
    :param kwargs: option parameters
    :return: results to datafusion-server processed arrow record batch
    """

    print("plugin_main.py: main(), kwargs=", kwargs)

    df: pd = source_df.to_pandas()
    print("pandas dataframe >>\n", df)

    pivot = df.pivot_table(
        values=kwargs.get("values"),
        index=kwargs.get("index"),
        columns=kwargs.get("columns"),
    )
    print("pandas pivot table >>\n", pivot)

    return pa.RecordBatch.from_pandas(pivot)
