import os
import re
from typing import Literal

import pandas as pd
import numpy as np
import datetime

from . import excel_functions
from .config import OUTPUTS_DIR

zeros_by_fuente = {
    "costa_centro": 10000000,
    "costa_norte": 10000000000,
    "costa_sur": 1000,
    "selva_norte": 10000,
}


def silver_filtering(fuente, files_df):

    df = pd.read_parquet(os.path.join(OUTPUTS_DIR, f"{fuente}_compiled.parquet"))

    if fuente in ["costa_centro", "costa_norte"]:
        ## There are some Excel cells that have a 1.234E-10 value which is captured as a E+12...
        filter_zeros = df[(df["nominal"] > zeros_by_fuente[fuente])].index
        df.loc[filter_zeros, "nominal"] = 0
        df["date_time"] = df["date_time"].dt.round("H")
        if fuente == "costa_norte":
            ### Wrong datetime in the Excel File...
            df = df.merge(
                files_df[
                    files_df["date"].isin(
                        pd.date_range(start="2018-02-01", freq="MS", end="2018-11-01")
                    )
                ][["path", "date"]].rename(columns={"path": "excel_file"}),
                how="left",
                on="excel_file",
            )
            wrong_index = df[~df["date"].isna()].index
            df.loc[wrong_index, "date_time"] = df.loc[wrong_index, "date_time"].apply(
                lambda dt: dt.replace(year=2018)
            )
            df = df.drop(columns="date")
        df = df[df["nominal"] != 0]
        df = (
            df.groupby(by=["date_time", "excel_sheet", "variables"])
            .agg({"excel_file": "sum", "nominal": "mean"})
            .reset_index()
        )
        df["year-month"] = df["date_time"].dt.to_period("M")
        df["year"] = df["date_time"].dt.year
        df["month"] = df["date_time"].dt.month

    elif fuente in [
        "sierra_norte",
        "mineria",
        "costa_sur",
        "sierra_sur",
        "selva_norte",
        "selva_sur",
    ]:

        if fuente == "sierra_norte":
            df = df.rename(columns={"data_time": "date_time"})
        elif fuente in ["selva_norte", "costa_sur", "selva_sur"]:
            df = df.rename(columns={"date_and_time": "date_time"})

        df["date_time"] = df["date_time"].dt.round("H")
        df = df[df["excel_sheet"] != " January old Format"]

        if fuente in ["costa_sur", "selva_norte"]:
            filter_zeros = df[(df["nominal"] > zeros_by_fuente[fuente])].index
            df.loc[filter_zeros, "nominal"] = 0

        if fuente in ["costa_sur"]:
            ## Adhoc some columns with totals:
            adhoc = [
                "Chimbote 220/132 kV T/F",
                "LA  LIBERTAD",
                "LA LIBERTAD",
                "TRPTN",
                "LORET  O",
                "LORET O",
                "Selva Alta",
                "Selva  Baja",
            ]
            df["variables"] = df["variables"].str.strip()
            df = df[~df["variables"].isin(adhoc)]

        df = df.merge(
            files_df[["path", "date"]].rename(columns={"path": "excel_file"}),
            how="inner",
            on="excel_file",
        )
        df["date"] = df[["date", "excel_sheet"]].apply(
            lambda x: str(x["date"].year)
            + "-"
            + str(x["excel_sheet"].strip()[:3])
            + "-01",
            axis=1,
        )
        df["date"] = pd.to_datetime(df["date"])
        df["date_time_D"] = df["date_time"].dt.strftime("%Y-%m-01")
        df["flag"] = df["date_time_D"] == df["date"]
        df["new_year"] = df["date"].dt.year
        df["new_month"] = df["date"].dt.month
        df["day"] = df["date_time"].dt.day
        df["time"] = df["date_time"].dt.time
        df["date_time"] = df.apply(
            lambda x: str(x["new_year"])
            + "-"
            + str(x["new_month"])
            + "-"
            + str(x["day"])
            + " "
            + str(x["time"]),
            axis=1,
        )
        df["date_time"] = pd.to_datetime(
            df["date_time"], errors="coerce"
        )  #### change after error in mineria.
        df = df.drop_duplicates()

    return df
