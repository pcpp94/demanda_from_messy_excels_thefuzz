import pandas as pd
import datetime
import os
from typing import Literal
import sys

# .py script
from .. import bronze_etl_functions, silver_etl_functions, gold_etl_functions
from ..config import DEMANDA_DIR, OUTPUTS_DIR, DataflowSkipException


class Demanda_Client:
    """
    Object to perform the ETL of the Demanda Data.
    Bronze, Silver, "Gold" layers.
    """

    ALLOWED_VALUES = [
        "costa_centro",
        "costa_norte",
        "sierra_norte",
        "mineria",
        "costa_sur",
        "sierra_sur",
        "selva_norte",
        "selva_sur",
    ]

    def __init__(
        self,
        fuente: Literal[
            "costa_centro",
            "costa_norte",
            "sierra_norte",
            "mineria",
            "costa_sur",
            "sierra_sur",
            "selva_norte",
            "selva_sur",
        ],
    ):
        self.fuente = fuente
        if self.fuente not in self.ALLOWED_VALUES:
            raise ValueError(
                f"Invalid fuente name: {fuente}. Must be one of {self.ALLOWED_VALUES}."
            )
        self.files_list = bronze_etl_functions.get_final_paths(
            DEMANDA_DIR, fuente=fuente
        )[0]
        self.files_df = bronze_etl_functions.get_final_paths(
            DEMANDA_DIR, fuente=fuente
        )[1]

    def etl_demanda_data_bronze(self, only_update: bool = False):
        if only_update == False:
            fuente_files = self.files_list
            df = bronze_etl_functions.parsing_excel_files(
                fuente_files, fuente=self.fuente
            )
            df.to_csv(
                os.path.join(OUTPUTS_DIR, f"{self.fuente}_compiled.csv"), index=None
            )
            df.to_parquet(
                os.path.join(OUTPUTS_DIR, f"{self.fuente}_compiled.parquet"),
                index=None,
            )
            print(f"Saved Bronze Layer for {self.fuente}")
        else:
            df = self.update_table()
            df.to_csv(
                os.path.join(OUTPUTS_DIR, f"{self.fuente}_compiled.csv"), index=None
            )
            df.to_parquet(
                os.path.join(OUTPUTS_DIR, f"{self.fuente}_compiled.parquet"),
                index=None,
            )
            print(f"Saved Bronze Layer for {self.fuente}")

    def etl_demanda_data_silver(self):
        df = silver_etl_functions.silver_filtering(
            fuente=self.fuente, files_df=self.files_df
        )
        df.to_csv(os.path.join(OUTPUTS_DIR, f"{self.fuente}_silver.csv"), index=None)
        df.to_parquet(
            os.path.join(OUTPUTS_DIR, f"{self.fuente}_silver.parquet"), index=None
        )
        print(f"Saved Silver Layer for {self.fuente}")

    def etl_demanda_data_gold(self):
        df = gold_etl_functions.gold_filtering(fuente=self.fuente)
        df.to_csv(os.path.join(OUTPUTS_DIR, f"{self.fuente}_gold.csv"), index=None)
        df.to_parquet(
            os.path.join(OUTPUTS_DIR, f"{self.fuente}_gold.parquet"), index=None
        )
        print(f"Saved Gold Layer for {self.fuente}")

    def update_table(self):
        demanda_files = self.files_df.copy()
        bronze_file = [
            os.path.join(OUTPUTS_DIR, x)
            for x in os.listdir(OUTPUTS_DIR)
            if ("compiled" in x) and ("parquet" in x) and (self.fuente in x)
        ][0]
        current_df = pd.read_parquet(bronze_file)
        demanda_files["table_modified"] = datetime.datetime.fromtimestamp(
            os.path.getmtime(bronze_file)
        )
        demanda_files["modified"] = demanda_files["modified"].apply(
            lambda x: datetime.datetime.fromtimestamp(x)
        )
        demanda_files["to_change"] = (
            demanda_files["table_modified"] <= demanda_files["modified"]
        )
        missing_files_list = (
            demanda_files[demanda_files["to_change"] == True]["path"].unique().tolist()
        )
        missing_files_df = demanda_files[demanda_files["path"].isin(missing_files_list)]

        if len(missing_files_list) == 0:
            raise DataflowSkipException(
                f"Skipping {self.fuente} due to no missing data."
            )
        self.files_df = missing_files_df
        self.files_list = missing_files_list
        df = bronze_etl_functions.parsing_excel_files(
            self.files_list, fuente=self.fuente
        )
        df_check_duplicate_date = current_df[
            current_df["excel_file"].isin(missing_files_df["path"].tolist())
        ]
        if len(df_check_duplicate_date) == 0:
            df = pd.concat([df, current_df]).reset_index(drop=True)
        else:
            current_df = current_df[
                ~current_df["excel_file"].isin(
                    df_check_duplicate_date["excel_file"].tolist()
                )
            ]
            df = pd.concat([df, current_df]).reset_index(drop=True)

        self.files_list = bronze_etl_functions.get_final_paths(
            DEMANDA_DIR, fuente=self.fuente
        )[0]
        self.files_df = bronze_etl_functions.get_final_paths(
            DEMANDA_DIR, fuente=self.fuente
        )[1]

        return df
