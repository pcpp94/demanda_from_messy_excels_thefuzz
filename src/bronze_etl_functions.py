import os
import re

import pandas as pd
import numpy as np
import datetime

from . import excel_functions
from .config import DATA_EXTRA_DIR


def get_final_paths(directory, fuente):
    files = get_files_path(directory)
    files_clean = files_filter_by_year(files)
    fuente_files = fuente_paths_filtering(files_clean, fuente=fuente)
    return fuente_files


def get_files_path(directory):
    """
    Getting the files' PATHS in the directory and subdirectories.

    Args:
        directory (str): Path to Directory

    Returns:
        file_paths (list): List with all the files' paths.
    """
    file_paths = []  # List to store file paths
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_paths.append(file_path)
    print("Files with respective paths retrieved.")
    return file_paths


def files_filter_by_year(files, year=2015, pattern=r"\\(\d{4})\\"):
    """
    Filters strings in a list by the year within the path.

    Args:
        files (list): List of paths
        pattern (regexp, optional): Regex formula for years. Defaults to r'\(\d{4})\'.

    Returns:
        files_clean (list): List with all the files' paths after specified year.
    """
    files_clean = []
    for file in files:
        if int(re.search(pattern, file).group(1)) < year:
            continue
        else:
            files_clean.append(file)

    print(f"Files filtered: From year {year} until most recent.")
    return files_clean


def fuente_paths_filtering(files_clean, fuente: str):
    """
    Filtering the

    Args:
        files_clean (list): List with all the files' paths after specified year.
        fuente (str]): Name of the fuente: costa_centro, costa_norte, etc.

    Returns:
        fuente_parse_list: Final list of Excel files to parse depending on version, modified time, etc.
    """

    fuente_files = adhoc_filtering(files_clean, fuente=fuente)
    fuente_parse_list, fuente_df = adhoc_filtering_2(
        fuente_files=fuente_files, fuente=fuente
    )
    return (fuente_parse_list, fuente_df)


def parsing_excel_files(fuente_parse_list, fuente: str):
    """
    Parsing all the latest Excel Files for a fuente, using many logics to get clean data.

    Args:
        fuente_parse_list (list): List of Excel Files to parse.
        fuente (str): Name of the fuente

    Returns:
        combined_df: Pandas DataFrame containing the parsed data for the fuente.
    """

    set_of_sheets = []
    all_dataframes = []

    if fuente in ["costa_centro", "costa_norte"]:
        # Iterate over each file in the directory
        for file_path in fuente_parse_list:
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            set_of_sheets = set_of_sheets + xls.sheet_names
            sheets = [
                sheet
                for sheet in xls.sheet_names
                if sheet not in ["Summary", "Matrix", "Sheet1"]
            ]
            for sheet_name in sheets:
                # Read the sheet into a dataframe
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                # Cleaning data
                df = excel_functions.basic_xlsx_clean(df, threshold=2)
                # Conditional in case there are more than 1 table from basic cleaning:
                if isinstance(df, list):
                    for dfito in df:
                        if dfito.shape[0] == 0 or dfito.shape[1] < 3:
                            continue
                        dfito = excel_functions.cleaning_by_column_type(dfito)
                        if dfito.shape[0] == 0 or dfito.shape[1] < 3:
                            continue
                        dfito = excel_functions.dropping_totals(dfito)
                        if dfito.shape[0] == 0 or dfito.shape[1] < 2:
                            continue
                        dfito["Excel File"] = file_path
                        dfito["Excel Sheet"] = sheet_name
                        dfito = excel_functions.tabular_melting(dfito)
                        if dfito.shape[0] == 0 or dfito.shape[1] < 2:
                            continue

                        # headers in lower-case, no symbolds:
                        dfito.columns = dfito.columns.str.lower()
                        dfito.columns = dfito.columns.str.replace("/", "_")
                        dfito.columns = dfito.columns.str.replace(" ", "_")

                        # Concatenating all Dataframes
                        all_dataframes.append(dfito)
                else:

                    if df.shape[0] == 0 or df.shape[1] < 3:
                        continue
                    df = excel_functions.cleaning_by_column_type(df)
                    if df.shape[0] == 0 or df.shape[1] < 3:
                        continue
                    df = excel_functions.merge_check(df, file_path, sheet_name)
                    if df.shape[0] == 0 or df.shape[1] < 3:
                        continue
                    df = excel_functions.dropping_totals(df)
                    if df.shape[0] == 0 or df.shape[1] < 2:
                        continue
                    if fuente in ["costa_centro", "costa_norte"]:
                        df = excel_functions.dropping_2(df, fuente=fuente)
                    if df.shape[0] == 0 or df.shape[1] < 2:
                        continue
                    df["Excel File"] = file_path
                    df["Excel Sheet"] = sheet_name
                    df = excel_functions.tabular_melting(df)
                    if df.shape[0] == 0 or df.shape[1] < 2:
                        continue
                    # headers in lower-case, no symbolds:
                    df.columns = df.columns.str.lower()
                    df.columns = df.columns.str.replace("/", "_")
                    df.columns = df.columns.str.replace(" ", "_")
                    df = excel_functions.merged_dfs(df)
                    if df.shape[0] == 0 or df.shape[1] < 2:
                        continue
                    # Concatenating all Dataframes
                    all_dataframes.append(df)
        ### Ad-hoc for Excel files with WRONG datetimes:
        for num, temp in [
            (i, df)
            for (i, df) in enumerate(all_dataframes)
            if (pd.api.types.is_datetime64_any_dtype(df["date_time"]) == False)
        ]:
            temp = temp[temp["date_time"].str.len() == 16]
            temp["date"] = temp["date_time"].apply(
                lambda x: datetime.datetime.strptime(x[:8], "%d.%m.%y")
            )
            temp["time"] = temp["date_time"].apply(
                lambda x: (
                    datetime.datetime.strptime(x[:-2][-5:], "%H:%M")
                    if x[:-2][-5:-3] != "24"
                    else np.nan
                )
            )
            temp["time"] = temp["time"].apply(
                lambda x: (
                    x - datetime.timedelta(hours=1) if (pd.isna(x) == False) else np.nan
                )
            )
            temp["time"] = temp["time"].fillna("23:00")
            temp["date_time"] = pd.to_datetime(
                temp["date"].astype(str)
                + " "
                + temp["time"].astype(str).str.slice(start=-8),
                format="%Y-%m-%d %X",
            )
            temp.drop(columns=["date", "time"], inplace=True)
            all_dataframes[num] = temp
        ### We need to get rid of the "missing columns"
        ### These have a hidden column which is copying the datetime values...
        [
            df.drop(columns="missing", inplace=True)
            for df in all_dataframes
            if "missing" in df.columns
        ]
        # Concatenate all the dataframes into one
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        # Save the data.
        if (
            combined_df[
                pd.to_datetime(combined_df["date_time"], errors="coerce").isna()
            ]["nominal"].sum()
            == 0
        ):
            combined_df["date_time"] = pd.to_datetime(
                combined_df["date_time"], errors="coerce"
            )
            combined_df = combined_df[~combined_df["date_time"].isna()].reset_index(
                drop=True
            )
            print(f"Parsed Excel Files for fuente: {fuente}")
            return combined_df
        else:
            print("Check wrong datetime values")

    elif fuente in ["sierra_norte"]:
        # Iterate over each file in the directory
        for file_path in fuente_parse_list:
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            set_of_sheets = set_of_sheets + xls.sheet_names
            sheets = [
                sheet
                for sheet in xls.sheet_names
                if sheet not in ["Summary", "Matrix", "Sheet1"]
            ]
            for sheet_name in sheets:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                df = excel_functions.repeat_merged_cells(df, file_path, sheet_name)
                df = excel_functions.basic_xlsx_clean_one_table(df, threshold=6)
                df = excel_functions.getting_time_series_frame(
                    df
                )  ## It's a tiny dataset so it's just one table per sheet.
                df = excel_functions.cleaning_by_column_type(df)
                to_drop = [
                    "intercambio",
                    "neto",
                    "suministro",
                    "suministrado",
                    "entrega",
                    "entregado",
                    "perdida",
                    "perdidas",
                    "missing",
                ]
                df = excel_functions.dropping_extras(df, to_drop)
                df = excel_functions.dropping_totals(df)
                df = excel_functions.fix_merged_headers(df, -2)
                df["Excel File"] = file_path
                df["Excel Sheet"] = sheet_name
                df = excel_functions.tabular_melting(df)
                # headers in lower-case, no symbolds:
                df.columns = df.columns.str.lower()
                df.columns = df.columns.str.replace("/", "_")
                df.columns = df.columns.str.replace(" ", "_")
                df.columns = df.columns.str.replace("&_", "")
                # Concatenating all Dataframes
                all_dataframes.append(df)
        # Concatenate all the dataframes into one
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        if "data_time" in combined_df.columns:
            combined_df = combined_df.rename(columns={"data_time": "date_time"})
        return combined_df

    elif fuente in ["mineria"]:
        # Iterate over each file in the directory
        for file_path in fuente_parse_list:
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            sheets = list(
                excel_functions.excel_sheets_with_revisions(xls.sheet_names).values()
            )
            for sheet_name in sheets:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                df = excel_functions.repeat_merged_cells(df, file_path, sheet_name)
                df = excel_functions.basic_xlsx_clean_one_table(df, threshold=3)
                df = excel_functions.getting_time_series_frame(df)
                df = excel_functions.cleaning_by_column_type(df)
                to_drop = [
                    "intercambio",
                    "neto",
                    "suministro",
                    "suministrado",
                    "entrega",
                    "entregado",
                    "perdida",
                    "perdidas",
                    "missing",
                    "commulative",
                    "actual",
                    "cumulative",
                ]
                df = excel_functions.dropping_extras(df, to_drop)
                df = excel_functions.dropping_totals(df)
                df = df.rename(
                    columns={df.select_dtypes("datetime").columns[0]: "date_time"}
                )
                df["Excel File"] = file_path
                df["Excel Sheet"] = sheet_name
                df = excel_functions.tabular_melting(df)
                # headers in lower-case, no symbolds:
                df.columns = df.columns.str.lower()
                df.columns = df.columns.str.replace("/", "_")
                df.columns = df.columns.str.replace(" ", "_")
                # Concatenating all Dataframes
                all_dataframes.append(df)
        # Concatenate all the dataframes into one
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df

    elif fuente in ["costa_sur"]:
        for file_path in fuente_parse_list:
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            sheets = list(
                excel_functions.excel_sheets_with_revisions(xls.sheet_names).values()
            )
            for sheet_name in sheets:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                df = excel_functions.repeat_merged_cells(df, file_path, sheet_name)
                df = excel_functions.basic_xlsx_clean_one_table(df, threshold=3)
                df = excel_functions.getting_time_series_frame(df)
                df = excel_functions.cleaning_by_column_type(df)
                try:
                    al_hayl = [x for x in df.columns if "al hayl" in x.lower()][0]
                    df = df.rename(
                        columns={
                            al_hayl: re.sub(
                                r"suministro",
                                "",
                                str(
                                    re.sub(r"()total", "", al_hayl, flags=re.IGNORECASE)
                                ),
                                flags=re.IGNORECASE,
                            )
                        }
                    )
                except:
                    pass
                try:
                    credit = [x for x in df.columns if "credit" in x.lower()][0]
                    df = df.rename(
                        columns={
                            credit: re.sub(r"total", "", credit, flags=re.IGNORECASE)
                        }
                    )
                except:
                    pass
                to_drop = [
                    "intercambio",
                    "neto",
                    "suministro",
                    "suministrado",
                    "entrega",
                    "entregado",
                    "perdida",
                    "perdidas",
                    "missing",
                    "1000.0",
                ]
                df = excel_functions.dropping_extras(df, to_drop)
                df = excel_functions.dropping_totals(df)
                df = excel_functions.fix_merged_headers(df, -2)
                df["Excel File"] = file_path
                df["Excel Sheet"] = sheet_name
                df = excel_functions.tabular_melting(df)
                df["variables"] = df["variables"].str.replace(
                    "Compensated", "Chimbote 220/132 kV T/F"
                )
                df["variables"] = df["variables"].str.replace("\n", " ")
                # headers in lower-case, no symbolds:
                df.columns = df.columns.str.lower()
                df.columns = df.columns.str.replace("/", "_")
                df.columns = df.columns.str.replace(" ", "_")
                # Concatenating all Dataframes
                all_dataframes.append(df)
        # Concatenate all the dataframes into one
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df

    elif fuente in ["sierra_sur"]:
        for file_path in fuente_parse_list:
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            sheets = list(
                excel_functions.excel_sheets_with_revisions(xls.sheet_names).values()
            )
            for sheet_name in sheets:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                df = excel_functions.repeat_merged_cells(df, file_path, sheet_name)
                df = excel_functions.basic_xlsx_clean_one_table(df, threshold=2)
                df = excel_functions.cleaning_by_column_type(df)
                to_drop = [
                    "intercambio",
                    "neto",
                    "suministro",
                    "suministrado",
                    "entrega",
                    "entregado",
                    "perdida",
                    "perdidas",
                    "missing",
                    "1000.0",
                    "actual",
                    "700",
                ]
                df = excel_functions.dropping_extras(df, to_drop)
                df = excel_functions.dropping_totals(df)
                col_split_max = max([len(col.split("|")) for col in df.columns])
                df = excel_functions.fix_merged_headers(df, -(col_split_max - 2))
                df = df.rename(
                    columns={df.select_dtypes("datetime").columns[0]: "date_time"}
                )
                df["Excel File"] = file_path
                df["Excel Sheet"] = sheet_name
                df = excel_functions.tabular_melting(df)
                # headers in lower-case, no symbolds:
                df.columns = df.columns.str.lower()
                df.columns = df.columns.str.replace("/", "_")
                df.columns = df.columns.str.replace(" ", "_")
                # Concatenating all Dataframes
                all_dataframes.append(df)
        # Concatenate all the dataframes into one
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df

    elif fuente in ["selva_norte"]:
        for file_path in fuente_parse_list:
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            sheets = list(
                excel_functions.excel_sheets_with_revisions(xls.sheet_names).values()
            )
            for sheet_name in sheets:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                df = excel_functions.repeat_merged_cells(df, file_path, sheet_name)
                df = excel_functions.basic_xlsx_clean_one_table(df, threshold=3)
                df = excel_functions.getting_time_series_frame(df)
                df = excel_functions.cleaning_by_column_type(df)
                to_drop = [
                    "intercambio",
                    "neto",
                    "suministro",
                    "suministrado",
                    "entrega",
                    "entregado",
                    "perdida",
                    "perdidas",
                    "missing",
                    "1000.0",
                    "actual",
                    "energy ",
                    "above ",
                    "upto",
                    "up to",
                    "firm",
                    "add",
                ]
                df = excel_functions.dropping_extras(df, to_drop)
                df = excel_functions.dropping_totals(df)
                df = excel_functions.fix_merged_headers(df, -3)
                to_drop2 = ["ICALIM", "Southern", "Firm|Firm|Firm", "Add|Add|Add"]
                for col in df.columns:
                    if col.strip() in to_drop2:
                        df = df.drop(columns=col)
                df["Excel File"] = file_path
                df["Excel Sheet"] = sheet_name
                df = excel_functions.tabular_melting(df)
                df["variables"] = df["variables"].str.replace(
                    "Compensated", "Chimbote 220/132 kV T/F"
                )
                df["variables"] = df["variables"].str.replace("\n", " ")
                # headers in lower-case, no symbolds:
                df.columns = df.columns.str.lower()
                df.columns = df.columns.str.replace("/", "_")
                df.columns = df.columns.str.replace(" ", "_")
                # Concatenating all Dataframes
                all_dataframes.append(df)
        # Concatenate all the dataframes into one
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df

    elif fuente in ["selva_sur"]:
        for file_path in fuente_parse_list:
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            sheets = list(
                excel_functions.excel_sheets_with_revisions(xls.sheet_names).values()
            )
            for sheet_name in sheets:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                df = excel_functions.repeat_merged_cells(df, file_path, sheet_name)
                df = excel_functions.basic_xlsx_clean_one_table(df, threshold=2)
                df = excel_functions.cleaning_by_column_type(df)
                # to_drop = ['intercambio', 'neto', 'suministro', 'suministrado', 'entrega', 'entregado', 'perdida', 'perdidas', 'missing', '1000.0', 'actual', '700']
                to_drop = [
                    "intercambio",
                    "neto",
                    "suministro",
                    "suministrado",
                    "entrega",
                    "entregado",
                    "perdida",
                    "perdidas",
                    "missing",
                    "actual",
                ]
                df = excel_functions.dropping_extras(df, to_drop)
                df = excel_functions.dropping_totals(df)
                df = excel_functions.fix_merged_headers(df, -1)
                df = df.rename(
                    columns={df.select_dtypes("datetime").columns[0]: "date_time"}
                )
                df["Excel File"] = file_path
                df["Excel Sheet"] = sheet_name
                df = excel_functions.tabular_melting(df)
                # headers in lower-case, no symbolds:
                df.columns = df.columns.str.lower()
                df.columns = df.columns.str.replace("/", "_")
                df.columns = df.columns.str.replace(" ", "_")
                # Concatenating all Dataframes
                all_dataframes.append(df)
        # Concatenate all the dataframes into one
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df


def adhoc_filtering(files_clean, fuente):

    if fuente.lower() == "costa_norte":
        fuente_files = [file for file in files_clean if "costa_norte" in file]
        fuente_files = [file for file in fuente_files if file[-4:] != ".msg"]
        fuente_files = [file for file in fuente_files if "~$" not in file]
        fuente_files = [
            file for file in fuente_files if "Reconciliation Data" not in file
        ]
        fuente_files = [
            file for file in fuente_files if "Energia Entregada" not in file
        ]
    elif fuente.lower() == "costa_centro":
        fuente_files = [file for file in files_clean if "costa_centro" in file]
        fuente_files = [
            file for file in fuente_files if "costa_centro" in file.split("\\")[-1]
        ]
        fuente_files = [file for file in fuente_files if file[-4:] != ".msg"]
        fuente_files = [file for file in fuente_files if "~$" not in file]
        fuente_files = [
            file for file in fuente_files if "Energia Entregada" not in file
        ]
        fuente_files = [
            file
            for file in fuente_files
            if re.search(excel_functions.combined_regex, file, re.IGNORECASE)
        ]
    elif fuente.lower() == "sierra_norte":
        fuente_files = [file for file in files_clean if "sierra_norte" in file]
        fuente_files = [file for file in fuente_files if file[-4:] != ".msg"]
        fuente_files = [file for file in fuente_files if "~$" not in file]
    elif fuente.lower() == "mineria":
        fuente_files = [file for file in files_clean if "mineria" in file]
        fuente_files = [file for file in fuente_files if file[-4:] != ".msg"]
        fuente_files = [file for file in fuente_files if ".xlsx" in file]
        fuente_files = [
            file for file in fuente_files if "~$" not in file
        ] + get_files_path(os.path.join(DATA_EXTRA_DIR, "mineria_xlsx"))
    elif fuente.lower() == "costa_sur":
        fuente_files = (
            [file for file in files_clean if "costa_s" in file]
            + [file for file in files_clean if " sur_costa" in file]
            + [
                os.path.join(
                    DATA_EXTRA_DIR,
                    "costa_sur",
                    "2021 Energia Entregada to sur_costa.xlsx",
                )
            ]
        )
        fuente_files = [file for file in fuente_files if file[-4:] != ".msg"]
        fuente_files = [file for file in fuente_files if "~$" not in file]
    elif fuente.lower() == "sierra_sur":
        fuente_files = [file for file in files_clean if "Kuelap" in file]
        fuente_files = [file for file in fuente_files if file[-4:] != ".msg"]
        fuente_files = [file for file in fuente_files if "~$" not in file]
    elif fuente.lower() == "selva_norte":
        fuente_files = [file for file in files_clean if "selva_norte" in file]
        fuente_files = [file for file in fuente_files if file[-4:] != ".msg"]
        fuente_files = [file for file in fuente_files if "~$" not in file]
    elif fuente.lower() == "selva_sur":
        fuente_files = [
            file for file in files_clean if "invoice costa_centro" in file
        ] + [file for file in files_clean if "LIM" in file]
        fuente_files = [file for file in fuente_files if file[-4:] != ".msg"]
        fuente_files = [file for file in fuente_files if "~$" not in file]

    return fuente_files


def adhoc_filtering_2(fuente_files, fuente):

    if fuente in ["costa_centro", "costa_norte"]:

        fuente_df = pd.DataFrame()

        for file in fuente_files:
            aux = {}
            aux["date"] = [excel_functions.retrieve_date(file)]
            aux["version"] = [excel_functions.find_version(file)]
            aux["path"] = [file]
            aux["modified"] = [os.path.getmtime(file)]
            fuente_df = pd.concat([fuente_df, pd.DataFrame(aux)])

        fuente_df["version"] = pd.to_numeric(fuente_df["version"])
        fuente_df = fuente_df.merge(
            fuente_df.groupby("date")[["version"]].max().reset_index(),
            how="left",
            on=["date", "version"],
            indicator=True,
        )
        fuente_df = fuente_df[fuente_df["_merge"] == "both"].drop(columns=["_merge"])
        fuente_df = fuente_df.merge(
            fuente_df.groupby("date")[["modified"]].max().reset_index(),
            how="left",
            on=["date", "modified"],
            indicator=True,
        )
        fuente_df = fuente_df[fuente_df["_merge"] == "both"].drop(columns=["_merge"])
        fuente_df["year"] = fuente_df["date"].dt.year
        fuente_df["month"] = fuente_df["date"].dt.month
        fuente_parse_list = fuente_df["path"].to_list()

        print(
            f"Filtered {fuente} Excel Files list to the latest version and latest modified files."
        )
        return (fuente_parse_list, fuente_df)

    if fuente in [
        "sierra_norte",
        "mineria",
        "costa_sur",
        "sierra_sur",
        "selva_norte",
        "selva_sur",
    ]:

        fuente_df = pd.DataFrame()

        for file in fuente_files:
            aux = {}
            aux["date"] = [excel_functions.retrieve_year(file)]
            aux["path"] = [file]
            aux["modified"] = [os.path.getmtime(file)]
            fuente_df = pd.concat([fuente_df, pd.DataFrame(aux)])

        fuente_df = fuente_df.merge(
            fuente_df.groupby("date")[["modified"]].max().reset_index(),
            how="left",
            on=["date", "modified"],
            indicator=True,
        )
        fuente_df = fuente_df[fuente_df["_merge"] == "both"].drop(columns=["_merge"])

        fuente_df["year"] = fuente_df["date"].dt.year
        fuente_df["month"] = fuente_df["date"].dt.month

        fuente_parse_list = fuente_df["path"].to_list()

        print(
            f"Filtered {fuente} Excel Files list to the latest version and latest modified files."
        )
        return (fuente_parse_list, fuente_df)
