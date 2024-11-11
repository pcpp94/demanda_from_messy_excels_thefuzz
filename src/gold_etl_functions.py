import os
import re
from typing import Literal

import pandas as pd
import numpy as np
import datetime

from . import excel_functions
from .config import OUTPUTS_DIR

zeros_by_fuente = {"costa_centro": 999999, "costa_norte": 400200}

doubled = {
    "sierra_norte": ["AprilApril"],
    "mineria": ["JuneJune", "NovemberNovember"],
    "costa_sur": [
        "DecemberDecember",
        "JanuaryJanuary",
        "FebruaryFebruary",
        "SeptemberSeptember",
        "SEPTSEPT",
    ],
    "sierra_sur": [
        "FebruaryFebruary",
        "JunJun",
        "JuneJune",
        "SepSep",
        "AprilApril",
        "SeptemberSeptember",
        "NovemberNovember",
        "NovNov",
        "Feb-23Feb-23",
        "Feb-24Feb-24",
        "Apr-23Apr-23",
        "Jun-23Jun-23",
        "Sep-23Sep-23",
        "Nov-23Nov-23",
        "Dec-23Dec-23",
    ],
    "selva_norte": ["FebruaryFebruary", "SEptSEpt"],
    "selva_sur": [
        "FebruaryFebruary",
        "JunJun",
        "JuneJune",
        "SepSep",
        "AprilApril",
        "SeptemberSeptember",
        "NovemberNovember",
        "Feb-23Feb-23",
        "Feb-24Feb-24",
        "Apr-23Apr-23",
        "Jun-23Jun-23",
        "Sep-23Sep-23",
        "Nov-23Nov-23",
        "Dec-23Dec-23",
    ],
}


def gold_filtering(fuente):

    df = pd.read_parquet(os.path.join(OUTPUTS_DIR, f"{fuente}_silver.parquet"))

    if fuente in ["costa_centro"]:
        df = df[df["variables"] != "Net"]

    # Basic cleaning - we'll be merging by these afterwards.
    df["variables"] = df["variables"].str.strip()

    if fuente in ["costa_centro", "costa_norte"]:
        # Variablesh have - and _ instead of . sometimes
        df["variables"] = df["variables"].str.replace("-", ".")
        df["variables"] = df["variables"].str.replace("_", ".")

    if fuente in ["costa_centro", "costa_norte"]:
        # From graphs by excel_sheet >> Some values are wrong > too high by Excel reading error.
        indices = df[df["nominal"] > zeros_by_fuente[fuente]].index
        df = df.drop(indices, axis=0).reset_index(drop=True)

    if fuente in ["costa_centro"]:
        # At the end of notebook >> Cochabamba has a different structure and we did not drop "Net" column.
        new_gasco = df[df["excel_sheet"] == "Cochabamba"].copy()
        new_gasco["ex_im"] = new_gasco["variables"].apply(
            lambda x: "export" if "export" in x.lower() else "import"
        )
        new_gasco["variables"] = (
            new_gasco["variables"]
            .str.split("|", expand=True)[0]
            .apply(lambda x: x if "Export" not in x else np.nan)
            .fillna(method="ffill")
        )
        new_gasco["nominal"] = new_gasco["nominal"] * new_gasco["ex_im"].apply(
            lambda x: -1 if "export" in x else 1
        )
        new_gasco = new_gasco.drop(columns=["ex_im"])
        df = pd.concat([df[df["excel_sheet"] != "Cochabamba"], new_gasco])

    if fuente in [
        "sierra_norte",
        "mineria",
        "costa_sur",
        "sierra_sur",
        "selva_norte",
        "selva_sur",
    ]:
        df = df[df["nominal"] > 0]

    if fuente in [
        "sierra_norte",
        "mineria",
        "costa_sur",
        "sierra_sur",
        "selva_norte",
        "selva_sur",
    ]:
        for month in doubled[fuente]:
            l = len(month)
            df["excel_sheet"] = df["excel_sheet"].str.replace(
                month, month[: int(l / 2)]
            )
        df["excel_sheet"] = df["excel_sheet"].str.strip()

    if fuente in ["costa_sur"]:
        df = costa_sur_regex(df)

    if fuente in ["selva_norte"]:
        df = selva_norte_regex(df)

    if fuente in ["sierra_sur"]:
        df = sierra_sur_regex(df)

    if fuente in ["mineria"]:
        df["selva_sur_flow"] = df["variables"].apply(lambda x: classify_description(x))

    if fuente in ["costa_centro", "costa_norte"]:
        # All values are in kWH - Estimtaed is in MWH so we will make it kWH
        indices = df[df["excel_sheet"] == "Estimated"].index
        df.loc[indices, "nominal"] = df.loc[indices, "nominal"] * 1000
        # Values from May-2015 on from No-Sierra are in MWh
        if fuente == "costa_centro":
            indices = df[
                (df["date_time"] >= "2015-05-01 01:00")
                & (df["excel_sheet"] == "No-Sierra")
            ].index
            df.loc[indices, "nominal"] = df.loc[indices, "nominal"] * 1000
            # MWH for Minas as well
            indices = df[(df["excel_sheet"] == "Minas")].index
            df.loc[indices, "nominal"] = df.loc[indices, "nominal"] * 1000
            # TRPT from Feb-2022 // Not working before that
            indices = df[
                (df["excel_sheet"] == "TRPT") & (df["date_time"] < "2022-02-01")
            ].index
            df = df.drop(indices, axis=0).reset_index(drop=True)
        else:
            indices = df[(df["excel_sheet"] == "No-Sierra")].index
            df.loc[indices, "nominal"] = df.loc[indices, "nominal"] * 1000

    if fuente in ["mineria"]:
        df = df[
            [
                "date_time",
                "selva_sur_flow",
                "excel_sheet",
                "variables",
                "excel_file",
                "nominal",
            ]
        ]
    else:
        df = df[["date_time", "excel_sheet", "variables", "excel_file", "nominal"]]

    min_date = df["date_time"].min().replace(hour=0)
    max_date = df["date_time"].max()

    missing_hours = list(
        set(pd.date_range(start=min_date, end=max_date, freq="H"))
        - set(df["date_time"].unique().tolist())
    )
    hour_after = [x + datetime.timedelta(hours=1) for x in missing_hours]
    both = missing_hours + hour_after

    missing_df = df[df["date_time"].isin(hour_after)].copy()
    missing_df["date_time"] = missing_df["date_time"].apply(
        lambda x: x - datetime.timedelta(hours=1)
    )

    df = pd.concat([df, missing_df]).sort_values("date_time")
    df = df.sort_values("date_time")

    if fuente in ["selva_norte"]:
        df = selva_norte_dhaid(df)

    if fuente in ["costa_sur"]:
        df["nominal"] = df["nominal"] * 1000

    return df


def costa_sur_regex(df):

    df[0] = df["variables"].str.split(",", expand=True)[0]
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("costa_s", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("- MAIN", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("1000", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace(" T/F", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("..", ".")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("\s+", " ", regex=True)
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("Instation readings from selva_sur (MWh)|", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].apply(lambda x: remove_text_within_parentheses(x))
    df[0] = df[0].str.strip()
    df[0] = df[0].apply(lambda x: remove_duplicate_substrings(x))
    df[0] = df[0].str.strip()
    df[0] = df[0].apply(lambda x: replace_transformer_variants_with_TR(x))
    df[0] = df[0].str.strip()
    df[0] = df[0].apply(lambda x: remove_space_after_TR(x))
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("220 132 kV", "220/132kV")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("132 kV", "132kV")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("meter roll ", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].apply(lambda x: attach_number_to_preceding_substring(x))
    df[0] = df[0].str.strip()
    df["year-month"] = df["date_time"].dt.to_period("M").astype(str)
    df = df.drop(columns="variables").rename(columns={0: "variables"})
    df = (
        df.groupby(
            by=["date_time", "variables", "excel_file", "excel_sheet", "year-month"]
        )[["nominal"]]
        .sum()
        .reset_index()
    )
    return df


def sierra_sur_regex(df):
    df["variables"] = df["variables"].str.replace("\s+", " ", regex=True)
    df["variables"] = df["variables"].str.strip()
    df["variables"] = df["variables"].apply(lambda x: remove_after_kwh(x))
    df["variables"] = df["variables"].str.strip()
    df["year-month"] = df["date_time"].dt.to_period("M").astype(str)
    df = (
        df.groupby(
            by=["date_time", "variables", "excel_file", "excel_sheet", "year-month"]
        )[["nominal"]]
        .sum()
        .reset_index()
    )
    return df


def selva_norte_regex(df):
    df[0] = df["variables"].replace("Export/Import", "", regex=True)
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("Import/Export", "", regex=True)
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("Credit for", "", regex=True)
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("selva_norte", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("at", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace(" _ ", "_")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("line", "Line")
    df[0] = df[0].str.strip()
    df[0] = df[0].apply(lambda x: remove_text_within_parentheses(x))
    df[0] = df[0].str.strip()
    df[0] = df[0].apply(lambda x: remove_duplicate_substrings(x))
    df[0] = df[0].str.strip()
    df[0] = df[0].apply(lambda x: move_keyword_to_end(x))
    df[0] = df[0].str.strip()
    df[0] = df[0].apply(lambda x: remove_space_after_(x))
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("TRU ", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("Maldonado ", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("MALDONADO ", "")
    df[0] = df[0].str.strip()
    df[0] = df[0].str.replace("Cala ", "")
    df[0] = df[0].str.strip()
    df["year-month"] = df["date_time"].dt.to_period("M").astype(str)
    df = df.drop(columns="variables").rename(columns={0: "variables"})
    df = (
        df.groupby(
            by=["date_time", "variables", "excel_file", "excel_sheet", "year-month"]
        )[["nominal"]]
        .sum()
        .reset_index()
    )
    return df


def selva_norte_dhaid(df):
    df = df[~df["variables"].str.lower().str.contains("sajja", regex=True)]
    dhaid = df[df["variables"].str.lower().str.contains("dhaid", regex=True)]
    df = df[~df["variables"].str.lower().str.contains("dhaid", regex=True)]
    dhaid["nominal"] = dhaid.apply(
        lambda x: (
            x["nominal"] * -1
            if x["variables"].lower().__contains__("import")
            else x["nominal"]
        ),
        axis=1,
    )
    dhaid = (
        dhaid.groupby(by=["date_time", "excel_sheet", "excel_file"])["nominal"]
        .sum()
        .reset_index()
    )
    dhaid["variables"] = "Chimbote Line"
    dhaid["nominal"] = dhaid["nominal"].apply(lambda x: x if x >= 0 else 0)
    df = pd.concat([df, dhaid])
    df["nominal"] = df["nominal"] * 1000
    return df


def remove_duplicate_substrings(input_string):
    # Split the string into words based on any non-word character using regex
    words = re.split(r"\W+", input_string)

    seen = set()
    unique_words = []
    for word in words:
        # Check if the word has been seen before, case-insensitive comparison
        if word.lower() not in seen:
            unique_words.append(word)
            seen.add(word.lower())

    # Reconstruct the string from unique words
    # This will not preserve original delimiters like "|", consider if this is acceptable
    result_string = " ".join(unique_words)

    return result_string


def remove_duplicate_strings(input_string):
    # Split the string into words based on any non-word character using regex
    words = re.split(r"\s+", input_string)

    seen = set()
    unique_words = []
    for word in words:
        # Check if the word has been seen before, case-insensitive comparison
        if word.lower() not in seen:
            unique_words.append(word)
            seen.add(word.lower())

    # Reconstruct the string from unique words
    # This will not preserve original delimiters like "|", consider if this is acceptable
    result_string = " ".join(unique_words)

    return result_string


def remove_text_within_parentheses(text):
    # This regex matches content within parentheses, including nested ones
    # It looks for an opening parenthesis, followed by any characters that are not a closing parenthesis (non-greedy), and a closing parenthesis
    return re.sub(r"\([^()]*\)", "", text)


def replace_transformer_variants_with_TR(text):
    # Pattern explanation:
    # - 'trans' matches the literal string "trans", case insensitive with (?i)
    # - '\s*' allows for any number of whitespace characters (including none) between "trans" and "former"
    # - '.{0,2}' allows for up to two of any character between "trans" and "former" to catch variations like "trans*former"
    # Adjust '.{0,2}' as needed to capture the expected range of variations
    pattern = r"(?i)trans.{0,2}\s*former"
    # Replace matches with "T"
    replaced_text = re.sub(pattern, "TR", text)
    return replaced_text


def remove_space_after_TR(text):
    # The pattern looks for 'TR' followed by a space and one or more digits
    # (?i) makes the pattern case-insensitive
    # The parentheses around \d+ make it a capturing group for later reference in the replacement
    pattern = r"(?i)(TR)\s+(\d+)"
    # Replace with 'TR' directly followed by the number, removing the space
    # \1 refers to the first capturing group (TR), and \2 refers to the second group (the number)
    replaced_text = re.sub(pattern, r"\1\2", text)
    return replaced_text


def attach_number_to_preceding_substring(text):
    # Pattern explanation:
    # \D matches any character that's not a digit (indicating the end of the preceding substring)
    # \s+ matches one or more whitespace characters
    # (\d+) captures one or more digits
    # (?<=\D) is a positive lookbehind assertion that ensures the match is preceded by a non-digit character without including it in the match
    # This way, we ensure we're not starting the match at the beginning of a string if it starts with spaces followed by numbers
    pattern = r"(?<=\D)\s+(\d+)"

    # Replace the matched pattern with just the captured digits (\1 refers to the first capturing group)
    replaced_text = re.sub(pattern, r"\1", text)

    return replaced_text


def move_keyword_to_end(text):
    # This pattern matches 'Import' or 'Export', case-insensitive
    pattern = r"(?i)(import|export)"

    # Find all occurrences of the pattern
    keywords = re.findall(pattern, text)

    # Remove the keywords from their original positions
    # The replacement string is an empty string for each match
    text_without_keywords = re.sub(pattern, "", text)

    # Remove extra spaces that may have been left where keywords were removed
    text_cleaned = re.sub(r"\s+", " ", text_without_keywords).strip()

    # Append the keywords to the end of the string
    # If keywords were found, add them back at the end separated by a space
    if keywords:
        # Joining keywords with a space in case there are multiple instances of 'Import'/'Export'
        # and capitalizing the first character
        keywords_str = " ".join(keywords).capitalize()
        return f"{text_cleaned} {keywords_str}"
    else:
        return text_cleaned


def remove_space_after_(text):
    # The pattern looks for '_' followed by a space and one or more digits
    # (?i) makes the pattern case-insensitive
    # The parentheses around \d+ make it a capturing group for later reference in the replacement
    pattern = r"(?i)(_)\s+(\d+)"
    # Replace with 'TR' directly followed by the number, removing the space
    # \1 refers to the first capturing group (TR), and \2 refers to the second group (the number)
    replaced_text = re.sub(pattern, r"\1\2", text)
    return replaced_text


# Define a function to classify each row based on the description
def classify_description(row):
    if pd.notnull(row) and re.search(r"export by selva_sur", row, re.IGNORECASE):
        return "Export"
    elif pd.notnull(row) and re.search(r"import by selva_sur", row, re.IGNORECASE):
        return "Import"
    else:
        return "Other"


def remove_after_kwh(text):
    # Pattern explanation:
    # (?i) - makes the pattern case insensitive
    # \s\(KWh\) - matches " (KWh)" including the preceding space.
    # Note: Parentheses and space are escaped with a backslash because they have special meanings in regex.
    # .*$ - matches everything after " (KWh)" until the end of the string.
    pattern = r"(?i)\s\(KWh\).*$"
    # Replace the matched pattern (everything from " (KWh)" to the end) with an empty string
    replaced_text = re.sub(pattern, "", text)
    return replaced_text
