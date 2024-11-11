import os

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
)
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
NOTEBOOKS_DIR = os.path.join(BASE_DIR, "notebooks")
DEMANDA_DIR = "External\\Directory\\"
DATA_EXTRA_DIR = os.path.join(BASE_DIR, "data_extra")


class DataflowSkipException(Exception):
    """Custom exception to skip a fuente without exiting the script."""

    pass
