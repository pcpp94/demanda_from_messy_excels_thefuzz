import sys
import os

sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))

fuentes = [
    "costa_centro",
    "costa_norte",
    "sierra_norte",
    "mineria",
    "costa_sur",
    "sierra_sur",
    "selva_norte",
    "selva_sur",
]

from src.client.demandas_client import Demanda_Client
from src.config import DataflowSkipException
import warnings

warnings.filterwarnings("ignore")


def fuente_etl(fuente, only_update: bool):
    client = Demanda_Client(fuente=fuente)
    print("Bronze ETL")
    client.etl_demanda_data_bronze(only_update=only_update)
    print("Silver ETL")
    client.etl_demanda_data_silver()
    print("Gold ETL")
    client.etl_demanda_data_gold()


def run_all():
    for fuente in fuentes:
        try:
            fuente_etl(fuente=fuente, only_update=True)
        except DataflowSkipException as e:
            print(f"Skipped {fuente}: {e}")
            continue  # Skip to the next fuente


if __name__ == "__main__":
    run_all()
