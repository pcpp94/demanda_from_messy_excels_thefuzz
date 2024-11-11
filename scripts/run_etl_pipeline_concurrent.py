import os
import sys

sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))
from src.config import BASE_DIR

main_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))
project_dir = BASE_DIR
import warnings

warnings.filterwarnings("ignore")

import concurrent.futures

# Determine the number of workers based on the CPU-bound heuristic
max_workers = os.cpu_count()

from src.client.demandas_client import Demanda_Client


def fuente_etl(fuente):

    client = Demanda_Client(fuente=fuente)

    print("Bronze ETL")
    client.etl_demanda_data_bronze()

    print("Silver ETL")
    client.etl_demanda_data_silver()

    print("Gold ETL")
    client.etl_demanda_data_gold()


if __name__ == "__main__":

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers * 4) as executor:

        tasks = [
            "costa_centro",
            "costa_norte",
            "sierra_norte",
            "mineria",
            "costa_sur",
            "sierra_sur",
            "selva_norte",
            "selva_sur",
        ]
        futures = [executor.submit(fuente_etl, task) for task in tasks]

        for future in concurrent.futures.as_completed(futures):
            print(future.result())
