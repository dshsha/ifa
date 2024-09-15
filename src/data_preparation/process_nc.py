import logging
from zipfile import ZipFile
from pathlib import Path

import xarray as xr
import pandas as pd


# логгирование в logs/py_log.py
logging.basicConfig(level=logging.INFO,
                    filename=Path(Path.cwd().parent,
                                  "logs",
                                  "py_log.log"),
                    filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

class PrepareDS(object):

    def __init__(self):
        ...


if __name__ == "__main__":
    path_to_data = Path(Path.cwd().parent, "data")
    for x in path_to_data.glob("*.zip"):
        with ZipFile(x, "r") as myzip:
            myzip.extractall(path=path_to_data)
    for x in path_to_data.glob("*.nc"):
        df = xr.open_dataset(x).to_dataframe()
        print(x.name.split(".")[5].replace("l", ""))
        name_for_csv: str = f"{str(x.name)[:-3]}.csv"
        df.to_csv(Path(path_to_data, name_for_csv))
        df_2 = pd.read_csv(Path(path_to_data, name_for_csv))
        df_2["level"] = x.name.split(".")[5].replace("l", "")
        df_2.to_csv(Path(path_to_data, name_for_csv))