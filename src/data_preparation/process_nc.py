import logging
from zipfile import ZipFile
from pathlib import Path

from typing import Optional

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
    path_to_data = Path(Path.cwd().parent, "data")

    def __init__(self):
        self.result_df: Optional[pd.DataFrame] = None

    @staticmethod
    def unzip() -> None:
        """Разархивировать .zip."""
        for x in PrepareDS.path_to_data.glob("*.zip"):
            with ZipFile(x, "r") as myzip:
                myzip.extractall(path=PrepareDS.path_to_data)
                logging.info(f"Unziped {x.name}")

    def assemble_resulting_dataframe(self):
        for x in PrepareDS.path_to_data.glob("*.nc"):
            df = xr.open_dataset(x).to_dataframe()
            name_for_csv: str = f"{str(x.name)[:-3]}.csv"
            df.to_csv(Path(PrepareDS.path_to_data, name_for_csv))
            df_2 = pd.read_csv(Path(PrepareDS.path_to_data, name_for_csv))
            df_2["level"] = x.name.split(".")[5].replace("l", "")
            # мгу 55.7, 37.5
            # ифа 55.7, 37.6
            df_2 = df_2[(df_2['lat'] == 55.7) &
                        ((df_2['lon'] == 37.5) | (df_2['lon'] == 37.6))]
            if self.result_df is None:
                self.result_df = df_2
                logging.info(f"init result_df; name = {name_for_csv}, shape = {self.result_df.shape}")
            else:
                self.result_df = pd.concat([self.result_df, df_2])
                logging.info(f"add to result_df; name = {name_for_csv}, shape = {self.result_df.shape}")

    def write_result_df(self):
        # time,lat,lon,pm2p5, level
        self.result_df = self.result_df.sort_values(["time", "level"],
                                                    ascending=True)
        self.result_df.to_csv(Path(PrepareDS.path_to_data, "result_df.csv"))
        logging.info(f"write result_df; shape = {self.result_df.shape}")

if __name__ == "__main__":
    PrepareDS.unzip()
    obj = PrepareDS()
    obj.assemble_resulting_dataframe()
    obj.write_result_df()
