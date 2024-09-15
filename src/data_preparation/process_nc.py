from pathlib import Path

import xarray as xr
import numpy as np
import pandas as pd


class ProcessNC(object):
    def __init__(self, file_path):
        self.df = xr.open_dataset(file_path).to_dataframe()


if __name__ == "__main__":
    print(Path(Path.cwd().parent,
                              "data",
                              "cams-europe-air-quality-reanalyses.nc"))
    obj = ProcessNC(Path(Path.cwd().parent,
                              "data",
                              "cams-europe-air-quality-reanalyses.nc"))