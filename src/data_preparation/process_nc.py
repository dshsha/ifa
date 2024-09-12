import xarray as xr
import numpy as np
import pandas as pd

if __name__ == "__main__":
    ds = xr.open_dataset("../cams.eaq.vra.ENSa.pm10.l1000.2018-04.area-subset.56.23.36.87.55.34.38.59.nc")
    df = ds.to_dataframe()
    df.to_csv('df.csv')