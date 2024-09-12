from pathlib import Path

import numpy as np
import pandas as pd


class AOD440(object):
    """
    Класс считывания и разбивки AOD440 на бины.
    """

    def __init__(self) -> None:
        # считывам из xlsx в df и сортируем
        self.df: pd.DataFrame = pd.read_excel(
            Path(Path.cwd().parent, "data", "AOD_440nm.xlsx")
        ).sort_values(by=["AOD_440nm"])

    def binning(self, n_bins: int = 20) -> pd.Series:
        """
        :param n_bins: количество бинов
        :return: df с количеством вхождений в бины
        """
        min_aod: float = self.df["AOD_440nm"].min()
        max_aod: float = self.df["AOD_440nm"].max()
        bins: np.ndarray = np.linspace(min_aod, max_aod, n_bins)
        ser: pd.Series = (
            pd.cut(self.df["AOD_440nm"], bins=bins).value_counts().sort_index()
        )
        return ser


if __name__ == "__main__":
    obj: AOD440 = AOD440()
    print(obj.binning(n_bins=20))
