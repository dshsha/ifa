import logging
import requests
from pathlib import Path

import yaml
import cdsapi


# логгирование в logs/py_log.py
logging.basicConfig(level=logging.INFO,
                    filename=Path(Path.cwd().parent,
                                  "logs",
                                  "py_log.log"),
                    filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

class UseCdsApi(object):
    """
    Класс для выгрузки CAMS European air quality reanalyses.
    """

    def __init__(self, token: str) -> None:
        """
        Инициализируем
        :param token: токен на сайте
        """
        self.client = cdsapi.Client(
            url="https://ads-beta.atmosphere.copernicus.eu/api", key=token
        )

    def download_data(self,
                      dataset: str,
                      year: str,
                      month: str,
                      level: str) -> None:
        """
        :param dataset: имя датасета
        :param request:
        :return:
        """
        # шаблон запроса
        request: dict[str, list[int] | list[str]] = {
            "variable": ["particulate_matter_2.5um"],
            "model": ["ensemble"],
            "level": [level],
            "type": ["validated_reanalysis"],
            "year": [year],
            "month": [month],
            "area": [56.23, 36.87, 55.34, 38.59, ], }
        try:
            self.client.retrieve(dataset,
                                 request,
                                 Path(Path.cwd().parent, "data", f"{year}_{month}_{level}.zip")).download()
            logging.info(f"year={year}, "
                         f"month={month}, "
                         f"level = {level} - success;")
        except AttributeError:
            pass
        except requests.exceptions.HTTPError:
            # отлавливаем ошибку доступа, логгируем и идём дальше
            logging.error(f"year={year}, "
                          f"month={month}, "
                          f"level = {level};")

    def download_pipeline(self, dataset: str,
                                levels: list[str],
                                years: list[str],
                                months: list[str]) -> None:
        """
        Ппроходимся по этим спискам и выкачиваем дынне.
        :param dataset: датасет
        :param levels: высоты
        :param years: годы
        :param months: месяцы
        """
        for year in years:
            for month in months:
                for level in levels:
                    self.download_data(dataset=dataset,
                                       year=year,
                                       month=month,
                                       level=level)


if __name__ == "__main__":
    # считываем токен для доступа из файла tok.yaml
    with open(Path(Path.cwd().parent, "env", "tok.yaml")) as yaml_file:
        token: str = yaml.safe_load(yaml_file).get("cds_token")
    obj: UseCdsApi = UseCdsApi(token=token)

    # набор уровней, лет, месяцем по которым мы будем итерироваться
    dataset: str = "cams-europe-air-quality-reanalyses"
    levels: list[str] = ["0", "50",
                         "100", "250",
                         "500", "750",
                         "1000", "2000",
                         "3000", "5000"]
    years: list[str] = ["2018", "2019", "2020", "2021", "2022"]
    months: list[str] = ["01", "02", "03", "05", "06", "07", "08", "09", "10", "11", "12"]

    obj.download_pipeline(dataset=dataset,
                          levels=levels,
                          years=years,
                          months=months)
