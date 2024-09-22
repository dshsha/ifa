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
    Класс для выгрузки CAMS.
    """

    def __init__(self, token: str) -> None:
        """
        Инициализируем
        :param token: токен на сайте
        """
        self.client = cdsapi.Client(
            # url="https://cds-beta.climate.copernicus.eu/api",
            # key=token
        )

    def download_data(self,
                      dataset: str,
                      variable: list[str],
                      year: str,
                      month: str,
                      level: str,
                      area: list[float]) -> None:
        """
        :param dataset: имя датасета
        :param variable: набор веществ
        :param year: года
        :param month: месяцы
        :param level: уровни
        :param area: ограничения по координатам
        :return: None
        """
        # шаблон запроса
        request: dict[str, list[int] | list[str]] = {
            "variable": variable,
            "model": ["ensemble"],
            "level": [level],
            "type": ["validated_reanalysis"],
            "year": [year],
            "month": [month],
            "area": area, }

        # подготовленная строка для удобного логгирования и названия .zip-архивов
        log_str: str = f"{'.'.join(variable)}_{year}_{month}_{level}_{'.'.join([str(x) for x in area])}"
        try:
            # скачиваем
            self.client.retrieve(dataset,
                                 request).download(target=Path(Path.cwd().parent,
                                                               "data",
                                                               f"{log_str}.zip"))
            logging.info(f"{log_str} - SUCCESS")
        except requests.exceptions.HTTPError as er_http:
            # отлавливаем ошибку доступа, логгируем и идём дальше
            logging.error(f"{er_http} {log_str}")

    def download_pipeline(self, dataset: str,
                                variable: list[str],
                                areas: list[list[float]],
                                levels: list[str],
                                years: list[str],
                                months: list[str]) -> None:
        """
        :param dataset: имя датасета
        :param variable: вещества
        :param areas: ограничения по координатам
        :param levels: набор уровней
        :param years: года
        :param months: месяцы
        :return: None
        """
        # итерируемся по всему этому
        for area in areas:
            for year in years:
                for month in months:
                    for level in levels:
                        self.download_data(dataset=dataset,
                                           variable=variable,
                                           year=year,
                                           month=month,
                                           level=level,
                                           area=area)


if __name__ == "__main__":
    # считываем токен для доступа из файла tok.yaml
    with open(Path(Path.cwd().parent, "env", "tok.yaml")) as yaml_file:
        token: str = yaml.safe_load(yaml_file).get("cds_token")
    obj: UseCdsApi = UseCdsApi(token=token)

    # имя датасета
    dataset: str = "cams-europe-air-quality-reanalyses"

    # вещества, которые нас интересуют
    variable: list[str] = ['nitrogen_dioxide']
    # 'nitrogen_dioxide', 'particulate_matter_2.5um', 'particulate_matter_10um'

    # ограничения по координатам
    # мгу 55.7, 37.5
    # ифа 55.7, 37.6
    # знс 55.6, 36.8
    areas: list[list[float]] = [[55.5, 36.7, 55.8, 37.8]]
    # набор уорвней
    levels: list[str] = [
        "0",
        "50",
        "100",
        "250",
        "500",
        "750",
        "1000",
        "2000",
        "3000",
        "5000"
    ]
    years: list[str] = [
        # "2018",
        # "2019",
        "2020",
        # "2021",
        # "2022",
        # "2023"
    ]
    months: list[str] = [
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12"
    ]

    # запускаем пайплайн
    obj.download_pipeline(dataset=dataset,
                          variable=variable,
                          areas=areas,
                          levels=levels,
                          years=years,
                          months=months)
