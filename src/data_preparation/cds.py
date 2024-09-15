import logging
import requests
from pathlib import Path

import yaml
import cdsapi


logging.basicConfig(level=logging.INFO,
                    filename=Path(Path.cwd().parent,
                                  "logs",
                                  "py_log.log"),
                    filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

class UseCdsApi(object):
    """
    Класс для выгрузки CAMS European air quality reanalyses
    """

    def __init__(self, token: str) -> None:
        """
        Инициализируем
        :param token: токен на сайте
        """
        self.client = cdsapi.Client(
            url="https://ads-beta.atmosphere.copernicus.eu/api", key=token
        )

    def download_data(self, dataset: str, request: dict[str, list[int] | list[str]]) -> None:
        """
        :param dataset: имя датасета
        :param request:
        :return:
        """
        try:
            self.client.retrieve(dataset,
                                 request).download()
            logging.info(f"level = {request.get('level')} -- success")
        except AttributeError:
            pass
        except requests.exceptions.HTTPError:
            logging.error(f"year = {request.get('year')}, level = {request.get('level')}")


if __name__ == "__main__":
    levels: list[str] = ["0", "50",
                         "100", "250",
                         "500", "750",
                         "1000", "2000",
                         "3000", "5000"]
    years: list[str] = ["2018", "2019", "2020", "2021", "2022"]
    months: list[str] = ["01", "02", "03", "05", "06", "07", "08", "09", "10", "11", "12"]
    with open(Path(Path.cwd().parent, "env", "tok.yaml")) as yaml_file:
        token: str = yaml.safe_load(yaml_file).get("cds_token")
    obj: UseCdsApi = UseCdsApi(token=token)
    dataset: str = "cams-europe-air-quality-reanalyses"
    for year in years:
        for month in months:
            for level in levels:
                    request: dict[str, list[int] | list[str]] = {
                        "variable": ["particulate_matter_2.5um"],
                        "model": ["ensemble"],
                        "level": [level],
                        "type": ["validated_reanalysis"],
                        "year": [year],
                        "month": [month],
                        "area": [56.23, 36.87, 55.34, 38.59,],}
                    obj.download_data(dataset=dataset,
                                      request=request)
