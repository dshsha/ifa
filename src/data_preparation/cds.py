from pathlib import Path

import yaml
import cdsapi


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
        print(token)

    def download_data(self, dataset: str, request: dict[str, list[int] | list[str]]) -> None:
        """
        :param dataset: имя датасета
        :param request:
        :return:
        """
        self.client.retrieve(dataset, request).download()


if __name__ == "__main__":
    with open(Path(Path.cwd().parent, "env", "tok.yaml")) as yaml_file:
        token: str = yaml.safe_load(yaml_file).get("cds_token")
    obj: UseCdsApi = UseCdsApi(token=token)
    dataset: str = "cams-europe-air-quality-reanalyses"
    request: dict[str, list[int] | list[str]] = {
        "variable": ["particulate_matter_10um"],
        "model": ["ensemble"],
        "level": ["1000"],
        "type": ["validated_reanalysis"],
        "year": ["2018"],
        "month": ["04"],
        "area": [
            56.23, 36.87, 55.34,
            38.59,],}
    obj.download_data(dataset=dataset,
                      request=request)
