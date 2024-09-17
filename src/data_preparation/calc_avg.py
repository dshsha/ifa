import logging
from pathlib import Path

from typing import Optional

import pandas as pd


# логгирование в logs/py_log.py
logging.basicConfig(level=logging.INFO,
                    filename=Path(Path.cwd().parent,
                                  "logs",
                                  "py_log.log"),
                    filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")


if __name__ == "__main__":
    path_to_data = Path(Path.cwd().parent, "data", "results")
    print(path_to_data)
