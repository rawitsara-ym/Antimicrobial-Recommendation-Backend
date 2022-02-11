import pandas as pd
from sqlalchemy.engine import Engine
import sqlalchemy
from IPython.display import display


class UploadValidator:

    def __init__(self, conn: Engine) -> None:
        self.conn = conn
        self.startup()

    def startup(self):
        pass