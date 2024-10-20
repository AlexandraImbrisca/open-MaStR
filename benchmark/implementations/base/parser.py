from sqlalchemy.engine import Engine
from typing import Literal
from zipfile import ZipFile

import pandas as pd

from benchmark.implementations.skeleton.parser import ParserSkeleton
from benchmark.implementations.skeleton.utilities import (
    default_add_table_to_database,
    default_read_xml,
)


class Parser(ParserSkeleton):
    def add_table_to_database(
            self,
            df: pd.DataFrame,
            xml_table_name: str,
            sql_table_name: str,
            if_exists: Literal["fail", "replace", "append"],
            engine: Engine,
    ) -> None:
        default_add_table_to_database(df, xml_table_name, sql_table_name, if_exists, engine)

    def read_xml(self, f: ZipFile, file_name: str) -> pd.DataFrame:
        return default_read_xml(f, file_name)
