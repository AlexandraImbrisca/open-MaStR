from typing import Literal
from zipfile import ZipFile
from sqlalchemy.engine import Engine
from lxml.etree import XMLSyntaxError

import pandas as pd

from benchmark.implementations.skeleton.parser import ParserSkeleton
from benchmark.implementations.skeleton.utilities import (
    default_add_table_to_database,
    handle_xml_syntax_error,
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
        data = f.read(file_name)
        try:
            return pd.read_xml(data, encoding="UTF-16", compression="zip", parser='etree')
        except XMLSyntaxError as error:
            return handle_xml_syntax_error(data.decode("utf-16"), error)
