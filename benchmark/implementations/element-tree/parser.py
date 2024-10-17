from typing import Literal
from zipfile import ZipFile
from sqlalchemy.engine import Engine
from pprint import pprint
from distutils.util import strtobool

import pandas as pd
import xml.etree.ElementTree as et

from benchmark.implementations.skeleton.parser import ParserSkeleton
from benchmark.implementations.skeleton.utilities import (
    default_add_table_to_database,
)


def convert_value(value):
    if value is None:
        return None

    # Try to evaluate the string as a boolean
    try:
        return bool(strtobool(value.lower()))
    except ValueError:
        pass

    # Try to evaluate the string as a numerical value
    try:
        return pd.to_numeric(value)
    except (ValueError, TypeError):
        pass

    return value


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
        data = f.open(file_name, 'r') .read().decode("utf-16")
        root = et.fromstring(data)  # Parse XML

        data = []
        cols = []

        for i, node in enumerate(root):
            row_data = {child.tag: convert_value(child.text) for child in node}

            for tag in row_data.keys():
                # Add new columns only if they weren't added already
                if tag not in cols:
                    cols.append(tag)

            data.append(row_data)

        return pd.DataFrame(data, columns=cols)
