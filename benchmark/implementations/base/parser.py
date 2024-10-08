from typing import Literal
from zipfile import ZipFile

import pandas as pd
from lxml.etree import XMLSyntaxError
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DataError, IntegrityError

from benchmark.implementations.skeleton.parser import ParserSkeleton
from benchmark.implementations.skeleton.utilities import (
    add_missing_columns_to_table,
    delete_wrong_xml_entry,
    handle_xml_syntax_error,
    write_single_entries_until_not_unique_comes_up,
)
from open_mastr.utils.orm import tablename_mapping


class Parser(ParserSkeleton):
    def add_table_to_database(
            self,
            df: pd.DataFrame,
            xml_table_name: str,
            sql_table_name: str,
            if_exists: Literal["fail", "replace", "append"],
            engine: Engine,
    ) -> None:
        # get a dictionary for the data types
        table_columns_list = list(
            tablename_mapping[xml_table_name]["__class__"].__table__.columns
        )
        dtypes_for_writing_sql = {
            column.name: column.type
            for column in table_columns_list
            if column.name in df.columns
        }

        add_missing_columns_to_table(engine, xml_table_name, column_list=df.columns.tolist())

        for _ in range(10000):
            try:
                with engine.connect() as con:
                    with con.begin():
                        df.to_sql(
                            sql_table_name,
                            con=con,
                            index=False,
                            if_exists=if_exists,
                            dtype=dtypes_for_writing_sql,
                        )
                        break

            except DataError as err:
                delete_wrong_xml_entry(err, df)

            except IntegrityError:
                # error resulting from Unique constraint failed
                df = write_single_entries_until_not_unique_comes_up(
                    df, xml_table_name, engine
                )

    def read_xml(self, f: ZipFile, file_name: str) -> pd.DataFrame:
        data = f.read(file_name)
        try:
            return pd.read_xml(data, encoding="UTF-16", compression="zip")
        except XMLSyntaxError as error:
            return handle_xml_syntax_error(data.decode("utf-16"), error)
