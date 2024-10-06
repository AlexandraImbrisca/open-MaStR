from abc import ABC, abstractmethod
from os import path
from typing import Literal
from zipfile import ZipFile

import pandas as pd
from sqlalchemy.engine import Engine

from benchmark.implementations.skeleton.utilities import (
    correct_ordering_of_filelist,
    create_database_table,
    extract_xml_table_name,
    extract_sql_table_name,
    is_first_file,
    preprocess_table_for_writing_to_database,
)
from open_mastr.utils.config import (
    get_output_dir
)
from open_mastr.utils.helpers import create_database_engine
from open_mastr.xml_download.utils_cleansing_bulk import cleanse_bulk_data


class ParserSkeleton(ABC):
    """
    Abstract class that defines a common interface for all the different parsing
    implementations. By implementing the abstract methods, each parser can provide
    diverse algorithms while taking advantage of the common functionality.
    """

    def __init__(self):
        engine_type = "sqlite"
        sqlite_folder_path = path.join(get_output_dir(), "data", engine_type)

        self.engine = create_database_engine(engine_type, sqlite_folder_path)

    def write_zip_to_database(self, zip_file_path: str):
        with ZipFile(zip_file_path, "r") as f:
            files_list = f.namelist()
            files_list = correct_ordering_of_filelist(files_list)

            for file_name in files_list:
                xml_table_name = extract_xml_table_name(file_name)
                sql_table_name = extract_sql_table_name(xml_table_name)

                if is_first_file(file_name):
                    create_database_table(self.engine, xml_table_name)
                    print(
                        f"Table '{sql_table_name}' is filled with data '{xml_table_name}' "
                        "from the bulk download."
                    )

                print(f"File '{file_name}' is parsed.")

                df = self.read_xml(f, file_name)
                df = preprocess_table_for_writing_to_database(df, xml_table_name)
                df = cleanse_bulk_data(df, zip_file_path)

                self.add_table_to_database(df, xml_table_name, sql_table_name, if_exists="append", engine=self.engine)

        print("Bulk download and data cleansing were successful.")

    @abstractmethod
    def add_table_to_database(
            self,
            df: pd.DataFrame,
            xml_table_name: str,
            sql_table_name: str,
            if_exists: Literal["fail", "replace", "append"],
            engine: Engine,
    ) -> None:
        pass

    @abstractmethod
    def read_xml(self, f: ZipFile, file_name: str) -> pd.DataFrame:
        pass
