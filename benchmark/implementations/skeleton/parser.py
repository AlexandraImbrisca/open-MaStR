import lxml
import pandas as pd
import sqlalchemy

from abc import ABC, abstractmethod
from datetime import datetime
from io import StringIO
import numpy as np
from open_mastr.utils.config import (
    get_output_dir,
    setup_logger
)
from open_mastr.utils.helpers import create_database_engine
from open_mastr.utils.orm import tablename_mapping
from os import path
from shutil import Error
from sqlalchemy import select
from sqlalchemy.sql import text
from zipfile import ZipFile


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

    @staticmethod
    def add_missing_columns_to_table(
            engine: sqlalchemy.engine.Engine,
            xml_tablename: str,
            column_list: list,
    ) -> None:
        """
        Some files introduce new columns for existing tables.
        If the pandas dataframe contains columns that do not
        exist in the database, they are added to the database.
        Parameters
        ----------
        engine
        xml_tablename
        df

        Returns
        -------

        """
        log = setup_logger()

        # get the columns name from the existing database
        inspector = sqlalchemy.inspect(engine)
        table_name = tablename_mapping[xml_tablename]["__class__"].__table__.name
        columns = inspector.get_columns(table_name)
        column_names_from_database = [column["name"] for column in columns]

        missing_columns = set(column_list) - set(column_names_from_database)

        for column_name in missing_columns:
            alter_query = 'ALTER TABLE %s ADD "%s" VARCHAR NULL;' % (
                table_name,
                column_name,
            )
            with engine.connect().execution_options(autocommit=True) as con:
                with con.begin():
                    con.execute(text(alter_query).execution_options(autocommit=True))
            log.info(
                "From the downloaded xml files following new attribute was "
                f"introduced: {table_name}.{column_name}"
            )

    @staticmethod
    def add_table_to_database(
            df: pd.DataFrame,
            xml_tablename: str,
            sql_tablename: str,
            if_exists: str,
            engine: sqlalchemy.engine.Engine,
    ) -> None:
        # get a dictionary for the data types

        table_columns_list = list(
            tablename_mapping[xml_tablename]["__class__"].__table__.columns
        )
        dtypes_for_writing_sql = {
            column.name: column.type
            for column in table_columns_list
            if column.name in df.columns
        }

        ParserSkeleton.add_missing_columns_to_table(engine, xml_tablename, column_list=df.columns.tolist())
        for _ in range(10000):
            try:
                with engine.connect() as con:
                    with con.begin():
                        df.to_sql(
                            sql_tablename,
                            con=con,
                            index=False,
                            if_exists=if_exists,
                            dtype=dtypes_for_writing_sql,
                        )
                        break

            except sqlalchemy.exc.DataError as err:
                ParserSkeleton.delete_wrong_xml_entry(err, df)

            except sqlalchemy.exc.IntegrityError:
                # error resulting from Unique constraint failed
                df = ParserSkeleton.write_single_entries_until_not_unique_comes_up(
                    df=df, xml_tablename=xml_tablename, engine=engine
                )

    @staticmethod
    def add_zero_as_first_character_for_too_short_string(df: pd.DataFrame) -> pd.DataFrame:
        """Some columns are read as integer even though they are actually strings starting with
        a 0. This function converts those columns back to strings and adds a 0 as first character.
        """

        dict_of_columns_and_string_length = {
            "Gemeindeschluessel": 8,
            "Postleitzahl": 5,
        }
        for column_name, string_length in dict_of_columns_and_string_length.items():
            if column_name not in df.columns:
                continue
            try:
                df[column_name] = df[column_name].astype("Int64").astype(str)
            except (ValueError, TypeError):
                # some Plz are in the format DK-9999 for danish Postleitzahl
                # or A-9999 for austrian PLz
                # They cannot be converted to integer
                df[column_name] = df[column_name].astype(str)
                continue
            df[column_name] = df[column_name].where(
                cond=-df[column_name].isin(["None", "<NA>"]), other=None
            )

            string_adding_series = pd.Series(["0"] * len(df))
            string_adding_series = string_adding_series.where(
                cond=df[column_name].str.len() == string_length - 1, other=""
            )
            df[column_name] = string_adding_series + df[column_name]
        return df

    @staticmethod
    def cast_date_columns_to_datetime(xml_tablename: str, df: pd.DataFrame) -> pd.DataFrame:
        sqlalchemy_columnlist = tablename_mapping[xml_tablename][
            "__class__"
        ].__table__.columns.items()
        for column in sqlalchemy_columnlist:
            column_name = column[0]
            if ParserSkeleton.is_date_column(column, df):
                # Convert column to datetime64, invalid string -> NaT
                df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
        return df

    @staticmethod
    def change_column_names_to_orm_format(
            df: pd.DataFrame, xml_tablename: str
    ) -> pd.DataFrame:
        if tablename_mapping[xml_tablename]["replace_column_names"]:
            df.rename(
                columns=tablename_mapping[xml_tablename]["replace_column_names"],
                inplace=True,
            )
        return df

    @staticmethod
    def create_database_table(engine: sqlalchemy.engine.Engine, xml_tablename: str) -> None:
        orm_class = tablename_mapping[xml_tablename]["__class__"]
        # drop the content from table
        orm_class.__table__.drop(engine, checkfirst=True)
        # create table schema
        orm_class.__table__.create(engine)

    @staticmethod
    def correct_ordering_of_filelist(files_list: list) -> list:
        """Files that end with a single digit number get a 0 prefixed to this number
        to correct the list ordering. Afterwards the 0 is deleted again."""
        files_list_ordered = []
        count_if_zeros_are_prefixed = 0
        for file_name in files_list:
            if len(file_name.split(".")[0].split("_")[-1]) == 1:
                file_name = file_name.split("_")[0] + "_0" + file_name.split("_")[1]
                count_if_zeros_are_prefixed += 1
            files_list_ordered.append(file_name)

        files_list_ordered.sort()
        # the list is now in right order, but the 0 has to be deleted
        files_list_correct = []
        for file_name in files_list_ordered:
            if file_name.split(".")[0].split("_")[-1][0] == "0":
                file_name = file_name.split("_")[0] + "_" + file_name.split("_0")[-1]
            files_list_correct.append(file_name)

        if count_if_zeros_are_prefixed >= 5:
            # check if file names from marktstammdatenregister have no prefixed 0 already
            files_list = files_list_correct

        return files_list

    @staticmethod
    def delete_wrong_xml_entry(err: Error, df: pd.DataFrame) -> pd.DataFrame:
        delete_entry = str(err).split("«")[0].split("»")[1]
        print(f"The entry {delete_entry} was deleted due to its false data type.")
        return df.replace(delete_entry, np.nan)

    @staticmethod
    def handle_xml_syntax_error(data: str, err: Error) -> pd.DataFrame:
        """Deletes entries that cause an xml syntax error and produces DataFrame.

        Parameters
        -----------
        data : str
            Decoded xml file as one string
        err : ErrorMessage
            Error message that appeared when trying to use pd.read_xml on invalid xml file.

        Returns
        ----------
        df : pandas.DataFrame
            DataFrame which is read from the changed xml data.
        """

        def find_nearest_brackets(xml_string: str, position: int) -> tuple[int, int]:
            left_bracket_position = xml_string.rfind(">", 0, position)
            right_bracket_position = xml_string.find("<", position)
            return left_bracket_position, right_bracket_position

        data = data.splitlines()

        for _ in range(100):
            # check for maximum of 100 syntax errors, otherwise return an error
            wrong_char_row, wrong_char_column = err.position
            row_with_error = data[wrong_char_row - 1]

            left_bracket, right_bracket = find_nearest_brackets(
                row_with_error, wrong_char_column
            )
            data[wrong_char_row - 1] = (
                    row_with_error[: left_bracket + 1] + row_with_error[right_bracket:]
            )
            try:
                print("One invalid xml expression was deleted.")
                df = pd.read_xml(StringIO("\n".join(data)))
                return df
            except lxml.etree.XMLSyntaxError as e:
                err = e
                continue

        raise Error("An error occured when parsing the xml file. Maybe it is corrupted?")

    @staticmethod
    def is_date_column(column, df: pd.DataFrame) -> bool:
        return (
                type(column[1].type)
                in [
                    sqlalchemy.sql.sqltypes.Date,
                    sqlalchemy.sql.sqltypes.DateTime,
                ]
                and column[0] in df.columns
        )

    @staticmethod
    def is_first_file(file_name: str) -> bool:
        """check if the file name indicates that it is the first file from the table"""
        return (
                file_name.split(".")[0].split("_")[-1] == "1"
                or len(file_name.split(".")[0].split("_")) == 1
        )

    @staticmethod
    def preprocess_table_for_writing_to_database(
            f: ZipFile,
            file_name: str,
            xml_tablename: str,
    ) -> pd.DataFrame:
        data = f.read(file_name)
        try:
            df = pd.read_xml(data, encoding="UTF-16", compression="zip")
        except lxml.etree.XMLSyntaxError as err:
            df = ParserSkeleton.handle_xml_syntax_error(data.decode("utf-16"), err)

        df = ParserSkeleton.add_zero_as_first_character_for_too_short_string(df)
        df = ParserSkeleton.change_column_names_to_orm_format(df, xml_tablename)

        # Add Column that refers to the source of the data
        bulk_download_date = datetime.today().strftime('%Y-%m-%d')
        df["DatenQuelle"] = "bulk"
        df["DatumDownload"] = bulk_download_date
        return df

    @staticmethod
    def write_single_entries_until_not_unique_comes_up(
            df: pd.DataFrame, xml_tablename: str, engine: sqlalchemy.engine.Engine
    ) -> pd.DataFrame:
        """
        Remove from dataframe these rows, which are already existing in the database table
        Parameters
        ----------
        df
        xml_tablename
        engine

        Returns
        -------
        Filtered dataframe
        """

        table = tablename_mapping[xml_tablename]["__class__"].__table__
        primary_key = next(c for c in table.columns if c.primary_key)

        with engine.connect() as con:
            with con.begin():
                key_list = (
                    pd.read_sql(sql=select(primary_key), con=con).values.squeeze().tolist()
                )

        len_df_before = len(df)
        df = df.drop_duplicates(
            subset=[primary_key.name]
        )  # drop all entries with duplicated primary keys in the dataframe
        df = df.set_index(primary_key.name)

        df = df.drop(
            labels=key_list, errors="ignore"
        )  # drop primary keys that already exist in the table
        df = df.reset_index()
        print(f"{len_df_before - len(df)} entries already existed in the database.")

        return df

    @abstractmethod
    def write_zip_to_database(self, zip_file_path: str):
        pass
