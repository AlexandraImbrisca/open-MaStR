from datetime import date
from io import StringIO
from shutil import Error

import numpy as np
import pandas as pd
import sqlalchemy
from lxml.etree import XMLSyntaxError
from sqlalchemy.sql.sqltypes import Date, DateTime

from open_mastr.utils.config import setup_logger
from open_mastr.utils.orm import tablename_mapping


def add_missing_columns_to_table(
        engine: sqlalchemy.engine.Engine,
        xml_table_name: str,
        column_list: list,
) -> None:
    """
    Some files introduce new columns for existing tables.
    If the pandas dataframe contains columns that do not
    exist in the database, they are added to the database.
    Parameters
    ----------
    engine
    xml_table_name
    column_list

    Returns
    -------

    """
    log = setup_logger()

    # get the columns name from the existing database
    inspector = sqlalchemy.inspect(engine)
    table_name = tablename_mapping[xml_table_name]["__class__"].__table__.name
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
                con.execute(sqlalchemy.sql.text(alter_query).execution_options(autocommit=True))
        log.info(
            "From the downloaded xml files following new attribute was "
            f"introduced: {table_name}.{column_name}"
        )


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


def cast_date_columns_to_datetime(xml_table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    column_list = tablename_mapping[xml_table_name][
        "__class__"
    ].__table__.columns.items()
    for column in column_list:
        column_name = column[0]
        if is_date_column(column, df):
            # Convert column to datetime64, invalid string -> NaT
            df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
    return df


def change_column_names_to_orm_format(
        df: pd.DataFrame, xml_table_name: str
) -> pd.DataFrame:
    if tablename_mapping[xml_table_name]["replace_column_names"]:
        df.rename(
            columns=tablename_mapping[xml_table_name]["replace_column_names"],
            inplace=True,
        )
    return df


def correct_ordering_of_filelist(files_list: list) -> list:
    """Files that end with a single digit number get a 0 prefixed to this number
    to correct the list ordering. Afterward the 0 is deleted again."""
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


def create_database_table(engine: sqlalchemy.engine.Engine, xml_table_name: str) -> None:
    orm_class = tablename_mapping[xml_table_name]["__class__"]
    # drop the content from table
    orm_class.__table__.drop(engine, checkfirst=True)
    # create table schema
    orm_class.__table__.create(engine)


def delete_wrong_xml_entry(err: Error, df: pd.DataFrame) -> pd.DataFrame:
    delete_entry = str(err).split("«")[0].split("»")[1]
    print(f"The entry {delete_entry} was deleted due to its false data type.")
    return df.replace(delete_entry, np.nan)


def extract_xml_table_name(file_name: str) -> str:
    """The XML table name is the beginning of the filename without the number
    in lowercase."""
    return file_name.split("_")[0].split(".")[0].lower()


def extract_sql_table_name(xml_table_name: str) -> str:
    """Extract the SQL table name from the table name mapping."""
    return tablename_mapping[xml_table_name]["__name__"]


def handle_xml_syntax_error(data: str, err: Error) -> pd.DataFrame:
    """Deletes entries that cause a xml syntax error and produces DataFrame.

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
        except XMLSyntaxError as e:
            err = e
            continue

    raise Error("An error occurred when parsing the xml file. Maybe it is corrupted?")


def is_date_column(column, df: pd.DataFrame) -> bool:
    return type(column[1].type) in [Date, DateTime] and column[
        0] in df.columns


def is_first_file(file_name: str) -> bool:
    """Check if the file name indicates that it is the first file from the table"""
    return (
            file_name.split(".")[0].split("_")[-1] == "1"
            or len(file_name.split(".")[0].split("_")) == 1
    )


def is_table_relevant(xml_table_name: str, include_tables: list) -> bool:
    """Checks if the table contains relevant data and if the user wants to
    have it in the database."""
    # few tables are only needed for data cleansing of the xml files and contain no
    # information of relevance
    boolean_write_table_to_sql_database = (
        tablename_mapping[xml_table_name]["__class__"] is not None
    )
    # check if the table should be written to sql database (depends on user input)
    include_count = include_tables.count(xml_table_name)

    return include_count == 1 and boolean_write_table_to_sql_database


def preprocess_table_for_writing_to_database(
        df: pd.DataFrame,
        xml_table_name: str,
) -> pd.DataFrame:
    df = add_zero_as_first_character_for_too_short_string(df)
    df = change_column_names_to_orm_format(df, xml_table_name)

    # Add Column that refers to the source of the data
    bulk_download_date = date.today().strftime("%Y%m%d")
    df["DatenQuelle"] = "bulk"
    df["DatumDownload"] = bulk_download_date
    return df


def write_single_entries_until_not_unique_comes_up(
        df: pd.DataFrame, xml_table_name: str, engine: sqlalchemy.engine.Engine
) -> pd.DataFrame:
    """
    Remove from dataframe these rows, which are already existing in the database table
    Parameters
    ----------
    df
    xml_table_name
    engine

    Returns
    -------
    Filtered dataframe
    """

    table = tablename_mapping[xml_table_name]["__class__"].__table__
    primary_key = next(c for c in table.columns if c.primary_key)

    with engine.connect() as con:
        with con.begin():
            key_list = (
                pd.read_sql(sql=sqlalchemy.select(primary_key), con=con).values.squeeze().tolist()
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
