from benchmark.implementations.skeleton.parser import ParserSkeleton
from open_mastr.utils.orm import tablename_mapping
from open_mastr.xml_download.utils_cleansing_bulk import cleanse_bulk_data
from zipfile import ZipFile


class BasePaper(ParserSkeleton):
    def write_zip_to_database(self, zip_file_path: str):
        with ZipFile(zip_file_path, "r") as f:
            files_list = f.namelist()
            files_list = ParserSkeleton.correct_ordering_of_filelist(files_list)

            for file_name in files_list:
                # xml_tablename is the beginning of the filename without the number in lowercase
                xml_tablename = file_name.split("_")[0].split(".")[0].lower()

                sql_tablename = tablename_mapping[xml_tablename]["__name__"]

                if ParserSkeleton.is_first_file(file_name):
                    ParserSkeleton.create_database_table(engine=self.engine, xml_tablename=xml_tablename)
                    print(
                        f"Table '{sql_tablename}' is filled with data '{xml_tablename}' "
                        "from the bulk download."
                    )
                print(f"File '{file_name}' is parsed.")

                df = ParserSkeleton.preprocess_table_for_writing_to_database(
                    f=f,
                    file_name=file_name,
                    xml_tablename=xml_tablename,
                )

                # Convert date and datetime columns into the datatype datetime
                df = ParserSkeleton.cast_date_columns_to_datetime(xml_tablename, df)

                df = cleanse_bulk_data(df, zip_file_path)

                ParserSkeleton.add_table_to_database(
                    df=df,
                    xml_tablename=xml_tablename,
                    sql_tablename=sql_tablename,
                    if_exists="append",
                    engine=self.engine,
                )

    print("Bulk download and data cleansing were successful.")

