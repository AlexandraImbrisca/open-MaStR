import time
from benchmark.scripts.utilities import (
    get_databases,
    get_implementations,
)
from mdutils.mdutils import MdUtils

mdFile = MdUtils(file_name='results', title='Performance results')

databases = get_databases()
implementations = get_implementations()

# mdutils relies on a list of strings to create a table
# Reference: https://github.com/didix21/mdutils?tab=readme-ov-file#create-a-table
list_of_strings = ["Implementation"]
list_of_strings.extend([f"Database {database.name}" for database in databases])

for implementation in implementations:
    list_of_strings.append(implementation.name)

    for database in databases:
        start = time.time()
        implementation.parser.write_zip_to_database(database.zip_file_path)
        end = time.time()

        list_of_strings.append(f"{end - start}")

no_columns = 1 + len(databases)
no_rows = 1 + len(implementations)

mdFile.new_table(columns=no_columns, rows=no_rows, text=list_of_strings, text_align='center')
mdFile.create_md_file()
