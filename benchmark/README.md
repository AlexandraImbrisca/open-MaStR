# Benchmark for testing the parsing speed

This benchmark will be used to evaluate different optimizations for
[increasing the parsing speed](https://github.com/OpenEnergyPlatform/open-MaStR/issues/546). 

## Databases

The databases are based on the
[Marktstammdatenregister (MaStR) dataset](https://www.marktstammdatenregister.de/MaStR/Datendownload). Since the data is
public, using this dataset requires no anonymization or other additional measures.

The benchmark consists of 3 datasets with variable sizes:

| Dataset    | Compressed size | Uncompressed size |
|------------|-----------------|-------------------|
| small.zip  | 112.9 MB        | 2.17 GB           |
| medium.zip | 452.2 MB        | 8.52 GB           |
| large.zip  | 854.4 MB        | 16.1 GB           |

The datasets were extracted from the MaStR by imposing certain conditions (e.g., each table can contribute with at most
X files).

## Implementations

The implementations directory contains:
- a skeleton class (```ParserSkeleton```) that defines the main parsing flow via the write_zip_to_database method. The
skeleton declares a few abstract methods that will allow each specific parser to implement its own logic for reading
XML files and adding the tables to the SQL database.
- a set of utilities that can be used widely across all implementations (e.g., casting functions)
- a base implementation extracted from the current open-MaStR repository
- a new parser (i.e., class that extends the ```ParserSkeleton```) for each of the new exploratory implementations
