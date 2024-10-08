import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from benchmark.implementations.base.parser import ParserSkeleton
from pydoc import locate


@dataclass
class Database:
    name: str
    zip_file_path: str


@dataclass
class Implementation:
    name: str
    parser: ParserSkeleton


BENCHMARK_PATH = Path().absolute().parent


def get_databases() -> List[Database]:
    return [Database(file_name, f"{BENCHMARK_PATH}/databases/{file_name}") for file_name in
            os.listdir(f"{BENCHMARK_PATH}/databases") if file_name.endswith('.zip')]


def get_implementations() -> List[Implementation]:
    """Returns the implementations names based on the file structure."""
    path = f"{BENCHMARK_PATH}/implementations"

    implementations_names = next(os.walk(path))[1]
    implementations_names.remove('skeleton')

    implementations = []
    module_name = path.split("open-MaStR/", 1)[1].replace('/', '.')

    for name in implementations_names:
        parser_class_module = locate(f"{module_name}.{name}.parser")
        parser_class = getattr(parser_class_module, "Parser")

        implementations.append(Implementation(name, parser_class()))

    return implementations
