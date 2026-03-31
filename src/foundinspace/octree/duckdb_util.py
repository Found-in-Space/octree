import duckdb
from decouple import config

TEMP_DIR = config("DUCKDB_TEMP_DIR", default=None)
MAX_TEMP_DIRECTORY_SIZE = config("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", default=None)
MEMORY_LIMIT = config("DUCKDB_MEMORY_LIMIT", default=None)
THREADS = config("DUCKDB_THREADS", default=None)
PRESERVE_INSERTION_ORDER = config("DUCKDB_PRESERVE_INSERTION_ORDER", default=None)


def configure_connection(con: duckdb.DuckDBPyConnection) -> None:
    if TEMP_DIR is not None:
        con.execute("SET temp_directory = ?", [TEMP_DIR])
        print(f"SET temp_directory = {TEMP_DIR}")
    if MAX_TEMP_DIRECTORY_SIZE is not None:
        con.execute("SET max_temp_directory_size = ?", [MAX_TEMP_DIRECTORY_SIZE])
        print(f"SET max_temp_directory_size = {MAX_TEMP_DIRECTORY_SIZE}")
    if MEMORY_LIMIT is not None:
        con.execute("SET memory_limit = ?", [MEMORY_LIMIT])
        print(f"SET memory_limit = {MEMORY_LIMIT}")
    if THREADS is not None:
        con.execute("SET threads = ?", [THREADS])
        print(f"SET threads = {THREADS}")
    if PRESERVE_INSERTION_ORDER is not None:
        con.execute("SET preserve_insertion_order = ?", [PRESERVE_INSERTION_ORDER])
        print(f"SET preserve_insertion_order = {PRESERVE_INSERTION_ORDER}")
