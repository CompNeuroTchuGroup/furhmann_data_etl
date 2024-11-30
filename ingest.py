"""
Loads h5 files into the main database.
"""

#%%
import pandas as pd
from glob import glob
import os
import h5py
import duckdb
from PrettyPrint import PrettyPrintTree
from colorama import Back
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

INGEST_PATH = os.environ.get("INGEST_PATH")
DUCKDB_PATH = os.environ.get("DUCKDB_PATH")
TABLE_NAME = os.environ.get("TABLE_NAME")

#%%
def tree(file):
    with h5py.File(file, "r") as f:
        def get_children(node):
            if hasattr(node, "keys"):
                return [node[key] for key in node.keys()]
        def get_value(node): return '/' + node.name.split('/')[-1]
        pt = PrettyPrintTree(get_children, get_value, color=Back.BLACK,orientation=PrettyPrintTree.Horizontal)
        pt(f)
#%%
def load_data(entry_point):
    """
    Loads all h5 files in the given directory (and its subdirectories).
    
    Parameters
    ----------
    entry_point : str
        The directory to search for h5 files.
    
    Returns
    -------
    df_all : pandas.DataFrame
        A DataFrame containing data from all h5 files.
    """
    file_list = glob(os.path.join(entry_point, "**/*.h5"), recursive = True)
    
    df_list = []
    for file in file_list:
        try:
            df = pd.read_hdf(file) #Should automatically get the only key in the file
            df['HASH'] = pd.util.hash_pandas_object(df) #This is to avoid duplicate insertions
            df['file_name'] = file[len(entry_point)+1:]
            df_list.append(df)
        except AttributeError:
            logger.error(f"Error reading {file}")
            tree(file)
    
    df_all = pd.concat(df_list, ignore_index=True)
    return df_all

# %%
def insert_data(
    dataframe,
    duckdb_path,
    table_name,
    hash_column = 'HASH'
    ):
    with duckdb.connect(duckdb_path) as connection:
        table_not_exists = connection.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone() is None
        if table_not_exists:
            logger.info(f"Creating table {table_name} and inserting all {len(dataframe)} records")
            connection.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM dataframe")
        else:
            hashes = connection.execute(f"SELECT {hash_column} FROM {table_name}").fetchall()
            hashes = [hash[0] for hash in hashes]
            dataframe = dataframe.loc[~dataframe[hash_column].isin(hashes)]
            logger.info("Inserting {len(dataframe)} records")
            connection.execute(f"INSERT INTO {table_name} SELECT * FROM dataframe")

if __name__ == "__main__":
    insert_data(load_data(INGEST_PATH), DUCKDB_PATH, TABLE_NAME)