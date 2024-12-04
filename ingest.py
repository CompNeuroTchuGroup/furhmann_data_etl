"""
Loads h5 files into the main database.
"""

#%%
import pandas as pd
from glob import glob
import os
import h5py
import duckdb
import yaml
from PrettyPrint import PrettyPrintTree
from colorama import Back
from dotenv import load_dotenv
from tqdm import tqdm
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
logger.propagate = False

INGEST_PATH = os.environ.get("INGEST_PATH")
DUCKDB_PATH = os.environ.get("DUCKDB_PATH")
TABLE_NAME = os.environ.get("TABLE_NAME")
DTYPE_PATH = os.environ.get("DTYPE_PATH")
with open(DTYPE_PATH, "r") as f:
    DTYPES = yaml.safe_load(f)
#%%
def tree(file):
    with h5py.File(file, "r") as f:
        def get_children(node):
            if hasattr(node, "keys"):
                return [node[key] for key in node.keys()]
        def get_value(node): return '/' + node.name.split('/')[-1]
        pt = PrettyPrintTree(get_children, get_value, color=Back.BLACK,orientation=PrettyPrintTree.Horizontal)
        pt(f)

def create_table(connection, table_name, dtypes):
    cols = ", ".join([f"\"{column['column']}\" {column['type']}" for column in dtypes])
    logger.debug(f"Creating table {table_name} with columns: {cols}")
    connection.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})")

#%%
def load_hdf(file, entry_point):
    df = pd.read_hdf(file) #Should automatically get the only key in the file
    df['file_name'] = file[len(entry_point)+1:]
    
    #Join neural data into an array data type (dimensionality may vary)
    #They are the numerical columns, so filter by that.
    numerical_columns = list(filter(lambda x: isinstance(x, int), df.columns))
    
    df["neural_data"] = df[numerical_columns].values.copy().tolist()
    df = df.drop(columns = numerical_columns)
    
    return df
#%%
def files(entry_point):
    return glob(os.path.join(entry_point, "**/*.h5"), recursive = True)
def load_data(entry_point):
    file_list = files(entry_point)    
    for file in file_list:
        try:
            yield load_hdf(file, entry_point)
        except AttributeError:
            logger.error(f"Error reading {file}")
            tree(file)
#%%
def get_schema(con, table_name):
    return con.execute(f"SELECT * FROM {table_name} LIMIT 1").fetch_df().dtypes

# %%
def insert_data(
    dataframe,
    duckdb_path,
    table_name
    ):
    with duckdb.connect(duckdb_path) as connection:
        table_not_exists = connection.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone() is None
        schema = get_schema(connection, table_name) if not table_not_exists else dataframe.dtypes
        if table_not_exists:
            logger.info(f"Creating table {table_name}")
            create_table(connection, table_name, DTYPES)
        #Check if schemas are the same. Otherwise, create the missing columns.
        df_schema = dataframe.dtypes
        schema_diff = set(df_schema.keys()) - set(schema.keys())
        if len(schema_diff) > 0:
            logger.warning(f"Schema for table {table_name} does not match. Creating missing columns.")
            for col in schema_diff:
                connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {df_schema[col]}")
        logger.info(f"Inserting {len(dataframe)} records")
        connection.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM dataframe")

# %%
if __name__ == "__main__":
    n = len(files(INGEST_PATH))
    for dataframe in tqdm(load_data(INGEST_PATH), total = n):
        insert_data(dataframe, DUCKDB_PATH, TABLE_NAME)
# %%
