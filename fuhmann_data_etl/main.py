#%%
from dask.distributed import Client
import os
from dotenv import load_dotenv
import logging

load_dotenv("../.env")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.propagate = False

INGEST_PATH = os.environ["INGEST_PATH"]
TARGET_PATH = os.environ["TARGET_PATH"]
DASK_SCHEDULER_ADDRESS = os.environ["DASK_SCHEDULER_ADDRESS"]

client = Client(DASK_SCHEDULER_ADDRESS)
