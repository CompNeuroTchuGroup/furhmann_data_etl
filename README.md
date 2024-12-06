# Description

ETL pipeline for the Furhmann data. The script `ingest.py` is responsible to load the h5 files into a DuckDB database, which is then processed by DBT scripts.

# Installation (BASH/ZSH)

- In the root directory, prepare a virtualenv with `pip -m venv .venv`. This should create a folder `.venv` in it. Then, load the virtualenv to the terminal with `source .venv/bin/activate`.

- Install the Python dependencies with `pip install -r requirements.txt`.

- Make a copy of the environment file `.env.example`: `cp .env.example .env`. In it, change the variables to correspond to the appropriate paths and names.

In order to instantiate the dask cluster, do `docker compose up`. Make sure to change the variable `replicas` in `docker-composer.yml` if you want to run multiple workers in the same machine. To use multiple machines connected via lan, make a copy of the docker compose file and change the ip of the command `dask-worker` to point to the proper scheduler address.

Alternatively, you can also install dask from scratch with the command `pip install dask[distributed]` in the worker machine and instantiate a worker with `dask worker tcp://ip-of-scheduler:port-of-scheduler`.