# Description

ETL pipeline for the Furhmann data. The script `ingest.py` is responsible to load the h5 files into a DuckDB database, which is then processed by DBT scripts.

# Installation (BASH/ZSH)

- In the root directory, prepare a virtualenv with `pip -m venv .venv`. This should create a folder `.venv` in it. Then, load the virtualenv to the terminal with `source .venv/bin/activate`.

- Install the Python dependencies with `pip install -r requirements.txt`.

- Make a copy of the environment file `.env.example`: `cp .env.example .env`. In it, change the variables to correspond to the appropriate paths and names.