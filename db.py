import os
import oracledb
from dotenv import load_dotenv

# Setting the oracledb to THICK mode,
# Read here: https://python-oracledb.readthedocs.io/en/latest/user_guide/initialization.html
################################
oracledb.init_oracle_client("instantclient_basic_windows.x64_23.7.0.25.01/")
################################

# loading data from the env file ################################
load_dotenv()
username = os.environ.get("db_username")
userpwd = os.environ.get("db_userpwd")
host = os.environ.get("db_host")
port = os.environ.get("db_port")
service_name = os.environ.get("db_service_name")
# ################################

# Database connection string
DSN = f"{username}/{userpwd}@{host}:{port}/{service_name}"


def connect_to_oracle_db() -> oracledb.Connection:
    try:
        connection = oracledb.connect(DSN)
        return connection
    except oracledb.DatabaseError as e:
        return None
