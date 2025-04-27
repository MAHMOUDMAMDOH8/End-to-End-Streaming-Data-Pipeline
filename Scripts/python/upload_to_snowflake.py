from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool
import os
import logging
from hdfs import InsecureClient
from dotenv import load_dotenv
import pandas as pd
import pyarrow.parquet as pq


def snowFlaek_connection(user, password, account, warehouse, database, schema, role):
    Conn_str = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
    
    try:
        engine = create_engine(Conn_str, poolclass=NullPool)
        connection = engine.connect()
        print("Connected to Snowflake successfully")
        return connection, engine
    
    except SQLAlchemyError as e:
        print(f"Error while connecting to Snowflake: {e}")
        return None, None

def insert_raw_data(table_name,data_frame,connection,engine):
    logging.info(f"loading raw data into table {table_name}")

    if connection:
        try:
            data_frame = pd.concat(data_frame, ignore_index=True)
            data_frame.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            logging.info(f"raw data loaded successfully into {table_name} with {len(data_frame)} rows")
        except SQLAlchemyError as e:
            logging.info(f"error while load raw data : {e} ")
    else:
        logging.info(f"Failed to connect to the database and load data into table {table_name}")

def read_parquet_from_hdfs(hdfs_uri, file_path):

    client = InsecureClient(hdfs_uri, user='hadoop')
    local_file = "/opt/airflow/includes/local_warehose/silver"
    logging.info(f"Downloading Parquet file from HDFS: {file_path}")
    client.download(file_path, local_file)

    logging.info(f"Reading Parquet file into DataFrame: {local_file}")
    parquet_path = os.path.join(local_file, "stock-prices")
    dfs = []
    parquet_files = [f for f in os.listdir(parquet_path) if f.endswith('.parquet')]
    for parquet_file in parquet_files:
        logging.info(f"Found Parquet file: {parquet_file}")
        table = pq.read_table(local_file,)
        df = table.to_pandas()
        dfs.append(df)
    return dfs


if __name__ == "__main__":

    load_dotenv("environment.env")
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('snowflake_uploader')

    # Snowflake connection parameters
    SNOWFLAKE_USER="snowtest11"
    SNOWFLAKE_PASSWORD="Sn1234567891011"
    SNOWFLAKE_ROLE="ACCOUNTADMIN"
    SNOWFLAKE_ACCOUNT="JN79948.eu-central-2.aws"
    SNOWFLAKE_WAREHOUSE ="COMPUTE_WH"
    SNOWFLAKE_DATABASE="STOCK"
    SNOWFLAKE_SCHEMA="SILVER"


    

    # HDFS connection parameters
    HDFS_URI = "http://namenode:9870"
    files_dir = "/silver/stock-prices"

    client = InsecureClient(HDFS_URI, user='hadoop')
    connection, engine = snowFlaek_connection(
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_WAREHOUSE,
            SNOWFLAKE_DATABASE,
            SNOWFLAKE_SCHEMA,
            SNOWFLAKE_ROLE
    )

    data_frame = read_parquet_from_hdfs(HDFS_URI, files_dir)

    # Insert data into Snowflake
    insert_raw_data('stock-prices', data_frame, connection, engine)