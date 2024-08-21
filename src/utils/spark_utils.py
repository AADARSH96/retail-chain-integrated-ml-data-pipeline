from pyspark.sql import SparkSession
import json
import logging
import os
from datetime import datetime
from src.data.database_operations import get_next_version_number

# Set environment variables for Python version consistency
os.environ["PYSPARK_PYTHON"] = "/opt/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/anaconda3/bin/python3"


def load_config(config_path='../../configs/data_config.json'):
    """
    Load configuration from a JSON file.

    :param config_path: Path to the JSON configuration file.
    :return: Configuration dictionary.
    """
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config


def initialize_spark(app_name, jdbc_driver_path):
    """
    Initialize a Spark session with the given application name and JDBC driver.

    :param app_name: The name of the Spark application.
    :param jdbc_driver_path: Path to the JDBC driver jar.
    :return: SparkSession object.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    return spark


def insert_dataframe_to_sqlite(df, table_name, db_path, jdbc_driver):
    """
    Insert a Spark DataFrame into an SQLite database using JDBC.

    :param df: The Spark DataFrame to insert.
    :param table_name: The name of the table to insert data into.
    :param db_path: Path to the SQLite database.
    :param jdbc_driver: The JDBC driver class name.
    """
    print(f"Inserting data into {table_name} table...")
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:sqlite:{db_path}") \
        .option("dbtable", table_name) \
        .option("driver", jdbc_driver) \
        .mode("append") \
        .save()
    print(f"Finished inserting data into {table_name} table.")


def read_dataframe_from_sqlite(spark, table_name, db_path, jdbc_driver):
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{db_path}") \
        .option("driver", jdbc_driver) \
        .option("dbtable", table_name) \
        .load()
    return df


def save_spark_model(model, model_name, db_path):
    version = get_next_version_number(model_name, db_path)
    date_path = datetime.now().strftime("%Y%m%d")
    base_path = f"./results/models/{date_path}"
    os.makedirs(base_path, exist_ok=True)
    file_path = f"{base_path}/{model_name}_v{version}"
    model.save(file_path)
    return file_path, version
