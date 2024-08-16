from src.utils.spark_utils import load_config, initialize_spark, insert_dataframe_to_sqlite
from data_generation import main as generate_data
import os

# Set environment variables for Python version consistency
os.environ["PYSPARK_PYTHON"] = "/opt/anaconda3/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/anaconda3/bin/python3"

# Load configuration
config = load_config()

# Initialize Spark session
spark = initialize_spark(config["spark"]["app_name"], config["spark"]["jdbc_driver_path"])

# Generate dataframes
dataframes = generate_data()

# Insert data into each table using the Spark utility function
for table_name, sdf in dataframes.items():
    insert_dataframe_to_sqlite(sdf, table_name, config['database']['path'], "org.sqlite.JDBC")

print("All tables populated successfully.")
