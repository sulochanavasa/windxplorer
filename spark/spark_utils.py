import os
import platform
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter

POSTGRES_HOST = os.getenv('POSTGRES_HOST', '10.0.0.6')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'windexplorer')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER','postgres')
POSTGRES_PWD = os.getenv('POSTGRES_PWD','postgres')

def get_postgres_url():
    return "jdbc:postgresql://%s:%s/%s" % (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)

def write_to_postgres(df, table, mode):
    DataFrameWriter(df).jdbc(get_postgres_url(), table, mode, {
        'user': POSTGRES_USER,
        'password': POSTGRES_PWD,
        'driver': 'org.postgresql.Driver'
    })

# Slaves have 6 cores and 14 GB of memory
# Create appropriate number of executors and partitions

def create_spark_session(app='windexplorer'):
    _spark = SparkSession.builder.appName(app) \
     .config("spark.executor.instances", "3") \
     .config("spark.executor.cores",6) \
     .config("spark.executor.memory", "4gb") \
     .getOrCreate()

    # Ensure python dependency files be available on spark driver/workers
    # TODO(): Automate pywtk install with the changes.
    for dirpath, dirnames, filenames in os.walk(os.path.dirname(os.path.realpath(__file__))):
        for file in filenames:
            if file.endswith('.py'):
                _spark.sparkContext.addPyFile(os.path.join(dirpath, file))

    return _spark

