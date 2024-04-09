from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

HDFS_FOLDER = "currency-pairs"

CURRENCY_RATES_FILE = "currency_rates.json"

CURRENCY_RATES_TABLE = "currency_rates"

warehouse_location = abspath('spark-warehouse')
# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Saving currency rates to Hive table") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file currency_rates.json from the HDFS
df = spark.read.json(f"hdfs://namenode:9000/{HDFS_FOLDER}/{CURRENCY_RATES_FILE}")

# Drop the duplicated rows based on the base and last_update columns
forex_rates = df.select('base', 'last_update', 'rates.eur', 'rates.usd', 'rates.rub', 'rates.gel', 'rates.jpy',
                        'rates.cad') \
    .dropDuplicates(['base', 'last_update']) \
    .fillna(1, subset=['EUR', 'USD', 'RUB', 'GEL', 'JPY', 'CAD'])

# Export the dataframe into the Hive table forex_rates
forex_rates.write.mode("append").insertInto(CURRENCY_RATES_TABLE)
