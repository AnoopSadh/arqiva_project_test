import sparkSession
from sparkSession import spark


# schema_user = StructType([
#     StructField("user_id", IntegerType()),
#     StructField("name", StringType()),
#     StructField("email", StringType()),
#     StructField("date_of_birth", DateType())
# ])
def readUserData():
    try:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("ingestion/input_file/users.csv").distinct()
    except Exception as err:
        print(f"Exception while reading source data => {err}")


def readTransactionData():
    try:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("ingestion/input_file/transactions.csv")
    except Exception as err:
        print(f"Exception while reading transaction data => {err}")


def readPreviousDayData(pre_date, tableName, layer):
    try:
        return (sparkSession.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            f"{layer}/output_file/{tableName}/{pre_date}"))
    except Exception as err:
        print(f"Exception while reading previous day data => {err}")


def readSnapshotData(cur_date, tableName, layer):
    try:
        return (sparkSession.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
            f"{layer}/output_file/{tableName}/{cur_date}"))
    except Exception as err:
        print(f"Exception while reading snapshot data => {err}")
