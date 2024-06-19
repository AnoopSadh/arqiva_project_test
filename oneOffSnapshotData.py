from datetime import date
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from dateutil.relativedelta import relativedelta
import sparkSession

df_hist = (sparkSession.spark.read \
           .format("csv") \
           .option("header", "true") \
           .option("inferSchema", "true") \
           .load(f"ingestion/output_file/transactions/"))
df_hist.printSchema()
df_hist.show()
cur_date = date.today()
his_date = cur_date + relativedelta(days=-3)
windowFunc = Window.partitionBy("transaction_id").orderBy("a_timestamp")
df_hist_final = df_hist.withColumn("rank", f.rank().over(windowFunc)).filter("rank==1")

df_hist_final.printSchema()
df_hist_final.show()
# df_hist.write.format("csv").option("inferSchema", "true").option("header", "true").save(
#             f"ingestion/output_file/{his_date}")