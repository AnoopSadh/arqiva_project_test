from pyspark.sql import functions as f, Window
import readSourceData


def transformationUserData(df_user):
    try:
        df_user_2 = df_user.withColumn("current_date", f.current_date())
        df_user_age = df_user_2.withColumn("Age_year", f.round((f.datediff("current_date", "date_of_birth")) / 365, 1)) \
            .drop("current_date")
        df_user_age2 = df_user_age.filter("Age_year < 18")
        df_final = df_user_age2.drop("Age_year")
        # df_final.printSchema()
        # df_final.show()
        return df_final
    except Exception as err:
        print(f"Exception/Error occurred => {err}")


def schemaEvolution(transformedDF, previousDayDF, cur_date, tableName, layer):
    try:
        df1_col = set(transformedDF.columns)
        print(df1_col)
        df2_col = set(previousDayDF.columns)
        df_diff = df2_col.difference(df1_col)
        print("df_diff: ", df_diff)
        if df_diff:
            writeCsv(transformedDF, cur_date, tableName, layer)
        # col_dataType = transformedDF.schema[f"{df_diff}"].dataType
        # print("df datatype: ", col_dataType)
        # sparkSession.spark.sql(f"ALTER TABLE TABLENAME ADD COLUMNS ({df_diff}, {col_dataType})")
        else:
            writeCsv(transformedDF, cur_date, tableName, layer)
        return transformedDF
    except Exception as err:
        print(f"Exception/Error occurred => {err}")


def snapShotDataCreation(df_trans, cur_date, pre_date, tableName, layer):
    # read previous day snapshot data
    try:
        snap_preDF = readSourceData.readPreviousDayData(pre_date, tableName, layer)

        # union delta data with snapshot data
        final_union_df = snap_preDF.union(df_trans)

        # Take our latest for each user
        windowFunc = Window.partitionBy("transaction_id").orderBy("a_timestamp")
        df_hist_final = final_union_df.withColumn("rank", f.rank().over(windowFunc)).filter("rank==1").drop("rank")

        # Load full snapshot to consumption layer
        writeCsv(df_hist_final, cur_date, tableName, layer)
        return df_hist_final
    except Exception as err:
        print(f"Exception Occurred => {err}")


def joinUsersTransactions(transformed_Users_DF, df_fullSnapshot_final):
    expr1 = transformed_Users_DF.user_id == df_fullSnapshot_final.user_id
    drop_col = transformed_Users_DF.user_id
    try:
        final_joined_DF = transformed_Users_DF.join(df_fullSnapshot_final, expr1, "inner").drop(drop_col)
        return final_joined_DF
    except Exception as ex:
        print(f"Error While performing joining => {ex}")


def aggregateData(snapshotDataDF):
    try:
        return snapshotDataDF.groupBy("user_id").agg(f.sum("amount").alias("Total_sum")).sort("user_id")
    except Exception as err:
        print(f"Error in aggregation process => {err}")


def writeCsv(finalDF, cur_date, tableName, layer):
    try:
        (finalDF.write.format("csv").mode("overwrite").option("inferSchema", "true").option("header", "true").save(
            f"{layer}/output_file/{tableName}/{cur_date}"))
    except Exception as err:
        print(f"Exception occurred while writing CSV file => {err}")
