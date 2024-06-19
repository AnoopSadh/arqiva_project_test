from pyspark.sql import functions as f
from datetime import date

import sparkSession
from configReader import readPropertyFile
from dataTransformation import schemaEvolution, writeCsv, snapShotDataCreation, joinUsersTransactions, \
    transformationUserData, aggregateData
from readSourceData import readUserData, readTransactionData, readPreviousDayData, readSnapshotData
from dateutil.relativedelta import relativedelta

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    config_file = readPropertyFile()
    print("history_flag -: ", config_file)

    cur_date = date.today()
    pre_date = cur_date + relativedelta(days=-1)
    print("cur_date", cur_date, pre_date)

    # Read users Data from source
    df_user = readUserData()
    tableName = "users"
    layer = "ingestion"
    writeCsv(df_user, cur_date, tableName, layer)

    # Read transactions data from source
    tableName = "transactions"
    df_trans = readTransactionData()
    writeCsv(df_user, cur_date, tableName, layer)

    # Schema evolution handling
    tableName = "transactions"
    layer = "ingestion"
    previousDayDF = readPreviousDayData(pre_date, tableName, layer)
    df_trans = schemaEvolution(df_trans, previousDayDF, cur_date, tableName, layer)

    # Transformation with users data
    transformed_Users_DF = transformationUserData(df_user)
    tableName = "users"
    layer = "consumption"
    writeCsv(transformed_Users_DF, cur_date, tableName, layer)

    # Transformation with transaction data
    tableName = "snapshotData/transactions"
    layer = "consumption"
    writeCsv(df_trans, pre_date, tableName, layer)
    df_fullSnapshot_final = snapShotDataCreation(df_trans, cur_date, pre_date, tableName, layer)

    # Joining users and transactions data
    tableName = "Users_transactions_cdp"
    layer = "consumption"
    finalJoinDF = joinUsersTransactions(transformed_Users_DF, df_fullSnapshot_final)
    writeCsv(finalJoinDF, cur_date, tableName, layer)

    # Aggregation Total amound spend by each user
    tableName = "Users_transactions_cdp"
    layer = "consumption"
    snapshotDataDF = readSnapshotData(cur_date, tableName, layer)
    aggregateDataDF = aggregateData(snapshotDataDF)

    tableName = "Aggregate_data"
    layer = "aggregation"
    writeCsv(aggregateDataDF, cur_date, tableName, layer)

    aggregateDataDF.show()








