from jproperties import Properties

# configs = Properties()
#
# with open('config/app-config.properties', 'rb') as config_file:
#     configs.load(config_file)
# print(configs.items())
#
# items_view = configs.items()
# list_keys = []
#
# for item in items_view:
#     list_keys.append(item[0])
#
# print(list_keys)
# # ['DB_HOST', 'DB_SCHEMA', 'DB_User', 'DB_PWD']
#
# from datetime import datetime
# from dateutil.relativedelta import relativedelta
#
# date_now = datetime.now().date()
# start_date = date_now + relativedelta(days=+1)
# print(date_now, start_date)

query = """
        CREATE DATABASE IF NOT EXISTS ARQIVADB;
        CREATE TABLE ARQIVADB.TRANSACTIONS (
            TRASANCTION_ID integer,
            user_id integer,
            amount number,
            transaction_date date
        )
"""