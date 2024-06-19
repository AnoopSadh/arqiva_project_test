from jproperties import Properties


def readPropertyFile():
    configs = Properties()
    item = []
    with open('config/param.properties', 'rb') as config_file2:
        configs.load(config_file2)
        history_flag = configs.get("history_flag").data
        ignoreSchemaChange = configs.get("ignoreSchemaChange").data
        item.extend([history_flag, ignoreSchemaChange])

    return item
