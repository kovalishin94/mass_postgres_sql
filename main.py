import os
import datetime
from server import get_server_list
from services import log, load_config, data_to_csv


if __name__ == '__main__':
    config = load_config("config.yaml")
    log("config.yaml was read successfully")
    server_list = get_server_list(config)

    if not os.path.exists("results"):
        os.makedirs("results")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join("results" ,f"result_{timestamp}.csv")

    for server in server_list:        
        result = server.execute_sql()
        if result:
            data_to_csv(result, filename, server.name)
