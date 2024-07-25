import datetime
import yaml
import csv


def log(txt):
    with open("log.txt", "a", encoding="UTF-8") as log_file:
        timestamp = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        log_file.writelines(f"{timestamp} {txt}\n")
        print(txt)

def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def data_to_csv(data, filename, table_name):
    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([table_name])
        dict_writer = csv.DictWriter(file, fieldnames=data[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(data)
