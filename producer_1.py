from kafka import KafkaProducer
import json
from csv import DictReader
from datetime import datetime, timedelta
import os

bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']
topicname = 'for_mappers_1'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
# producer = KafkaProducer()

backup_file = "./start_day_1_backup.txt"

# counter = 0

# Read the last saved start_day from the file
try:
    with open("./start_day_1.txt", "r") as file:
        if os.stat("./start_day_1.txt").st_size != 0:        
            start_day = file.read().strip()
            # print(start_day)
            start_day = datetime.strptime(start_day, "%Y-%m-%d %H:%M:%S").date()
        else:
            if os.stat(backup_file).st_size != 0: 
                with open(backup_file, "r") as backup:
                    start_day = backup.read().strip()
                    # print(start_day)
                    start_day = datetime.strptime(start_day, "%Y-%m-%d %H:%M:%S").date()
            else:
                start_day = None
                # print(start_day)
except FileNotFoundError:
    start_day = None


    

with open('./dataset/Citi_Bike_trip_data.csv','r') as read_obj:
    csv_dict_reader = DictReader(read_obj)

    if start_day == None:
        first_row = next(csv_dict_reader)
        datetime_str = first_row['starttime']
        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        start_day = datetime_obj.date()
        selected_columns = {
            'start_lat': first_row['start station latitude'],
            'start_long': first_row['start station longitude'],
        }
        producer.send(topicname, json.dumps(selected_columns).encode('utf-8'))
        # counter += 1
        with open("./start_day_1.txt", "w") as file:
            file.write(f"{datetime_str}")
        with open(backup_file, "w") as backup:
            backup.write(f"{datetime_str}")




    for row in csv_dict_reader:
        datetime_str = row['starttime']
        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        day = datetime_obj.day

        if datetime_obj.date() >= start_day and datetime_obj.date() < start_day + timedelta(days=10):
            day = datetime_obj.day
            
            selected_columns = {
                'start_lat': row['start station latitude'],
                'start_long': row['start station longitude'],
            }
            
            producer.send(topicname, json.dumps(selected_columns).encode('utf-8'))
            # counter += 1
            with open("./start_day_1.txt", "w") as file:
                file.write(f"{datetime_str}")
            with open(backup_file, "w") as backup:
                backup.write(f"{datetime_str}")
        else:
            with open("./start_day_1.txt", "w") as file:
                file.write(f"{datetime_str}")
            with open(backup_file, "w") as backup:
                backup.write(f"{datetime_str}")
            if datetime_obj.date() >= start_day + timedelta(days=10):
                producer.flush()
                break
        # print(counter)
        