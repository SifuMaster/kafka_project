from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
import json
from json import loads
from csv import DictReader
from datetime import datetime, timedelta

# Required setting for Kafka Producer
bootstrap_servers = ['localhost:9092']
topicname = 'for_mappers_2'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

start_day = None

# Read the last saved start_day from the file
try:
    with open("./start_day.txt", "r") as file:
        start_day = file.read().strip()
        # print(start_day)
        start_day = datetime.strptime(start_day, "%Y-%m-%d %H:%M:%S").date()
        # print(start_day)
except FileNotFoundError:
    pass
    



# iterate over each line as a ordered dictionary and print only few column by column name
with open('./dataset/Citi_Bike_trip_data.csv','r') as read_obj:

    csv_dict_reader = DictReader(read_obj)

    if start_day == None:
        # Get the first row to determine the start day
        first_row = next(csv_dict_reader)
        
        # Extract the value from the datetime column
        datetime_str = first_row['starttime']
        
        # Convert the datetime string to a datetime object
        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        
        # Determine the start day from the first read
        start_day = datetime_obj.date()

    for row in csv_dict_reader:
        # Extract the value from the datetime column
        datetime_str = row['starttime']
        
        # Convert the datetime string to a datetime object
        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        
        # Extract the day from the datetime object
        day = datetime_obj.day

        # Check if the current row's day is within the desired range
        if datetime_obj.date() >= start_day and datetime_obj.date() < start_day + timedelta(days=10):
            # Extract the day from the datetime object
            day = datetime_obj.day
            
            selected_columns = {
                'start_time': row['starttime'],
                'station_id': row['start station id'],
                'station_lat': row['start station latitude'],
                'station_long': row['start station longitude']
            }
            
            ack = producer.send(topicname, json.dumps(selected_columns).encode('utf-8'))
            metadata = ack.get()
            # print(metadata.topic, metadata.partition)
            # print(datetime_str)
        else:
            with open("./start_day.txt", "w") as file:
                file.write(f"{datetime_str}")
            if datetime_obj.date() >= start_day + timedelta(days=10):
                break
            