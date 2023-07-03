from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, time
from multiprocessing import Process


def mapper():
    print("I started")
    center_lat = 40.735923
    center_long = -73.990294

    # Prepare consumer
    topics = ["for_mappers_1", "for_mappers_2"]

    configs = {
        "group_id": "mappers",
        "api_version": (0,9)
    }



    consumer = KafkaConsumer(*topics, **configs)

    # Prepare producer
    bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers)


    # i = 0
    # Start polling
    running = True
    while running:
        
        # while not consumer.poll():{} # wait for messages

        for msg in consumer:
            # i += 1
            print(i)
            if msg.topic == "for_mappers_1":
                
                start_lat = float(json.loads(msg.value.decode('utf-8'))["start_lat"])
                start_long = float(json.loads(msg.value.decode('utf-8'))["start_long"])

                if start_lat >= center_lat and start_long < center_long:
                    msg_to_send = {'Q1': 1}
                elif start_lat > center_lat and start_long >= center_long:
                    msg_to_send = {'Q2': 1}
                elif start_lat <= center_lat and start_long > center_long:
                    msg_to_send = {'Q3': 1}
                else:
                    msg_to_send = {'Q4': 1}
                producer.send("for_reducers_1", json.dumps(msg_to_send).encode('utf-8'))
                # print(msg_to_send)

            elif msg.topic == "for_mappers_2":
                
                start_time = datetime.strptime(json.loads(msg.value.decode('utf-8'))["start_time"], '%Y-%m-%d %H:%M:%S').time()
                station_id = json.loads(msg.value.decode('utf-8'))["station_id"]
                station_lat = float(json.loads(msg.value.decode('utf-8'))["station_lat"])
                station_long = float(json.loads(msg.value.decode('utf-8'))["station_long"])

                
                if start_time >= time(12, 0, 0) and start_time < time (18, 0, 0):
                    msg_to_send = {'T1': station_id, 'lat': station_lat, 'long': station_long}
                elif start_time >= time(18, 0, 0) and start_time <= time (23, 59, 59):
                    msg_to_send = {'T2': station_id, 'lat': station_lat, 'long': station_long}
                elif start_time >= time(0, 0, 0) and start_time < time (6, 0, 0):
                    msg_to_send = {'T3': station_id, 'lat': station_lat, 'long': station_long}
                else:
                    msg_to_send = {'T4': station_id, 'lat': station_lat, 'long': station_long}
                producer.send("for_reducers_2", json.dumps(msg_to_send).encode('utf-8'))
            # print(msg_to_send)


def main():
    # Create and start multiple consumer processes
    processes = []
    for _ in range(3):
        process = Process(target=mapper)
        process.start()
        processes.append(process)

    # Wait for all processes to complete
    for process in processes:
        process.join()


if __name__ == '__main__':
    main()
