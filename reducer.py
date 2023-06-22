from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, time
from minio import Minio
import io

# prepare minio client - everything is default

client = Minio(
    "127.0.0.1:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# create buckets if they dont exist

found = client.bucket_exists("quadrants.results")
if not found:
    client.make_bucket("quadrants.results")

    data = str(0).encode('utf-8')

    data_stream = io.BytesIO(data)
    client.put_object("quadrants.results",'Q1', data_stream, len(data))
    data_stream = io.BytesIO(data)
    client.put_object("quadrants.results",'Q2', data_stream, len(data))
    data_stream = io.BytesIO(data)
    client.put_object("quadrants.results",'Q3', data_stream, len(data))
    data_stream = io.BytesIO(data)
    client.put_object("quadrants.results",'Q4', data_stream, len(data))


found = client.bucket_exists("time.results")
if not found:
    client.make_bucket("time.results")

    dictionary = {}
    json_string = json.dumps(dictionary)
    data = json_string.encode('utf-8')

    data_stream = io.BytesIO(data)    
    client.put_object("time.results",'T1', data_stream, len(data))
    data_stream = io.BytesIO(data)    
    client.put_object("time.results",'T2', data_stream, len(data))
    data_stream = io.BytesIO(data)    
    client.put_object("time.results",'T3', data_stream, len(data))
    data_stream = io.BytesIO(data)    
    client.put_object("time.results",'T4', data_stream, len(data))

    print(data)

# Prepare consumer
topics = ["for_reducers_1", "for_reducers_2"]

configs = {
    "group_id": None,
    "consumer_timeout_ms": 100,
    "api_version": (0,9)
}

consumer = KafkaConsumer(*topics, **configs)

running = True
while running:
    
    while not consumer.poll(timeout_ms=1000):{}
    
    Q = {
        "Q1": 0,
        "Q2": 0,
        "Q3": 0,
        "Q4": 0
    }

    T = {
        "T1": {},
        "T2": {},
        "T3": {},
        "T4": {}
    }

    for msg in consumer:
        if msg.topic == "for_reducers_1":
            msg_dict= json.loads(msg.value.decode('utf-8'))
            key = list(msg_dict.keys())[0]
            Q[key] += msg_dict[key]
        elif msg.topic == "for_reducers_2":
            msg_dict= json.loads(msg.value.decode('utf-8'))
            key = list(msg_dict.keys())[0]
            if str(msg_dict[key]) in T[key]:
                T[key][str(msg_dict[key])] += 1
            else:
                T[key][str(msg_dict[key])] = 1 



    # If more than one reducers i should lock here!

    # Get results from bucket until now    
    response = client.get_object("quadrants.results", "Q1")
    # Read the object's data as bytes
    data = response.data
    Q1_from_bucket = int(data.decode('utf-8'))

    response = client.get_object("quadrants.results", "Q2")
    data = response.data
    Q2_from_bucket = int(data.decode('utf-8'))

    response = client.get_object("quadrants.results", "Q3")
    data = response.data
    Q3_from_bucket = int(data.decode('utf-8'))

    response = client.get_object("quadrants.results", "Q4")
    data = response.data
    Q4_from_bucket = int(data.decode('utf-8'))

    response = client.get_object("time.results", "T1")
    data = response.data
    T1_from_bucket = json.loads(data.decode('utf-8'))

    response = client.get_object("time.results", "T2")
    data = response.data
    T2_from_bucket = json.loads(data.decode('utf-8'))

    response = client.get_object("time.results", "T3")
    data = response.data
    T3_from_bucket = json.loads(data.decode('utf-8'))

    response = client.get_object("time.results", "T4")
    data = response.data
    T4_from_bucket = json.loads(data.decode('utf-8'))


    # Update results in buckets
    Q["Q1"] += Q1_from_bucket
    Q["Q2"] += Q2_from_bucket
    Q["Q3"] += Q3_from_bucket
    Q["Q4"] += Q4_from_bucket

    for i in range(1,5):
        # 1
        data = str(Q["Q"+str(i)]).encode('utf-8')
        data_stream = io.BytesIO(data)
        client.put_object("quadrants.results",'Q'+str(i), data_stream, len(data))
        print(data)
       
        # 2


    





