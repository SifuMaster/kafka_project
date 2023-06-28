from kafka import KafkaConsumer
import json
from minio import Minio
import io
from collections import Counter
import folium
import folium
import time
from selenium import webdriver
from PIL import Image
import os



def find_top_n(n, d):
    counter = Counter(d)
    result = dict(counter.most_common(n))
    # return list(result)
    return result
 

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
    data_stream = io.BytesIO(data)    
    client.put_object("time.results",'regiments', data_stream, len(data))




# Prepare consumer
topics = ["for_reducers_1", "for_reducers_2"]

configs = {
    "group_id": None,
    "consumer_timeout_ms": 100,
    "api_version": (0,9)
}

consumer = KafkaConsumer(*topics, **configs)

i=0

running = True
while running:
    
    # while not consumer.poll():{}
    
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

    # Station regiments needed for the map
    response = client.get_object("time.results", "regiments")
    data = response.data
    R = json.loads(data.decode('utf-8'))
        

    new = False
    for msg in consumer:
        new =True
        i+=1
        print(i)
        if msg.topic == "for_reducers_1":
            msg_dict= json.loads(msg.value.decode('utf-8'))
            key = list(msg_dict.keys())[0]
            Q[key] += msg_dict[key]
        elif msg.topic == "for_reducers_2":
            msg_dict= json.loads(msg.value.decode('utf-8'))
            key = list(msg_dict.keys())[0]
            # print(key)
            if str(msg_dict[key]) in T[key]:
                T[key][str(msg_dict[key])] += 1
            else:
                T[key][str(msg_dict[key])] = 1
                if str(msg_dict[key]) not in R:
                    R[str(msg_dict[key])] = (msg_dict['lat'], msg_dict['long'])

    if new == True:
        # Update stations regiments
        R_json = json.dumps(R)
        data = str(R_json).encode('utf-8')
        data_stream = io.BytesIO(data)
        client.put_object("time.results", 'regiments', data_stream, len(data))




        # If more than one reducers, I should lock here!

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

        # 1
        Q["Q1"] += Q1_from_bucket
        Q["Q2"] += Q2_from_bucket
        Q["Q3"] += Q3_from_bucket
        Q["Q4"] += Q4_from_bucket


        for i in range(1,5):
            data = str(Q["Q"+str(i)]).encode('utf-8')
            data_stream = io.BytesIO(data)
            client.put_object("quadrants.results",'Q'+str(i), data_stream, len(data))


        # 2
        for (T_from_bucket, T_new, i) in [(T1_from_bucket, T["T1"], 1) , (T2_from_bucket, T["T2"], 2), 
                                    (T3_from_bucket, T["T3"], 3), (T4_from_bucket, T["T4"], 4)]:
            for station in list(T_new.keys()):
                if station in T_from_bucket:
                    T_from_bucket[station] += T_new[station]
                else:
                    T_from_bucket[station] = T_new[station]
            
            json_string = json.dumps(T_from_bucket)
            data = json_string.encode('utf-8')
            data_stream = io.BytesIO(data)    
            client.put_object("time.results",'T'+str(i), data_stream, len(data))
            T['T'+str(i)] = find_top_n(10, T_from_bucket)


        # Draw the map

        # Create a map object
        m = folium.Map(location=[40.7128, -74.0060], zoom_start=12)  # New York coordinates

        response = client.get_object("time.results", "regiments")
        data = response.data
        R = json.loads(data.decode('utf-8'))

        for (i, c) in [(1,'red'), (2,'blue'), (3,'green'), (4,'black')]:
            for s in T['T'+str(i)]:
                # print(s)
                lat = R[s][0]
                long =  R[s][1]
                folium.Marker([lat, long], popup=s, icon=folium.Icon(color=c)).add_to(m)


        # # Create a temporary HTML file
        html_file = 'map.html'
        m.save(html_file)
        path = os.path.abspath("map.html")

        # Here Chrome  will be used
        driver = webdriver.Chrome()
        
        url = f'file://{path}'
        driver.get(url)

        time.sleep(5)  # Wait for the map to load

        
        driver.save_screenshot("image.png")
        
        image = Image.open("image.png")
        image.show()

        print(Q)
        print(T)
