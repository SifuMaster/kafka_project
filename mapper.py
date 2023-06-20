from kafka import KafkaConsumer
consumer = KafkaConsumer('for_mappers_1', max_poll_records=10)
for msg in consumer:
    print (msg.value.decode('utf-8'))