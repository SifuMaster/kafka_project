from kafka import KafkaConsumer
consumer = KafkaConsumer('for_mappers', max_poll_records=10)
for msg in consumer:
    print (msg.value.decode('utf-8'))