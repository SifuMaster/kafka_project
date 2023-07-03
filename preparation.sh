#!/bin/bash

sudo systemctl start kafka_0

sudo systemctl start kafka_1

sudo systemctl start kafka_2 

# Create topics
sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_mappers_1 --partitions 3 --replication-factor 3 --bootstrap-server localhost:9092,localhost:9093,localhost:9094"

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_mappers_2 --partitions 3 --replication-factor 3 --bootstrap-server localhost:9092,localhost:9093,localhost:9094"

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_reducers_1 --partitions 3 --replication-factor 3 --bootstrap-server localhost:9092,localhost:9093,localhost:9094"

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_reducers_2 --partitions 3 --replication-factor 3 --bootstrap-server localhost:9092,localhost:9093,localhost:9094"

# Exctract dataset
mkdir ./dataset
unzip dataset.zip -d dataset/
mv ./dataset/2013-11\ -\ Citi\ Bike\ trip\ data.csv ./dataset/Citi_Bike_trip_data.csv
