#!/bin/bash

# Create topics
sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_mappers_1 --bootstrap-server localhost:9092"

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_mappers_2 --bootstrap-server localhost:9092"

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_reducers_1 --bootstrap-server localhost:9092"

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_reducers_2 --bootstrap-server localhost:9092"

# Exctract dataset
mkdir ./dataset
unzip dataset.zip -d dataset/
mv ./dataset/2013-11\ -\ Citi\ Bike\ trip\ data.csv ./dataset/Citi_Bike_trip_data.csv
