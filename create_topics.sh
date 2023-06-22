#!/bin/bash

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_mappers_1 --bootstrap-server localhost:9092"

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_mappers_2 --bootstrap-server localhost:9092"

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_reducers_1 --bootstrap-server localhost:9092"

sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_reducers_2 --bootstrap-server localhost:9092"
