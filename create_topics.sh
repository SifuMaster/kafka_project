#!/bin/bash
sudo su - kafka -c "~/kafka/bin/kafka-topics.sh --create --topic for_mappers --bootstrap-server localhost:9092"
