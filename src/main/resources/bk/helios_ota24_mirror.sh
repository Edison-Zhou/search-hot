#!/usr/bin/env bash
../bin/kafka-mirror-maker.sh --consumer.config hangzhou-consumer.properties --producer.config ucloud-producer.properties --num.producers 5 --num.streams 5 --queue.size 2000 --whitelist 'helios-rec4search-topvideo-ota24-sid|helios-rec4search-topvideo-sid'