../bin/kafka-mirror-maker.sh --consumer.config hangzhou-consumer.properties \
--producer.config ucloud-producer.properties --num.producers 5 --num.streams 5 \
--queue.size 2000 --whitelist 'moretv-rec4search-topvideo|moretv-rec4search-topvideo-old|usee-rec4search-topvideo|search-score-to-cms|kidsedu-rec4search-topvideo'

