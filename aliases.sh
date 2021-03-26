
alias k-config-topic="/opt/kafka/bin/kafka-configs.sh --zookeeper ${ZOOKEEPER} --entity-type topics --entity-name "
alias k-topic="/opt/kafka/bin/kafka-topics.sh --zookeeper ${ZOOKEEPER} "
alias k-consumer="/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${KAFKA} "
alias k-groups="/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server ${KAFKA} "
alias k-producer="/opt/kafka/bin/kafka-console-producer.sh --broker-list ${KAFKA} --topic "

#alias k-config-topic='/opt/kafka2/bin/kafka-configs.sh --zookeeper 192.168.1.52:2181,192.168.1.53:2181,192.168.1.54:2181 --entity-type topics --entity-name '
#alias k-topic='/opt/kafka2/bin/kafka-topics.sh --zookeeper 192.168.1.52:2181,192.168.1.53:2181,192.168.1.54:2181 '
#alias k-consumer='/opt/kafka2/bin/kafka-console-consumer.sh --bootstrap-server 192.168.1.52:9092,192.168.1.53:9092,192.168.1.54:9092 '
#alias k-groups='/opt/kafka2/bin/kafka-consumer-groups.sh --bootstrap-server 192.168.1.52:9092,192.168.1.53:9092,192.168.1.54:9092 '
#alias k-producer='/opt/kafka2/bin/kafka-console-producer.sh --broker-list 192.168.1.52:9092,192.168.1.53:9092,192.168.1.54:9092 --topic '

# tail:                  k-consumer -topic <>
# less:                  k-consumer --from-beginning -topic <>
# list consumers:        k-groups --list
# reset consumer group:  k-groups --reset-offsets <to where> --group <> --topic <> --execute
# current group offset:  k-groups --describe --new-consumer --group <>
