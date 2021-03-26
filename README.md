Requirements

- Java installed, the demo / lab git project uses Java 11. (that is what I have installed.)
- IntelliJ is better than best for this :-P.

- Kafka installed. I will setup servers. Do not bother about starting kafka and zookeeper locally,
no cluster no fun. My Kafka servers are 2.6.1 , with Scala 2.12. 
  - Download the tgz archive https://kafka.apache.org/downloads
  - put it somewhere convenient, most of the helpers I provide will assume that it is 
    located at /opt/kafka on your machine. This makes it very painful to use windows without 
    ubuntu/bash :-), so install that too if you are forced to use windows... 
    Otherwise I hope you figure that one out yourself.
    But for all Linux/Mac users this is what I did...:
    ```
    > curl http://apache.mirrors.spacedump.net/kafka/2.6.1/kafka_2.12-2.6.1.tgz --output /tmp/kafka.tgz
    > cd /opt
    > sudo tar xvfz /tmp/kafka.tgz
    > sudo ln -s kafka_2.12-2.6.1 kafka
    > sudo chown -R dawi:dawi /opt/kafka /opt/kafka_2.12-2.6.1
    ```

- if you want to be really prepared, also setup the aliases i have in 'aliases.sh'
  BUT: Make sure to add the IP address variables I provided in the email first in the
  file before executing the command below, otherwise you will have issues reaching the 
  servers. ALSO: This file actually refers your kafka install above, so if you did install 
  somewhere else than /opt/kafka you better update the path to the binaries as well...
  ```
  $ source aliases.sh
  ```

# Mina noteringar
## Kommandon
k-topic  --create --topic cag-checkin --replication-factor --partitions
k-consumer --topic cag-checkin
k-producer cag-checkin

k-groups --list

k-consumer --topic cag-checkin --from-beginning

Hur starta från därman var senast?

## Partitioner
Dela upp i partitioner för att kunna lastdela consumers.
Ha fler consumers än partitions för att få redundans.
Om man har fler partitions än consumers så kopplar consumern upp sig på alla partitioner.
