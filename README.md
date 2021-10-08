# how to run locally

0. This code is used to get message from twitter, or hard code message, use camel to send a specific partition, refered: https://github.com/apache/camel-examples/blob/main/examples/kafka/src/main/java/org/apache/camel/example/kafka/MessagePublisherClient.java and apache-kafka-series in safari book

1. start zookeeper 

```
zookeeper-server-start /usr/local/etc/zookeeper/zoo.cfg
```

note: zookeeper will run on 0.0.0.0/0.0.0.0:2181

2. start kafka server
 
```
kafka-server-start /usr/local/etc/kafka/server.properties
```

3. create a topic

```
kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic
twitter_tweets --partitions 6 --replication-factor 1
```
 
4. check consumer

```
Kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic
twitter_tweets
```
