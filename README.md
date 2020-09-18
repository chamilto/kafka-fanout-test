# Producers

## Local without k8s (MacOS):

#### install and run kafka
```
brew install kafka

zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
```

#### run producer to create messages
```
python simple_producer.py -t test-simple-producer -b localhost:9092
```

#### view topic and message count

```
kafka-topics --list --zookeeper localhost:2181

kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic test-simple-producer --time -1 --offsets 1 | awk -F ":" '{sum
+= $3} END {print sum}'
```

#### view messages using default console consumer
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic test-simple-producer --from-beginning
```
