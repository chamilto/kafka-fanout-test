# Producer

## Local without k8s (MacOS):

#### install and run kafka
```
brew install kafka

zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
```

#### run producer to create messages
```
# generate 100 messages and exit
python simple_producer.py -t simple-test -b localhost:9092

# generate 10000000000 messages, waiting 1 second between each producer call
python simple_producer.py -t simple-test -b localhost:9092 -n 10000000000 -w 1000
```

#### view topic and message count

```
kafka-topics --list --zookeeper localhost:2181

kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic simple-test --time -1 --offsets 1 | awk -F ":" '{sum
+= $3} END {print sum}'
```

#### view messages using default console consumer
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-test --from-beginning
```

# Consumer
```
go run consumer/cmd/dispatcher/main.go  -brokers="127.0.0.1:9092" -topics="simple-test" -group="test"
```
