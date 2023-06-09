# go-kafka-message-generator
A Kafka message generator, but for Go.

Currently, the generated files only support deserialization.

## To build
```shell
make build
```

## To run
```shell
./kmg generate -i path/to/kafka/messages/jsons/dir -i path/to/other/kafka/messages/json/dir -o output/dir -p packagename
```
