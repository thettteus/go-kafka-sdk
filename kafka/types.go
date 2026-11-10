package kafka

import (
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	inner *ckafka.Producer
}

type Consumer struct {
	inner *ckafka.Consumer
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp int64
}
