package kafka

import (
	"context"
	"strings"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// NewProducer creates a Kafka producer with the given brokers and configuration.
// Returns a configured *Producer or an error if creation fails.
func NewProducer(brokers []string, config map[string]string) (*Producer, error) {
	conf := &ckafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
		"acks":              "all",
	}
	for k, v := range config {
		(*conf)[k] = v
	}
	p, err := ckafka.NewProducer(conf)
	if err != nil {
		return nil, err
	}
	return &Producer{inner: p}, nil
}

// StartDeliveryHandler starts a background goroutine to handle delivery reports for the given producer.
// Call this once after creating the producer, and ensure the goroutine is stopped on shutdown.
func (p *Producer) StartDeliveryHandler(ctx context.Context, reportFunc func(*Message)) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-p.inner.Events():
				switch ev := e.(type) {
				case *ckafka.Message:
					if reportFunc != nil {
						reportFunc(&Message{
							Topic:     *ev.TopicPartition.Topic,
							Partition: ev.TopicPartition.Partition,
							Offset:    int64(ev.TopicPartition.Offset),
							Key:       ev.Key,
							Value:     ev.Value,
							Timestamp: ev.Timestamp.UnixMilli(),
						})
					}
				}
			}
		}
	}()
}

// Produce sends a message to the given Kafka topic asynchronously.
// Errors are only returned if the message could not be queued for delivery.
func (p *Producer) Produce(_ context.Context, msg *Message) error {
	return p.inner.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &msg.Topic, Partition: ckafka.PartitionAny},
		Value:          msg.Value,
		Key:            msg.Key,
	}, nil)
}

// Close shuts down the producer, waiting for any outstanding messages to be delivered.
func (p *Producer) Close() {
	p.inner.Close()
}

// Flush waits for all messages in the producer queue to be delivered or the
// specified timeout to elapse, whichever comes first. It returns the number of
// messages that were not delivered before the timeout.
func (p *Producer) Flush(timeoutMs int) int {
	return p.inner.Flush(timeoutMs)
}
