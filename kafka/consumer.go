package kafka

import (
	"context"
	"log"
	"strings"
	"sync"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// BlockForever is a constant used to indicate that the consumer should block indefinitely when reading messages.
const BlockForever = -1

// NewConsumer creates and subscribes a Kafka consumer using the provided brokers, groupID, topics, and config.
// Returns a configured *Consumer or an error if creation or subscription fails.
func NewConsumer(brokers []string, groupID string, topics []string, config map[string]string) (*Consumer, error) {
	conf := &ckafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	}
	for k, v := range config {
		(*conf)[k] = v
	}
	c, err := ckafka.NewConsumer(conf)
	if err != nil {
		return nil, err
	}
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}
	return &Consumer{inner: c}, nil
}

// Close shuts down the consumer.
func (c *Consumer) Close() {
	if err := c.inner.Close(); err != nil {
		log.Printf("Failed to close consumer: %v", err)
	}
}

// Consume reads messages from the Kafka consumer and calls the handler for each message.
// It blocks until the context is cancelled or an error occurs.
func (c *Consumer) Consume(ctx context.Context, handler func(msg *Message)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			ev := c.inner.Poll(100) // 100 ms timeout
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *ckafka.Message:
				handler(&Message{
					Topic:     *e.TopicPartition.Topic,
					Partition: e.TopicPartition.Partition,
					Offset:    int64(e.TopicPartition.Offset),
					Key:       e.Key,
					Value:     e.Value,
					Timestamp: e.Timestamp.UnixMilli(),
				})
			case ckafka.Error:
				log.Printf("Consumer error: %v", e)
			}
		}
	}
}

// ConsumeBatch reads messages from the Kafka consumer in batches and calls the handler with a slice of messages.
// It blocks until the context is cancelled or an error occurs.
func (c *Consumer) ConsumeBatch(ctx context.Context, handler func(msgs []*Message)) error {
	batch := make([]*Message, 0, 100)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			ev := c.inner.Poll(100) // 100 ms timeout
			if ev == nil {
				if len(batch) > 0 {
					handler(batch)
					batch = batch[:0]
				}
				continue
			}
			switch e := ev.(type) {
			case *ckafka.Message:
				batch = append(batch, &Message{
					Topic:     *e.TopicPartition.Topic,
					Partition: e.TopicPartition.Partition,
					Offset:    int64(e.TopicPartition.Offset),
					Key:       e.Key,
					Value:     e.Value,
					Timestamp: e.Timestamp.UnixMilli(),
				})
				if len(batch) >= 100 {
					handler(batch)
					batch = batch[:0]
				}
			case ckafka.Error:
				log.Printf("Consumer error: %v", e)
			}
		}
	}
}

// StartConsumers starts multiple Kafka consumers in separate goroutines.
// Each consumer is created using the newConsumer function and processes messages using the provided handler.
// The function blocks until all consumers have exited.
// Returns an error if any consumer fails to start.
func StartConsumers(ctx context.Context, num int, newConsumer func() (*Consumer, error), handler func(msg *Message)) error {
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer, err := newConsumer()
			if err != nil {
				log.Printf("Failed to create consumer: %v", err)
				return
			}
			defer consumer.Close()
			_ = consumer.Consume(ctx, handler)
		}()
	}
	wg.Wait()
	return nil
}
