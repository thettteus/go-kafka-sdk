//go:build integration

// Package kafka integration tests. These require a running Kafka broker on
// localhost:9092 and are only compiled with the "integration" build tag:
//
//	go test -tags integration -v ./kafka
package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestProducerConsumerIntegration(t *testing.T) {
	started := time.Now()
	brokers := []string{"localhost:9092"}
	topic := fmt.Sprintf("test-integration-%d", started.UnixNano())
	groupID := topic + "-group"
	// Local Docker brokers may expose only IPv4, even when localhost resolves
	// to both IPv4 and IPv6. Apply this to advertised broker addresses as well.
	config := map[string]string{"broker.address.family": "v4"}

	testKey := []byte("test-key")
	testValue := []byte("test message")

	// --- Step 1: Produce ---
	producer, err := NewProducer(brokers, config)
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()
	deliveryCtx, stopDelivery := context.WithCancel(context.Background())
	defer stopDelivery()
	// Flush includes queued delivery reports, so drain them while waiting.
	producer.StartDeliveryHandler(deliveryCtx, nil)

	err = producer.Produce(context.Background(), &Message{
		Topic: topic,
		Key:   testKey,
		Value: testValue,
	})
	if err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	if remaining := producer.Flush(30_000); remaining != 0 {
		t.Fatalf("producer flush timed out with %d outstanding events", remaining)
	}

	// --- Step 2: Consume ---
	consumer, err := NewConsumer(brokers, groupID, []string{topic}, config)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	var received *Message

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = consumer.Consume(ctx, func(msg *Message) {
		t.Logf(
			"Consumed message | topic=%s partition=%d offset=%d key=%s value=%s timestamp=%d",
			msg.Topic,
			msg.Partition,
			msg.Offset,
			string(msg.Key),
			string(msg.Value),
			msg.Timestamp,
		)

		if string(msg.Value) == string(testValue) {
			received = msg
			cancel()
		}
	})

	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Fatalf("consumer error: %v", err)
	}

	if received == nil {
		t.Fatal("consumer did not receive the produced message")
	}

	// --- Assertions ---
	if received.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, received.Topic)
	}

	if received.Offset < 0 {
		t.Errorf("expected offset >= 0, got %d", received.Offset)
	}

	if received.Timestamp <= 0 {
		t.Errorf("expected valid timestamp, got %d", received.Timestamp)
	}

	// The producer timestamp must belong to this run, without imposing a
	// ten-second limit on broker startup or consumer group assignment.
	now := time.Now().UnixMilli()
	if received.Timestamp < started.UnixMilli() || received.Timestamp > now {
		t.Errorf("timestamp outside test run: start=%d msg=%d now=%d", started.UnixMilli(), received.Timestamp, now)
	}

	if string(received.Key) != string(testKey) {
		t.Errorf("expected key %s, got %s", testKey, received.Key)
	}

	if string(received.Value) != string(testValue) {
		t.Errorf("expected value %s, got %s", testValue, received.Value)
	}
}
