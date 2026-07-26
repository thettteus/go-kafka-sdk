package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/joho/godotenv/autoload"
	kafkasdk "github.com/thettteus/go-kafka-sdk/kafka"
)

func main() {
	// Ctrl+C cancel
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load config
	cfg := kafkasdk.NewConfigFromEnv()

	if len(cfg.Topics) == 0 {
		fmt.Println("no topics defined via KAFKA_TOPICS")
		os.Exit(1)
	}
	topic := cfg.Topics[0]

	// Producer
	producer, err := kafkasdk.NewProducer(cfg.Brokers, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create producer: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	// Delivery report
	producer.StartDeliveryHandler(ctx, func(m *kafkasdk.Message) {
		fmt.Printf("delivered to topic %s partition %d\n", m.Topic, m.Partition)
	})

	// Publish test message
	payload := []byte("hello from go-kafka-sdk")
	fmt.Printf("Producing to topic '%s'...\n", topic)

	err = producer.Produce(ctx, &kafkasdk.Message{
		Topic: topic,
		Value: payload,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "produce error: %v\n", err)
	}

	producer.Flush(5000)
}
