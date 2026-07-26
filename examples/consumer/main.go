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

	// Load config from env
	cfg := kafkasdk.NewConfigFromEnv()

	consumer, err := kafkasdk.NewConsumer(cfg.Brokers, cfg.GroupID, cfg.Topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create consumer: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	fmt.Printf("Consuming from %v (group=%s)\n", cfg.Topics, cfg.GroupID)

	err = consumer.Consume(ctx, func(msg *kafkasdk.Message) {
		fmt.Printf("topic=%s partition=%d key=%s value=%s\n",
			msg.Topic,
			msg.Partition,
			string(msg.Key),
			string(msg.Value),
		)
	})
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		fmt.Fprintf(os.Stderr, "consumer error: %v\n", err)
	}
}
