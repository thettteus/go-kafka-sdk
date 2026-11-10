package kafka

import "testing"

func TestNewConfigFromEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "localhost:9092,localhost:9093")
	t.Setenv("KAFKA_GROUP_ID", "test-group")
	t.Setenv("KAFKA_TOPICS", "topic1,topic2")

	cfg := NewConfigFromEnv()
	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("expected parsed brokers, got: %v", cfg.Brokers)
	}
	if cfg.GroupID != "test-group" {
		t.Errorf("expected group id 'test-group', got: %s", cfg.GroupID)
	}
	if len(cfg.Topics) != 2 || cfg.Topics[1] != "topic2" {
		t.Errorf("expected parsed topics, got: %v", cfg.Topics)
	}
}
