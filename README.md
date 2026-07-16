# go-kafka-sdk

Thin, ergonomic Go wrappers around [`confluent-kafka-go`](https://github.com/confluentinc/confluent-kafka-go) for producing and consuming Kafka messages with less boilerplate.

[![Go Reference](https://pkg.go.dev/badge/github.com/tzpereira/go-kafka-sdk.svg)](https://pkg.go.dev/github.com/tzpereira/go-kafka-sdk)
[![Release](https://img.shields.io/github/v/tag/tzpereira/go-kafka-sdk?label=release)](https://github.com/tzpereira/go-kafka-sdk/tags)

## Why

`confluent-kafka-go` is a faithful binding to `librdkafka` — powerful, but low-level. This SDK keeps that engine underneath and adds a small, opinionated surface on top:

- **One `Message` type** for both producing and consuming (`Topic`, `Partition`, `Offset`, `Key`, `Value`, `Timestamp`) instead of juggling `kafka.Message` / `kafka.TopicPartition`.
- **Sensible defaults baked in** — producers set `acks=all`, consumers set `auto.offset.reset=earliest` — while still letting you override any `librdkafka` setting via a plain `map[string]string`.
- **`context.Context`-aware loops** — `Consume`, `ConsumeBatch`, and the delivery handler all stop cleanly when their context is cancelled.
- **Helpers for common shapes** — batch consumption (`ConsumeBatch`) and a fixed-size consumer worker pool (`StartConsumers`).
- **Env-driven config** — `NewConfigFromEnv` reads brokers, group ID, and topics straight from environment variables.

It does **not** add serialization (keys/values are raw `[]byte`), schema registry integration, or automatic retry/backoff — those remain your application's responsibility.

## Requirements

- **Go 1.24+** (the module declares `go 1.24.5`).
- **CGO enabled** and **`librdkafka`** available — `confluent-kafka-go v1.9.2` binds to it. On most platforms the bundled static build works out of the box; some ARM environments need a system `librdkafka` (see [Build](#build-amd64--arm64)).
- **A running Kafka broker.** A `docker-compose.yml` for local development is included below.

## Installation

```sh
go get github.com/tzpereira/go-kafka-sdk
```

The public API lives in the `kafka` package:

```go
import kafkasdk "github.com/tzpereira/go-kafka-sdk/kafka"
```

The examples also use [`godotenv`](https://github.com/joho/godotenv) to load a `.env` file automatically:

```sh
go get github.com/joho/godotenv
```

## Quickstart

Both snippets read their configuration from the environment (see [Configuration](#configuration)).

### Producer

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/joho/godotenv/autoload"
	kafkasdk "github.com/tzpereira/go-kafka-sdk/kafka"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := kafkasdk.NewConfigFromEnv()
	if len(cfg.Topics) == 0 {
		fmt.Fprintln(os.Stderr, "no topics defined via KAFKA_TOPICS")
		os.Exit(1)
	}
	topic := cfg.Topics[0]

	producer, err := kafkasdk.NewProducer(cfg.Brokers, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create producer: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	// Delivery reports are delivered asynchronously to this callback.
	producer.StartDeliveryHandler(ctx, func(m *kafkasdk.Message) {
		fmt.Printf("delivered to topic %s partition %d\n", m.Topic, m.Partition)
	})

	err = producer.Produce(ctx, &kafkasdk.Message{
		Topic: topic,
		Value: []byte("hello from go-kafka-sdk"),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "produce error: %v\n", err)
	}

	// Block until queued messages are delivered (or 5s elapses).
	producer.Flush(5000)
}
```

Run it:

```sh
go run ./examples/producer
```

### Consumer

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/joho/godotenv/autoload"
	kafkasdk "github.com/tzpereira/go-kafka-sdk/kafka"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
			msg.Topic, msg.Partition, string(msg.Key), string(msg.Value))
	})
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		fmt.Fprintf(os.Stderr, "consumer error: %v\n", err)
	}
}
```

Run it:

```sh
go run ./examples/consumer
```

## Configuration

### `Config` and `NewConfigFromEnv`

`NewConfigFromEnv` builds a `*Config` from environment variables. Comma-separated lists are split and trimmed.

| Field     | Type       | Environment variable | Description                                           |
| --------- | ---------- | -------------------- | ----------------------------------------------------- |
| `Brokers` | `[]string` | `KAFKA_BROKERS`      | Comma-separated broker addresses (e.g. `host:9092`).  |
| `GroupID` | `string`   | `KAFKA_GROUP_ID`     | Consumer group ID.                                    |
| `Topics`  | `[]string` | `KAFKA_TOPICS`       | Comma-separated topics to produce to / subscribe to.  |

Example `.env` (matches [`.env.example`](.env.example)):

```sh
KAFKA_BROKERS=localhost:9092
KAFKA_GROUP_ID=test-group
KAFKA_TOPICS=test-topic
```

`Config` is a plain struct — you can also build it directly instead of reading the environment:

```go
cfg := &kafkasdk.Config{
	Brokers: []string{"localhost:9092"},
	GroupID: "my-group",
	Topics:  []string{"orders"},
}
```

### Tuning `librdkafka`

`NewProducer` and `NewConsumer` accept a `config map[string]string` that is passed straight through to the underlying `librdkafka` `ConfigMap`. Any entry you provide overrides the SDK defaults. Pass `nil` to accept the defaults.

| Constructor   | Default key         | Default value   | Notes                                  |
| ------------- | ------------------- | --------------- | -------------------------------------- |
| `NewProducer` | `bootstrap.servers` | joined brokers  | Set from the `brokers` argument.       |
| `NewProducer` | `acks`              | `all`           | Override to change durability.         |
| `NewConsumer` | `bootstrap.servers` | joined brokers  | Set from the `brokers` argument.       |
| `NewConsumer` | `group.id`          | `groupID` arg   | Set from the `groupID` argument.       |
| `NewConsumer` | `auto.offset.reset` | `earliest`      | Override to change first-read offset.  |

Example overriding defaults and adding extra `librdkafka` keys:

```go
producer, err := kafkasdk.NewProducer(cfg.Brokers, map[string]string{
	"acks":               "1",
	"compression.type":   "snappy",
	"enable.idempotence": "true",
})
if err != nil {
	return fmt.Errorf("create producer: %w", err)
}
defer producer.Close()
```

See the [`librdkafka` configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) for the full list of valid keys.

## Usage patterns

### Error handling

There are three distinct error surfaces; treat them separately.

- **`Produce` return value** — reports only whether the message was *enqueued*. Actual delivery happens asynchronously.

  ```go
  if err := producer.Produce(ctx, &kafkasdk.Message{Topic: topic, Value: payload}); err != nil {
      // queue is full or the producer is closed
      return fmt.Errorf("enqueue message: %w", err)
  }
  ```

- **Delivery reports** — register a callback with `StartDeliveryHandler` to observe messages once the broker acknowledges them.

  ```go
  producer.StartDeliveryHandler(ctx, func(m *kafkasdk.Message) {
      log.Printf("delivered: topic=%s partition=%d offset=%d", m.Topic, m.Partition, m.Offset)
  })
  ```

- **Consumer errors** — `Consume` and `ConsumeBatch` log broker-level errors internally (via the standard `log` package) and keep polling; they return an `error` only when the context is cancelled (`context.Canceled` / `context.DeadlineExceeded`). Your handler is only invoked for successfully received messages.

> **Note:** the SDK does not implement retry or backoff. If you need retries (e.g. on a failed handler), wrap your handler logic yourself.

### Graceful shutdown & cancellation

Every loop is driven by a `context.Context`. Cancel it — typically from an OS signal — to stop consuming or to stop the delivery handler goroutine, then `Close()` to release resources.

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

consumer, err := kafkasdk.NewConsumer(cfg.Brokers, cfg.GroupID, cfg.Topics, nil)
if err != nil {
	return fmt.Errorf("create consumer: %w", err)
}
defer consumer.Close()

// Returns context.Canceled when the signal fires; that's the clean-exit path.
if err := consumer.Consume(ctx, handle); err != nil && err != context.Canceled {
	return fmt.Errorf("consume: %w", err)
}
```

For producers, call `Flush` before `Close` to give outstanding messages a chance to be delivered:

```go
producer.Flush(5000) // wait up to 5s
producer.Close()
```

### Batch consumption

`ConsumeBatch` accumulates messages and invokes the handler with a slice. The batch is flushed when it reaches 100 messages, or on the next empty poll if any messages are pending.

```go
err := consumer.ConsumeBatch(ctx, func(msgs []*kafkasdk.Message) {
	if err := writeBatch(ctx, msgs); err != nil {
		log.Printf("batch write failed for %d messages: %v", len(msgs), err)
	}
})
if err != nil && err != context.Canceled {
	return fmt.Errorf("consume batch: %w", err)
}
```

### Consumer worker pool

`StartConsumers` spins up `num` consumers, each in its own goroutine, using a factory you provide. It blocks until every consumer goroutine exits (i.e. when `ctx` is cancelled). This is useful for scaling out consumption within a single process.

```go
newConsumer := func() (*kafkasdk.Consumer, error) {
	return kafkasdk.NewConsumer(cfg.Brokers, cfg.GroupID, cfg.Topics, nil)
}

handle := func(msg *kafkasdk.Message) {
	log.Printf("worker got: %s", string(msg.Value))
}

if err := kafkasdk.StartConsumers(ctx, 4, newConsumer, handle); err != nil {
	return fmt.Errorf("start consumers: %w", err)
}
```

## API reference

All exported symbols live in package `github.com/tzpereira/go-kafka-sdk/kafka`.

### Types

```go
type Config struct {
	Brokers []string
	GroupID string
	Topics  []string
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp int64 // Unix milliseconds
}

type Producer struct { /* unexported fields */ }
type Consumer struct { /* unexported fields */ }
```

### Constants

| Constant       | Value | Description                                                              |
| -------------- | ----- | ------------------------------------------------------------------------ |
| `BlockForever` | `-1`  | Sentinel for "block indefinitely" when reading messages.                 |

### Functions & methods

| Symbol | Signature | Description |
| ------ | --------- | ----------- |
| `NewConfigFromEnv` | `func() *Config` | Build a `Config` from `KAFKA_BROKERS`, `KAFKA_GROUP_ID`, `KAFKA_TOPICS`. |
| `NewProducer` | `func(brokers []string, config map[string]string) (*Producer, error)` | Create a producer (`acks=all` by default). |
| `(*Producer) Produce` | `func(ctx context.Context, msg *Message) error` | Asynchronously enqueue a message; error means it could not be queued. |
| `(*Producer) StartDeliveryHandler` | `func(ctx context.Context, reportFunc func(*Message))` | Start a goroutine delivering delivery reports to `reportFunc`; stops on `ctx` cancel. |
| `(*Producer) Flush` | `func(timeoutMs int) int` | Wait up to `timeoutMs` for delivery; returns the count of undelivered messages. |
| `(*Producer) Close` | `func()` | Shut down the producer, waiting for outstanding messages. |
| `NewConsumer` | `func(brokers []string, groupID string, topics []string, config map[string]string) (*Consumer, error)` | Create and subscribe a consumer (`auto.offset.reset=earliest` by default). |
| `(*Consumer) Consume` | `func(ctx context.Context, handler func(msg *Message)) error` | Poll and invoke `handler` per message until `ctx` is cancelled. |
| `(*Consumer) ConsumeBatch` | `func(ctx context.Context, handler func(msgs []*Message)) error` | Poll and invoke `handler` with batches (flush at 100 or on idle) until `ctx` is cancelled. |
| `(*Consumer) Close` | `func()` | Shut down the consumer. |
| `StartConsumers` | `func(ctx context.Context, num int, newConsumer func() (*Consumer, error), handler func(msg *Message)) error` | Run `num` consumers concurrently; blocks until all exit. |

## Running Kafka locally (Docker Compose)

A ready-to-use [`docker-compose.yml`](docker-compose.yml) is included (Confluent Platform 7.5.0 — ZooKeeper + a single broker on `localhost:9092`):

```sh
docker-compose up -d
```

## Tests

The `kafka` package has two tests:

- `TestNewConfigFromEnv` — pure unit test, no broker required.
- `TestProducerConsumerIntegration` — **requires a Kafka broker on `localhost:9092`** (start it with the Docker Compose file above). It produces a message and consumes it back, asserting on topic, key, value, offset, and timestamp.

Run everything:

```sh
go test -v ./kafka
```

## Build (amd64 + arm64)

Because the SDK depends on `librdkafka` via `confluent-kafka-go`, builds require CGO. A helper script handles the ARM fallback:

```sh
./scripts/build.sh
```

Behavior:

- Always sets `CGO_ENABLED=1`.
- On `arm`/`arm64`, tries a normal (static) build first; if it fails, retries with `TAGS=dynamic` (using a system `librdkafka` located via `pkg-config`).

Examples:

```sh
GOARCH=amd64 ./scripts/build.sh   # native amd64
GOARCH=arm64 ./scripts/build.sh   # native arm64 (normal build first)
TAGS=dynamic ./scripts/build.sh   # force dynamic linking (requires system librdkafka)
```

When **consuming** this module from another project that needs dynamic linking, install the system deps and add the `dynamic` build tag in that project:

```sh
# macOS (Homebrew)
brew install librdkafka pkg-config

# Linux (Debian/Ubuntu)
sudo apt-get install -y librdkafka-dev pkg-config

CGO_ENABLED=1 go build -tags dynamic ./...
CGO_ENABLED=1 go run   -tags dynamic ./...
CGO_ENABLED=1 go test  -tags dynamic ./...
```

## Contributing

1. Start a local broker: `docker-compose up -d`.
2. Make your change and keep it building: `./scripts/build.sh` (or `go build ./...`).
3. Run the tests: `go test -v ./kafka`.
4. Open a pull request against `main` at [tzpereira/go-kafka-sdk](https://github.com/tzpereira/go-kafka-sdk).

## Open questions

These could not be determined from the code and should be confirmed before publishing:

- **Supported Kafka broker versions.** The code pins `confluent-kafka-go v1.9.2` and the Docker Compose file uses Confluent Platform 7.5.0 (Kafka 3.5.x), but no explicit compatibility range is documented. What broker versions are officially supported?
- **License.** There is no `LICENSE` file in the repository. Which license applies?
- **CI.** No CI workflow was found (`.github/workflows` is absent). Is there a CI pipeline whose badge should be added?
- **`BlockForever` constant.** It is exported and documented but not referenced anywhere in the SDK (the poll timeout in `Consume`/`ConsumeBatch` is hard-coded to 100 ms). Is it intended for public use, or dead code?
```