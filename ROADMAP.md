# Roadmap — go-kafka-sdk

Plano para levar o `go-kafka-sdk` de "wrapper funcional" a SDK de referência,
pronto para produção. As fases estão ordenadas por maturidade: de fundação e
corretude até observabilidade e a estabilização da API 1.0.

> Cada item é rastreável a um gap real no código atual. Os campos "Estado atual"
> refletem a base na data de escrita (2026-07-16).

## Sequência recomendada

```
Fase 0 (LICENSE + CI)
   → Fase 1 (corretude)
   → Fase 2 (DX: options + serialização + error handler)
   → Fase 3 / Fase 4 em paralelo (resiliência + observabilidade)
   → Fase 5 (1.0)
```

**Alavancas de maior impacto** para "SDK de respeito", nesta ordem:
1. Serialização plugável com Schema Registry (Fase 2).
2. Observabilidade — consumer lag + tracing distribuído (Fase 4).

São exatamente os motivos pelos quais times abandonam o client cru e adotam um
SDK. O padrão de options e o tratamento de erro correto (Fases 1–2) são o
alicerce que precisa vir antes delas.

---

## Fase 0 — Fundação e confiança

Pré-requisito para qualquer adoção séria. Sem isso, ninguém coloca em produção.

| Item | Por quê | Estado atual |
| ---- | ------- | ------------ |
| **`LICENSE`** (MIT / Apache-2.0) | Sem licença, empresas legalmente **não podem** usar. Bloqueador nº 1. | Ausente |
| **CI** (GitHub Actions: `build`, `vet`, `test`, `staticcheck`, `golangci-lint`) | Prova que compila e passa a cada PR; gera o badge de CI. | Ausente |
| **Separar unit vs. integration tests** (`-tags integration` ou skip quando não há broker) | Hoje `TestProducerConsumerIntegration` quebra sem broker em `localhost:9092`. CI precisa rodar unit sem Kafka e integração com broker efêmero. | Misturado |
| **`SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, templates de issue/PR** | Sinais de projeto sério; reduz atrito de contribuição. | Ausente |
| **SemVer + `CHANGELOG.md`** | Já existem tags `v0.1.x`; falta disciplina de changelog e release notes. | Parcial |

## Fase 1 — Corretude

Buracos latentes, não features. É o que quebra em produção silenciosamente.

- **Delivery reports não capturam erro.** `StartDeliveryHandler` só trata
  `*ckafka.Message` e ignora `ev.TopicPartition.Error`. Uma mensagem que falha
  na entrega é reportada como sucesso.
  → Adicionar `Err error` ao `Message` do callback, ou um callback de erro separado.
- **Erros do consumer só vão pro `log.Printf`.** Impossível reagir
  programaticamente. → Injetar um `Logger` e/ou um `ErrorHandler func(error)`.
- **Sem controle de commit de offset.** `auto.offset.reset=earliest` +
  auto-commit implícito = risco de perda ou reprocessamento silencioso.
  → Expor commit manual (`CommitMessage`, at-least-once explícito) e documentar
  as garantias de entrega.
- **`BlockForever` é dead code.** Exportado, documentado, nunca usado (poll
  hard-coded em 100 ms). → Usar de verdade ou remover (breaking, aceitável pré-1.0).
- **Batch size (100) e poll timeout (100 ms) hard-coded.** → Tornar configuráveis.

## Fase 2 — Developer Experience

O que faz alguém escolher este SDK em vez de mais um wrapper.

- **Functional options em vez de `map[string]string` cru.** O map é frágil
  (typos silenciosos, sem type-safety). Adotar o padrão idiomático:

  ```go
  p, err := kafka.NewProducer(brokers,
      kafka.WithAcks(kafka.AckAll),
      kafka.WithCompression(kafka.Snappy),
      kafka.WithIdempotence(true),
  )
  ```

  Mantendo um escape hatch `WithRawConfig(map[string]string)` para keys avançadas
  do librdkafka.
- **Camada de serialização plugável.** Hoje só `[]byte`. Definir interfaces
  `Serializer` / `Deserializer` e fornecer implementações JSON e, idealmente,
  Avro/Protobuf + Schema Registry. É o recurso que empresas realmente procuram.
- **Handler que retorna `error`.** Hoje `func(msg *Message)` não sinaliza falha.
  Mudar para `func(ctx, *Message) error` habilita retry, DLQ e commit condicional.
- **Godoc examples executáveis** (`Example_producer`, `Example_consumer`) —
  aparecem no pkg.go.dev e funcionam como testes de documentação.
- **`slog` em vez de `log`** — logging estruturado, injetável, padrão moderno do
  ecossistema Go.

## Fase 3 — Resiliência e produção

- **Retry com backoff configurável** (exponencial + jitter) para o handler do consumer.
- **Dead Letter Queue (DLQ)** — rotear mensagens que esgotaram retries para um
  tópico de erro. Feature "enterprise" muito requisitada.
- **Graceful shutdown de verdade** — hoje `Close()` no consumer não garante
  commit do offset pendente antes de sair. Coordenar drain + commit final.
- **Health checks / readiness** — expor estado da conexão para probes do Kubernetes.

## Fase 4 — Observabilidade

Obrigatório para "top tier".

- **Métricas via OpenTelemetry** — throughput, lag, erros, latência de entrega.
  Consumer lag é a métrica nº 1 que operadores querem.
- **Tracing distribuído** — propagar trace context via headers Kafka
  (W3C `traceparent`).
- **`Message.Headers`** — o struct atual não tem headers, o que bloqueia tracing
  e roteamento por header.

## Fase 5 — Adoção e polimento para 1.0

- **Testes de integração com broker efêmero** (testcontainers-go) —
  determinísticos no CI, sem depender de `localhost:9092`.
- **Benchmarks** (`go test -bench`) publicados — throughput/latência vs.
  confluent-kafka-go cru, provando que a abstração não custa caro.
- **Documentação em site** (godoc + guia de conceitos: entrega, DLQ, tuning).
- **API freeze + `v1.0.0`** com garantia de compatibilidade.
- **`Admin` client opcional** — criar tópicos, inspecionar consumer groups
  (muito pedido em dev/testes).
