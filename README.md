# flink-data-mesh-pipeline

> Part of the **Code Solutions Event-Driven & Streaming Toolkit** product line. Reference implementation of a Data Mesh architecture with Apache Flink and Apache Kafka.

Pipeline implementing **Data Mesh** with two independent data products, each with its own schema, owned by its own team, with enrichment at the platform layer.

## Why this base

- **Data Mesh** done right: two independent data products (orders + customers), each with their own contract
- **Platform-level enrichment**: customer attributes joined into the order stream at the platform layer, not duplicated
- **End-to-end Flink + Kafka**: demonstrates the modern stream-processing stack
- **Pydantic contracts**: type-safe schemas shared between producers and consumers

## Quick start

**Prerequisites:** Java + sbt + Docker (for Kafka + Flink).

```bash
# 1) Start Kafka + Flink
docker compose up -d

# 2) Run the pipeline
sbt run
```

The pipeline will:
- Consume `orders` and `customers` topics
- Enrich each order with customer attributes
- Emit to `orders-enriched` topic
- Sink to Elasticsearch for analytics (optional)

## Run the tests

```bash
sbt test
```

## Extend for real use

- Add your own data products (one schema per team)
- Add new enrichment sources (Redis cache, external APIs)
- Add exactly-once checkpoints
- Add dead-letter queues for malformed events

## Tech stack

- Scala 2.12
- Apache Flink 1.x
- Apache Kafka 2.x
- Pydantic-style contracts (Scala)
- sbt build tool

> **Português?** Veja [`README.pt-BR.md`](./README.pt-BR.md).

## See also

- **Related base**: [flink-kafka-scala-base](https://github.com/ivamartins/flink-kafka-scala-base), [akka-scala-base](https://github.com/ivamartins/akka-scala-base)
- **Product line**: [Event-Driven & Streaming Toolkit](https://ivamartins.github.io/code-solutions-site/#produtos)
- **Code Solutions on LinkedIn**: [linkedin.com/company/code-solutions-it](https://www.linkedin.com/company/code-solutions-it/)
- **All Code Solutions open source**: [github.com/ivamartins](https://github.com/ivamartins)

## License

MIT — see `LICENSE`.
