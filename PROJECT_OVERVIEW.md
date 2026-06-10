# flink-data-mesh-pipeline — Overview & flow

**Flink + Kafka** pipeline implementing **Data Mesh**: two independent data products (orders and customers) joined by a platform-level enrichment job. Python for Data Engineering, Apache Flink, Data Mesh, Pydantic, Data Quality.

## Stack (with versions)

- **Scala 2.12.18** (Flink 1.18+ is built against Scala 2.12)
- **sbt** (version in `project/build.properties`)
- **Apache Flink 1.18.1** (`flink-streaming-scala`, `flink-clients`, `flink-json`, `flink-avro`)
- **Flink Kafka Connector 3.0.2-1.18** (`flink-connector-kafka`)
- **Flink Test Utils 1.18.1** + **Flink Runtime Web 1.18.1** + **Flink Tests 1.18.1** (testing)
- **Apache Avro 1.11.3**
- **Spray-JSON 1.3.6**
- **Logback 1.4.14**
- **ScalaTest 3.2.17**
- **Python**: Pydantic (with `EmailStr`/`Literal`) + `pydantic` (see `requirements.txt`)
- **Docker Compose**: Kafka + Flink

---

## Main flow

### 1. Data Mesh pattern
- **Domain ownership**: `orders` and `customers` each with its own case class, contract (Pydantic), Kafka topic and owner.
- **Data as a product**: each product is versioned, validated and consumed independently.
- **Self-serve platform**: the Flink join is a platform capability — domains publish products, not write joins.
- **Federated governance**: Pydantic contracts are the global schema; Scala validators mirror them.

### 2. Execution modes
- **`runWithCollections` (default)**: everything in-process, no Kafka. Creates `StreamExecutionEnvironment` with checkpoint every 5s, parallelism 1, reads hard-coded collections of orders/customers, applies enrichment and prints `EnrichedOrder` to stdout.
- **`runWithKafka`** (production): reads from `orders.input` and `customers.input` via `KafkaSource` (Brokers via `BOOTSTRAP_SERVERS` env var, groupIds `data-mesh-orders`/`data-mesh-customers`, `OffsetsInitializer.earliest()`), applies enrichment, serializes `EnrichedOrder` to JSON and publishes to `orders.enriched`.

### 3. Kafka topics
| Topic              | Function                                   |
|--------------------|--------------------------------------------|
| `orders.input`     | Source: orders data product                |
| `customers.input`  | Source: customers data product             |
| `orders.enriched`  | Output: order enriched with the customer   |

### 4. Enrichment (`CustomerEnrichmentFunction`)
- `BroadcastProcessFunction`: the customer stream is **broadcast** and cached in operator state; the order stream is **connected** (`.connect(broadcastCustomers)`) and receives the join via `process(...)`.
- For each `OrderDataProduct`, enriches with the `customerTier` of the corresponding `CustomerDataProduct`, producing `EnrichedOrder(order, customerTier, enrichedAt)`.

### 5. Validation (`EnrichmentLib` Scala + `data_contracts.py` Python)
Every rule is duplicated in both languages (parity), ensuring federated governance:

| Rule | Pydantic (Python) | Scala (EnrichmentLib) |
|---|---|---|
| `amount > 0` | `Field(gt=0)` | `if (o.amount <= 0) Left(...)` |
| `currency` 3 letters | `min_length=3, max_length=3` | `o.currency.length != 3` |
| `status` enum | `Literal[...]` | `Set("CREATED", ...)` |
| `email` valid | `EmailStr` | `c.email.contains("@")` |
| `tier` enum | `Literal[...]` | `Set("BRONZE", ...)` |

In Scala, `parseOrder` and `parseCustomer` return `Either[Err, T]`, chained with `flatMap(validateOrder)`.

---

## What's in each subfolder

### Root
- `build.sbt` — Scala 2.12.18, Flink 1.18.1, Kafka Connector 3.0.2-1.18, ScalaTest 3.2.17.
- `project/build.properties` — sbt version.
- `requirements.txt` — Python deps (Pydantic, pytest).
- `docker-compose.yml` — starts Kafka + Flink for E2E.
- `README.md` — quickstart, modes, parity table.
- `.gitignore`, `.connection-test` (sentinel).

### `src/main/scala/com/codesolutions/flink/mesh/`
- `DataMeshJob.scala` — `main`; routes between `runWithCollections` (default) and `runWithKafka` (arg `kafka`).
- `CustomerEnrichmentFunction.scala` — `BroadcastProcessFunction` with `customerDescriptor` (MapStateDescriptor of the broadcast).
- `DataProducts.scala` — declarative data products registry (name, owner, topic, SLA, schema).
- `OrderDataProduct.scala` — `case class` of the orders data product + constant `TopicIn`.
- `CustomerDataProduct.scala` — `case class` of the customers data product + constant `TopicIn`.
- `EnrichedOrder.scala` — `case class` of the join result.
- `EnrichmentLib.scala` — pure functions: `parseOrder`/`parseCustomer`, `validateOrder`, `toJson`.
- `JsonProtocols.scala` — `spray-json` `RootJsonFormat` for all case classes.

### `src/main/python/contracts/`
- `data_contracts.py` — Pydantic `BaseModel` for `OrderDataProduct`, `CustomerDataProduct`, `EnrichedOrder`; `currency` field is normalized to UPPERCASE via `field_validator`; `DATA_PRODUCTS` dict (registry) with owner/topic/SLA/JSON schema.

### `src/test/scala/com/codesolutions/flink/mesh/`
- `EnrichmentSpec.scala` — unit tests of pure enrichment functions.

### `src/test/python/`
- `test_contracts.py` — pytest tests of Pydantic contracts.

---

## How to run

### Local mode (no infra)
```bash
sbt run
```
Output on stdout with `EnrichedOrder` of 3 orders × 2 customers.

### Kafka mode (production)
```bash
docker-compose up -d
sbt "runMain com.codesolutions.flink.mesh.DataMeshJob kafka"
```

## How to test

```bash
# Scala
sbt test

# Python
pip install -r requirements.txt
PYTHONPATH=src/main/python:src/test/python pytest src/test/python
```
