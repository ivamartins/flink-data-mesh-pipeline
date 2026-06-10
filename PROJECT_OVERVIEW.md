# flink-data-mesh-pipeline — Visão geral e fluxo

Pipeline **Flink + Kafka** implementando **Data Mesh**: dois data products independentes (orders e customers) unidos por um job de enriquecimento em nível de plataforma. Python para Data Engineering, Apache Flink, Data Mesh, Pydantic, Data Quality.

## Stack (com versões)

- **Scala 2.12.18** (Flink 1.18+ é built against Scala 2.12)
- **sbt** (versão em `project/build.properties`)
- **Apache Flink 1.18.1** (`flink-streaming-scala`, `flink-clients`, `flink-json`, `flink-avro`)
- **Flink Kafka Connector 3.0.2-1.18** (`flink-connector-kafka`)
- **Flink Test Utils 1.18.1** + **Flink Runtime Web 1.18.1** + **Flink Tests 1.18.1** (testes)
- **Apache Avro 1.11.3**
- **Spray-JSON 1.3.6**
- **Logback 1.4.14**
- **ScalaTest 3.2.17**
- **Python**: Pydantic (com `EmailStr`/`Literal`) + `pydantic` (ver `requirements.txt`)
- **Docker Compose**: Kafka + Flink

---

## Fluxo principal

### 1. Padrão Data Mesh
- **Domain ownership**: `orders` e `customers` cada um com seu case class, contrato (Pydantic), tópico Kafka e owner.
- **Data as a product**: cada produto é versionado, validado e consumido de forma independente.
- **Self-serve platform**: o join Flink é uma capacidade de plataforma — domínios publicam produtos, não escrevem joins.
- **Federated governance**: contratos Pydantic são o schema global; validadores Scala os espelham.

### 2. Modos de execução
- **`runWithCollections` (default)**: tudo in-process, sem Kafka. Cria `StreamExecutionEnvironment` com checkpoint a cada 5s, parallelism 1, lê coleções hard-coded de orders/customers, aplica enrichment e imprime o `EnrichedOrder` no stdout.
- **`runWithKafka`** (produção): lê de `orders.input` e `customers.input` via `KafkaSource` (Brokers via `BOOTSTRAP_SERVERS` env var, groupIds `data-mesh-orders`/`data-mesh-customers`, `OffsetsInitializer.earliest()`), aplica enrichment, serializa o `EnrichedOrder` para JSON e publica em `orders.enriched`.

### 3. Tópicos Kafka
| Tópico              | Função                                     |
|---------------------|--------------------------------------------|
| `orders.input`      | Fonte: data product de pedidos             |
| `customers.input`   | Fonte: data product de clientes            |
| `orders.enriched`   | Saída: pedido enriquecido com o cliente    |

### 4. Enrichment (`CustomerEnrichmentFunction`)
- `BroadcastProcessFunction`: o stream de clientes é **broadcast** e cacheado no estado do operator; o stream de pedidos é **conectado** (`.connect(broadcastCustomers)`) e recebe o join via `process(...)`.
- Para cada `OrderDataProduct`, enriquece com o `customerTier` do `CustomerDataProduct` correspondente, produzindo `EnrichedOrder(order, customerTier, enrichedAt)`.

### 5. Validação (`EnrichmentLib` Scala + `data_contracts.py` Python)
Toda regra é duplicada nas duas linguagens (parity), garantindo governança federada:

| Regra | Pydantic (Python) | Scala (EnrichmentLib) |
|---|---|---|
| `amount > 0` | `Field(gt=0)` | `if (o.amount <= 0) Left(...)` |
| `currency` 3 letras | `min_length=3, max_length=3` | `o.currency.length != 3` |
| `status` enum | `Literal[...]` | `Set("CREATED", ...)` |
| `email` válido | `EmailStr` | `c.email.contains("@")` |
| `tier` enum | `Literal[...]` | `Set("BRONZE", ...)` |

Em Scala, `parseOrder` e `parseCustomer` retornam `Either[Err, T]`, encadeados com `flatMap(validateOrder)`.

---

## O que tem em cada subpasta

### Raiz
- `build.sbt` — Scala 2.12.18, Flink 1.18.1, Kafka Connector 3.0.2-1.18, ScalaTest 3.2.17.
- `project/build.properties` — versão sbt.
- `requirements.txt` — deps Python (Pydantic, pytest).
- `docker-compose.yml` — sobe Kafka + Flink para E2E.
- `README.md` — quickstart, modos, parity table.
- `.gitignore`, `.connection-test` (sentinel).

### `src/main/scala/com/codesolutions/flink/mesh/`
- `DataMeshJob.scala` — `main`; roteia entre `runWithCollections` (default) e `runWithKafka` (arg `kafka`).
- `CustomerEnrichmentFunction.scala` — `BroadcastProcessFunction` com `customerDescriptor` (MapStateDescriptor do broadcast).
- `DataProducts.scala` — registry declarativo de data products (nome, owner, topic, SLA, schema).
- `OrderDataProduct.scala` — `case class` do data product de pedidos + constante `TopicIn`.
- `CustomerDataProduct.scala` — `case class` do data product de clientes + constante `TopicIn`.
- `EnrichedOrder.scala` — `case class` do resultado do join.
- `EnrichmentLib.scala` — funções puras: `parseOrder`/`parseCustomer`, `validateOrder`, `toJson`.
- `JsonProtocols.scala` — `spray-json` `RootJsonFormat` para todos os case classes.

### `src/main/python/contracts/`
- `data_contracts.py` — Pydantic `BaseModel` para `OrderDataProduct`, `CustomerDataProduct`, `EnrichedOrder`; campo `currency` é normalizado para UPPERCASE via `field_validator`; dicionário `DATA_PRODUCTS` (registry) com owner/topic/SLA/schema JSON.

### `src/test/scala/com/codesolutions/flink/mesh/`
- `EnrichmentSpec.scala` — testes unitários das funções puras de enrichment.

### `src/test/python/`
- `test_contracts.py` — testes pytest dos contratos Pydantic.

---

## Como rodar

### Modo local (sem infra)
```bash
sbt run
```
Saída no stdout com `EnrichedOrder` de 3 orders × 2 customers.

### Modo Kafka (produção)
```bash
docker-compose up -d
sbt "runMain com.codesolutions.flink.mesh.DataMeshJob kafka"
```

## Como testar

```bash
# Scala
sbt test

# Python
pip install -r requirements.txt
PYTHONPATH=src/main/python:src/test/python pytest src/test/python
```
