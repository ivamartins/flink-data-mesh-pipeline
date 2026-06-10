# flink-data-mesh-pipeline

Flink + Kafka pipeline implementing a **Data Mesh** architecture with two independent data products (orders and customers) joined by a platform-level enrichment job.

## Why this project?

Built to showcase the **Data Engineer** role requirements:

| JD requirement | Where |
|---|---|
| ETL Data Pipelines | `DataMeshJob` |
| Python for Data Engineering | `src/main/python/contracts/data_contracts.py` (Pydantic) |
| Apache Flink | `DataMeshJob` + `CustomerEnrichmentFunction` |
| Data Mesh Architecture | Two independent data products + platform-level join |
| Pydantic | `data_contracts.py` |
| Data Quality Frameworks | Validation in `EnrichmentLib` + Pydantic contracts |
| Advanced PostgreSQL | n/a (Kafka) — see `dbt-airflow-data-platform` |
| AWS, dbt, Airflow | n/a here — see sibling projects |

## Project layout

```
flink-data-mesh-pipeline/
├── src/main/scala/com/codesolutions/flink/mesh/
│   ├── DataMeshJob.scala               # main: collection + kafka modes
│   ├── CustomerEnrichmentFunction.scala # BroadcastProcessFunction
│   ├── DataProducts.scala              # Data product registry
│   ├── OrderDataProduct.scala          # case class
│   ├── CustomerDataProduct.scala       # case class
│   ├── EnrichedOrder.scala             # case class
│   ├── EnrichmentLib.scala             # pure parsing + validation + enrichment
│   └── JsonProtocols.scala             # spray-json formats
├── src/main/python/contracts/
│   └── data_contracts.py               # Pydantic contracts
├── src/test/scala/.../EnrichmentSpec.scala
├── src/test/python/test_contracts.py
├── docker-compose.yml                 # Kafka + Flink for full E2E
└── build.sbt
```

## How to run

### Mode 1 — local collection (no infrastructure)

```bash
sbt run
```

This runs a 2-domain job entirely in-process with sample data. Output is printed to stdout.

### Mode 2 — Kafka (production-like)

```bash
docker-compose up -d
sbt "runMain com.codesolutions.flink.mesh.DataMeshJob kafka"
```

Then produce messages to:
- `orders.input`
- `customers.input`

The enriched output goes to `orders.enriched`.

## How to test

### Scala
```bash
sbt test
```

### Python (data contracts)
```bash
pip install -r requirements.txt
PYTHONPATH=src/main/python:src/test/python pytest src/test/python
```

## Data Mesh concepts illustrated

1. **Domain ownership**: `orders` and `customers` each have their own case class, contract (Pydantic), topic, and (declarative) owner.
2. **Data as a product**: each product is independently versioned, validated, and consumed.
3. **Self-serve platform**: the Flink join is a platform-level capability — domains don't write joins, they publish products.
4. **Federated governance**: Pydantic contracts are the global schema; Scala validators mirror them.

## Pydantic ⇄ Scala parity

Every validation rule is enforced on BOTH sides:

| Rule | Pydantic (Python) | Scala (EnrichmentLib) |
|---|---|---|
| `amount > 0` | `Field(gt=0)` | `if (o.amount <= 0) Left(...)` |
| `currency` 3 letters | `min_length=3, max_length=3` | `o.currency.length != 3` |
| `status` enum | `Literal[...]` | `Set("CREATED", ...)` |
| `email` valid | `EmailStr` | `c.email.contains("@")` |
| `tier` enum | `Literal[...]` | `Set("BRONZE", ...)` |

## See also

- `akka-scala-base` (Senior Software Engineer role)
- `dbt-airflow-data-platform` (Data Engineer role — dbt + Airflow)
- `scala-akka-aws-microservice` (Senior Software Engineer role — AWS Fargate)
