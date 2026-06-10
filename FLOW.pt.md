# Fluxo de interação entre classes — flink-data-mesh-pipeline

Visualização rápida de como os dados fluem do Kafka (ou collection local) até a saída enriquecida.

## 1. Bootstrap

```
DataMeshJob.main("kafka" | default)
  └─> StreamExecutionEnvironment
        ├─> enableCheckpointing(5000)
        ├─> setParallelism(1 | 2)
        └─> ... define pipeline ...
              └─> env.execute("Flink Data Mesh Pipeline - ...")
```

## 2. Pipeline (modo `kafka`)

```
KafkaSource (orders.input)         ──> orders: DataStream[String]
  └─> EnrichmentLib.parseOrder()           [mesh/EnrichmentLib]                (Scala)
        └─> EnrichmentLib.validateOrder()   [mesh/EnrichmentLib]                (Scala)
              └─> parsedOrders: DataStream[OrderDataProduct]

KafkaSource (customers.input)      ──> customers: DataStream[String]
  └─> EnrichmentLib.parseCustomer()        [mesh/EnrichmentLib]
              └─> customerParsed: DataStream[CustomerDataProduct]

customerParsed.broadcast(customerDescriptor)  ──> BroadcastStream[CustomerDataProduct]
  └─> parsedOrders
        .connect(broadcastCustomers)
        .process(CustomerEnrichmentFunction)  [mesh/CustomerEnrichmentFunction]
              └─> EnrichedOrder(order, customerTier, enrichedAt)
                    └─> EnrichmentLib.toJson()  [mesh/EnrichmentLib]
                          └─> print()  ──> stdout (ou orders.enriched em prod)
```

**Caminho resumido:**
`(orders.input + customers.input) → parse → validate → broadcast/connect → process → toJson → output`

## 3. Pipeline (modo `collection` local)

```
env.fromCollection(orderEvents)        ──> DataStream[String]
  └─> EnrichmentLib.parseOrder + validateOrder ──> parsedOrders

env.fromCollection(customerEvents)     ──> DataStream[String]
  └─> EnrichmentLib.parseCustomer       ──> customerParsed

customerParsed.broadcast(...) ──> BroadcastStream
parsedOrders.connect(broadcastCustomers).process(CustomerEnrichmentFunction)
  └─> .print() (stdout)
```

## 4. Validação (parity Scala ↔ Python)

```
Scala:                            Python (data_contracts.py):
EnrichmentLib.parseOrder    ─┐    OrderDataProduct (Pydantic)
EnrichmentLib.validateOrder  ┼──> amount > 0, currency 3, status enum
                              │    (mesmas regras, two-language parity)
EnrichmentLib.parseCustomer  ┘    CustomerDataProduct (Pydantic)
                                   email válido, tier enum
```

## 5. Tipos de dados

```
OrderDataProduct        (case class Scala)    ─┐
CustomerDataProduct     (case class Scala)    ─┴──> EnrichedOrder (case class Scala)
                                                   └─> JsonProtocols (spray-json) ──> JSON
```

## Mapa de pacotes

```
com.codesolutions.flink.mesh
├── DataMeshJob.scala                    ← main + entry point
├── CustomerEnrichmentFunction.scala     ← BroadcastProcessFunction
├── DataProducts.scala                   ← registry (nome, owner, topic, SLA, schema)
├── OrderDataProduct.scala               ← case class + TopicIn
├── CustomerDataProduct.scala            ← case class + TopicIn
├── EnrichedOrder.scala                  ← case class (resultado)
├── EnrichmentLib.scala                  ← parse + validate + toJson
└── JsonProtocols.scala                  ← spray-json formats

contracts/  (Python)
└── data_contracts.py                    ← Pydantic models + DATA_PRODUCTS registry
```

## Erros

`EnrichmentLib` retorna `Either[Err, T]`. No `DataMeshJob`, o `match` extrai o `Left(err)` e lança `RuntimeException` com a mensagem, o que faz o job do Flink falhar com a stack completa (modo dev). Em produção, o ideal é enviar para um side output / dead-letter.
